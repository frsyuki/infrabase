require 'rubygems'
require 'msgpack/rpc'

module DataServer
	class Resource; end
	class ModRPC; end
	class ModChain; end
	class ModStore; end

	def self.init(port, chain, self_index)
		$rs = Resource.new(chain, self_index)
		$rs.net.listen '0.0.0.0', port, $rs.mod_rpc
		$rs.net
	end
end
Server = DataServer


class Server::Resource
	def initialize(chain, self_index)
		@net = MessagePack::RPC::Server.new
		@mod_chain = Server::ModChain.new(chain, self_index)
		@mod_store = Server::ModStore.new
		@mod_rpc = Server::ModRPC.new
	end
	attr_reader :net
	attr_reader :mod_chain, :mod_store, :mod_rpc
end


class Server::ModRPC
	def initialize
	end

	def write(key, val)
		#puts "write: '#{key}' '#{val}'"
		$rs.mod_chain.write(key, val)
	end

	def awrite(key, val)
		#puts "awrite: '#{key}' '#{val}'"
		$rs.mod_chain.awrite(key, val)
	end

	def chain(key, val, ver)
		#puts "chain: '#{key}' '#{val}' #{ver}"
		$rs.mod_chain.chain(key, val, ver)
	end

	def read(key)
		#puts "read: '#{key}'"
		$rs.mod_chain.read(key)
	end

	def sread(key)
		#puts "sread: '#{key}'"
		$rs.mod_chain.sread(key)
	end

	def vquery(key)
		#puts "vquery: '#{key}'"
		$rs.mod_store.vquery(key)
	end

	def clean(key, ver)
		#puts "clean: '#{key}' #{ver}"
		$rs.mod_store.clean(key, ver)
	end
end


class Server::ModStore
	class Entry
		def initialize(val, ver)
			@val = val
			@ver = ver
			@clean = false
		end
		attr_reader :val, :ver
		attr_accessor :clean
	end

	def initialize
		@db = {}
	end

	def write(key, val)
		ovals = @db[key]
		if ovals
			ver = ovals.last.ver + 1
			nvals = ovals + [Entry.new(val, ver)]
			nvals = nvals.sort_by {|e| e.ver }
		else
			ver = 0
			nvals = [Entry.new(val, ver)]
		end
		@db[key] = nvals
		ver
	end

	def chain(key, val, ver)
		ovals = @db[key]
		ovals ||= []
		ovals.push Entry.new(val, ver)
		ovals = ovals.sort_by {|e| e.ver }
		@db[key] = ovals
		nil
	end

	def read(key)
		# read-commited read
		ovals = @db[key]
		ovals ||= []
		val = nil
		ovals.reverse_each {|e|
			if e.clean
				val = e.val
				break
			end
		}
		val
	end

	def sread(key)
		ovals = @db[key]
		unless ovals
			return nil
		end
		last = ovals.last
		if last.clean
			return last.val
		else
			return false
		end
	end

	def vread(key, ver)
		ovals = @db[key]
		ovals ||= []
		val = nil
		ovals.reverse_each {|e|
			if e.ver == ver
				val = e.val
				break
			end
		}
		val
	end

	def vquery(key)
		ovals = @db[key]
		ovals ||= []
		ver = nil
		ovals.reverse_each {|e|
			if e.clean
				ver = e.ver
				break
			end
		}
		ver
	end

	def clean(key, ver)
		ovals = @db[key]
		target = ovals.find {|e| e.ver == ver }
		if target
			target.clean = true
		end
		# FIXME 古いバージョンを消す
		nil
	end
end


class Server::ModChain
	def initialize(chain, self_index)
		@chain = chain
		@self_index = self_index
	end

	def write(key, val)
		ver = $rs.mod_store.write(key, val)
		do_chain(key, val, ver)
	end

	def awrite(key, val)
		ver = $rs.mod_store.write(key, val)
		do_chain(key, val, ver)
		ver
	end

	def chain(key, val, ver)
		$rs.mod_store.chain(key, val, ver)
		do_chain(key, val, ver)
	end

	def sread(key)
		ret = $rs.mod_store.sread(key)
		if ret.nil?
			return nil
		end
		if ret
			return ret
		else
			as = MessagePack::RPC::AsyncResult.new
			addr = tail_for(key)
			s = $rs.net.get_session(*addr)
			s.callback(:vquery, key) {|err, res|
				if err
					as.error(err)
				else
					ver = res
					val = $rs.mod_store.vread(key, ver)
					as.result(val)
					$rs.mod_store.clean(key, ver)
				end
			}
			return as
		end
	end

	def read(key)
		$rs.mod_store.read(key)
	end

	private
	def do_chain(key, val, ver)
		n = next_for(key)
		if n
			s = $rs.net.get_session(*n)
			as = MessagePack::RPC::AsyncResult.new
			callback = nil
			callback = Proc.new {|err, res|
				if err
					# FIXME 無限にリトライ -> 障害検出とキューが必要
					s.callback(:chain, key, val, ver, &callback)
				else
					ver = res
					$rs.mod_store.clean(key, ver)
					as.result(ver)
				end
			}
			s.callback(:chain, key, val, ver, &callback)
			return as
		else
			# tail
			$rs.mod_store.clean(key, ver)
			#chains_for(key).each {|addr|
			#	s = $rs.net.get_session(*addr)
			#	s.notify(:clean, key, ver)
			#}
			return ver
		end
	end

	def next_for(key)
		@chain[@self_index+1]
	end

	def tail_for(key)
		@chain.last
	end
end


if ARGV.size < 2
	puts "usage: #{$0} <host:port> ... <index>"
	exit 1
end

index = ARGV.pop.to_i
hosts = ARGV.map {|addr|
	host, port = addr.split(':', 2)
	MessagePack::RPC::Address.new(host, port.to_i)
}

port = hosts[index].port

net = Server.init(port, hosts, index)
net.run

