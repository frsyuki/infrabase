require 'rubygems'
require 'msgpack/rpc'
require 'digest/sha1'

# query   := [part, pquery]
# pquery  := [pentry, ...]
# pentry  := [row, rquery]
# rquery  := [rentry, ...]
# rentry  := [method, args]

module TransactionServer
	class Resource; end
	class ModRPC; end
	class ModAIS; end
	class ModTx; end

	def self.init(port)
		$rs = Resource.new
		$rs.net.listen '0.0.0.0', port, $rs.mod_rpc
		$rs.net
	end
end
Server = TransactionServer


class Server::Resource
	def initialize
		@net = MessagePack::RPC::Server.new
		@mod_ais = Server::ModAIS.new
		@mod_tx  = Server::ModTx.new
		@mod_rpc = Server::ModRPC.new
	end
	attr_reader :net
	attr_reader :mod_ais, :mod_tx, :mod_rpc
end


class Server::ModRPC
	def write(part, pquery)
		$rs.mod_tx.write(part, pquery)
	end

	def read(part, pquery)
		$rs.mod_tx.read(part, pquery)
	end

	def update_aismap(aismap)
		$rs.mod_ais.update_aismap(aismap)
	end

	def get_aismap
		$rs.mod_ais.get_aismap
	end
end


class Server::ModAIS
	def initialize
		@map = []
	end

	def update_aismap(map)
		@map = map
	end

	def get_aismap
		@map
	end

	def phash(part)
		Digest::SHA1.digest(part).unpack('Q')[0]
	end

	def rhash(row)
		Digest::SHA1.digest(row).unpack('Q')[0]
	end

	def replicators_of(phash)
		raise "No node" if @map.empty?
		addrs = []
		3.times {|i|
			addrs.push @map[(phash + i) % @map.size]
		}
		addrs.uniq!
		addrs
	end
end


class Server::ModTx
	class TXQueueEntry
		def initialize(phash, part, pquery, as)
			@phash = phash
			@part = part
			@pquery = pquery
			@as = as
			@retval = []
		end
		attr_reader :phash, :part, :pquery, :retval

		def result(retval)
			puts "result: #{retval.inspect}"
			@as.result(retval)
		end

		def error(err)
			puts "error: #{err}"
			@as.error(err)
		end

		def start(count)
			@count = count
		end

		def done
			@count -= 1
			@count == 0
		end
	end

	def initialize
		@progress = []
		@txqueue  = []
	end

	def write(part, pquery)
		puts "write '#{part}': #{pquery.inspect}"

		phash = $rs.mod_ais.phash(part)
		as = MessagePack::RPC::AsyncResult.new
		e = TXQueueEntry.new(phash, part, pquery, as)
		@txqueue.push(e)
		run_queue

		as
	end

	def read(part, pquery)
		puts "read on '#{part}': #{pquery.inspect}"

		phash = $rs.mod_ais.phash(part)
		addrs = $rs.mod_ais.replicators_of(phash)
		addr = addrs.first

		as = MessagePack::RPC::AsyncResult.new
		s = $rs.net.get_session(*addr)
		s.callback(:read, part, pquery) do |err, res|
			as.result(res, err)
		end

		as
	end

	private
	def run_queue
		return if @txqueue.empty?
		$rs.net.submit do
			return if @txqueue.empty?
			e = @txqueue.last
			return if @progress.include?(e.phash)

			@txqueue.pop
			@progress.push(e.phash)
			run_queue

			begin
				tx = Hash.new {|hash,key| hash[key] = [] }

				e.pquery.each {|row, rquery|
					p row
					p rquery
					rhash = $rs.mod_ais.rhash(row)
					addrs = $rs.mod_ais.replicators_of(rhash)
					addrs.each {|addr|
						tx[addr].push [row, rquery]
					}
				}

				e.start(tx.size)
				tx.each_pair {|addr, pquery|
					s = $rs.net.get_session(*addr)
					s.callback(:write, e.part, pquery) do |err, res|
						queue_callback(err, res, e)
					end
				}
			rescue
				@progress.delete(e.phash)
				e.error($!.to_s)
				run_queue
			end

		end
	end

	def queue_callback(err, res, e)
		e.retval.concat [err, res]
		if e.done
			@progress.delete(e.phash)
			run_queue
			e.result(e.retval)
		end
	end
end


if ARGV.size != 1
	puts "usage: #{$0} <port>"
	exit 1
end

port = ARGV.shift.to_i

net = Server.init(port)
net.run

