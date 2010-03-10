require 'rpc'
require 'cclog'
require 'route'
require 'term'

module DataServer
	def self.init(self_addr, qs_addrs)
		$rs = Resource.new(self_addr, qs_addrs)
		$rs.module_init
		$rs.net.listen '0.0.0.0', self_addr.port, $rs.mod_rpc
		$rs.net
	end
end
Server = DataServer


class Server::Resource
	def initialize(self_addr, qs_addrs)
		@net = MessagePack::RPC::Server.new
		@self_addr = self_addr
		@qs_addrs = qs_addrs
		@route = RoutingManager.new(self_addr)
	end
	def module_init
		@mod_store = Server::ModStore.new
		@mod_route = Server::ModRoute.new
		@mod_alive = Server::ModAlive.new
		@mod_rpc = Server::ModRPC.new
	end
	attr_reader :net
	attr_reader :mod_store, :mod_route, :mod_alive, :mod_rpc
	attr_reader :self_addr, :qs_addrs, :route
end


class Server::ModRPC
	def set(row, record)
		$log.TRACE "RPC set row=#{row.inspect} record=#{record.inspect}"
		$rs.mod_store.set(row, record)
	end

	def merge(row, record)
		$log.TRACE "RPC merge row=#{row.inspect} record=#{record.inspect}"
		$rs.mod_store.merge(row, record)
	end

	def get(row, cols)
		$log.TRACE "RPC get row=#{row.inspect} cols=#{cols.inspect}"
		$rs.mod_store.get(row, cols)
	end

	def push_route(qsaddr, rsrc)
		qsaddr = MessagePack::RPC::Address.load(qsaddr)
		rsrc = RoutingSource.from_msgpack(rsrc)
		$log.TRACE "RPC push_route qsaddr=#{qsaddr} rsrc=#{rsrc}"
		$rs.mod_route.push_route(qsaddr, rsrc)
	end
end


class Server::ModAlive
	def initialize
		@eater = TermEater.new
		$rs.net.start_timer(1, true, &method(:do_heartbeat))
	end

	private
	def do_heartbeat
		$log.TRACE "do_heartbeat"
		$rs.qs_addrs.each {|qsaddr|
			s = $rs.net.get_session(*qsaddr)
			s.callback(:heartbeat, $rs.self_addr) do |err, res|
				if res
					@eater.feed(qsaddr, res)
				end
			end
		}
	end
end


class Server::ModRoute
	def initialize
		@route = RoutingManager.new($rs.self_addr)
		$rs.net.start_timer(10, true, &method(:do_get_routing_source))
		do_get_routing_source
	end

	def push_route(qsaddr, rsrc)
		if @route.update_qs(qsaddr, rsrc)
			route_changed
		end
		nil
	end

	private
	def do_get_routing_source
		$rs.qs_addrs.each {|qsaddr|
			s = $rs.net.get_session(*qsaddr)
			s.callback(:get_routing_source, @route.tag_of_qs(qsaddr)) do |err, res|
				$log.TRACE "res RPC get_routing_source #{err.inspect} #{res.inspect}"
				if res
					rsrc = RoutingSource.from_msgpack(res)
					push_route(qsaddr, rsrc)
				end
			end
		}
	end

	def route_changed
		# FIXME check the deference
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

	def set(row, record)
		raise "type error" if map.class != Hash
		@db[row] = record
		nil
	end

	def merge(row, record)
		raise "type error" if map.class != Hash
		record = @db[row] || Hash.new
		@db[row] = record.update(record)
		nil
	end

	def get(row, cols)
		unless cols
			return @db[row]
		end
		raise "type error" if map.class != Array
		if record = @db[row]
			record.reject! {|col,val| !cols.include?(col) }
			return record
		else
			return nil
		end
	end
end


if ARGV.size < 2
	puts "usage: #{$0} <QS host:port> ... <self host:port>"
	exit 1
end

$log = CCLog.new
$log.code_format = " [code:IBDS.00.00.%05d.RB]"

host, port = ARGV.pop.split(':',2)
port = (port || 9800).to_i
self_addr = MessagePack::RPC::Address.new(host, port)

qs_addrs = ARGV.map {|addr|
	host, port = addr.split(':', 2)
	port = (port || 9900).to_i
	MessagePack::RPC::Address.new(host, port)
}

net = Server.init(self_addr, qs_addrs)
$log.INFO "start #{self_addr}"
net.run

