require 'rpc'
require 'cclog'
require 'route'
require 'term'

module QuorumServer
	def self.init(self_addr)
		$rs = Resource.new(self_addr)
		$rs.module_init
		$rs.net.listen '0.0.0.0', self_addr.port, $rs.mod_rpc
		$rs.net
	end
end
Server = QuorumServer


class Server::Resource
	def initialize(self_addr)
		@net = MessagePack::RPC::Server.new
		@self_addr = self_addr
	end
	def module_init
		@mod_route = Server::ModRoute.new
		@mod_alive = Server::ModAlive.new
		@mod_rpc = Server::ModRPC.new
	end
	attr_reader :net
	attr_reader :mod_route, :mod_alive, :mod_rpc
	attr_reader :self_addr
end


class Server::ModRPC
	def heartbeat(dsaddr)
		dsaddr = MessagePack::RPC::Address.load(dsaddr)
		$log.TRACE "RPC heartbeat dsaddr=#{dsaddr}"
		$rs.mod_alive.heartbeat(dsaddr)
	end

	def get_routing_source(tag = nil)
		$log.TRACE "RPC get_routing_source tag=#{tag}"
		$rs.mod_route.get_routing_source(tag)
	end

	def set_phase(sprep, mprep, ready, fault, remove)
		sprep.map! {|obj| VPoint.from_msgpack(obj) }
		mprep.map! {|obj| VPoint.from_msgpack(obj) }
		ready.map! {|obj| VPoint.from_msgpack(obj) }
		fault.map! {|obj| VPoint.from_msgpack(obj) }
		remove.map!{|obj| VPoint.from_msgpack(obj) }
		$log.TRACE "RPC set_phase sprep=#{sprep} mprep=#{mprep} ready=#{ready} remove=#{remove} fault=#{fault} remove=#{remove}"
		$rs.mod_route.set_phase(sprep, mprep, ready, fault, remove)
	end
end


class Server::ModAlive
	def initialize
		@feeder = TermFeeder.new(4,2)   # FIXME 秒数
		$rs.net.start_timer(1, true, &method(:do_expire))
	end

	def heartbeat(dsaddr)
		@feeder.order(dsaddr)
	end

	def new_active_node(ds_addrs)
		$log.TRACE "new_active_node #{ds_addrs.join(', ')}"
		ds_addrs.each {|dsaddr|
			@feeder.reset(dsaddr)
		}
	end

	private
	def do_expire
		$log.TRACE "do_expire"
		expired = @feeder.pass_next
		unless expired.empty?
			$rs.mod_route.fault_detected(expired)
		end
	end
end


class Server::ModRoute
	def initialize
		@rsrc = RoutingSource.new([], [], [], [])
		@active_nodes = []
	end

	attr_reader :active_nodes

	def get_routing_source(tag)
		if tag && @rsrc.tag == tag
			return nil
		end
		@rsrc
	end

	def set_phase(sprep, mprep, ready, fault, remove)
		sprep.each {|vp| @rsrc.set_sprep(vp) }
		mprep.each {|vp| @rsrc.set_mprep(vp) }
		ready.each {|vp| @rsrc.set_ready(vp) }
		fault.each {|vp| @rsrc.set_fault(vp) }
		remove.each{|vp| @rsrc.remove(vp) }
		route_updated
		new_active_node = (sprep + mprep + ready).map {|vp| vp.address }.uniq
		$rs.mod_alive.new_active_node(new_active_node)
		nil
	end

	def fault_detected(ds_addrs)
		$log.DEBUG "fault detected #{ds_addrs.join(', ')}"
		changed = false
		ds_addrs.each {|dsaddr|
			changed = true if @rsrc.shift_fault_addr(dsaddr)
		}
		route_updated if changed
	end

	private
	def route_updated
		@active_nodes = (@rsrc.sprep + @rsrc.mprep + @rsrc.ready).map {|vp| vp.address }.uniq.sort
		$log.TRACE "route updated #{@rsrc.inspect}"
		$log.TRACE "active_nodes #{@active_nodes.join(', ')}"
		push_route_ds
	end

	def push_route_ds
		@active_nodes.each {|dsaddr|
			s = $rs.net.get_session(*dsaddr)
			s.callback(:push_route, $rs.self_addr, @rsrc) do |err, res|
				# FIXME ignore?
			end
		}
	end
end


if ARGV.size != 1
	puts "usage: #{$0} <self host:port>"
	exit 1
end

$log = CCLog.new
$log.code_format = " [code:IBQS.00.00.%05d.RB]"

host, port = ARGV.shift.split(':', 2)
port = (port || 9900).to_i
self_addr = MessagePack::RPC::Address.new(host, port)

net = Server.init(self_addr)
$log.INFO "start #{self_addr}"
net.run

