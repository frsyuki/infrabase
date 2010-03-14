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
	include RPCModule

	# heartbeat rsrc_tag phase dsaddr
	def heartbeat(dsaddr, rsrc_tag)
		dsaddr = MessagePack::RPC::Address.load(dsaddr)
		$log.TRACE "RPC heartbeat dsaddr=#{dsaddr} rsrc_tag=#{rsrc_tag}"
		$rs.mod_alive.heartbeat(rsrc_tag, dsaddr)
	end

	def get_routing_source(tag = nil)
		$log.TRACE "RPC get_routing_source tag=#{tag}"
		$rs.mod_route.get_routing_source(tag)
	end

	def set_stage(add, join, ready, fault, leave, remove)
		add.map! {|obj|
			addr  = MessagePack::RPC::Address.load(obj[0])
			nvids = obj[1].to_i
			name  = obj[2].to_s
			[addr, nvids, name]
		}
		join.map!  {|obj| MessagePack::RPC::Address.load(obj) }
		ready.map! {|obj| MessagePack::RPC::Address.load(obj) }
		fault.map! {|obj| MessagePack::RPC::Address.load(obj) }
		leave.map! {|obj| MessagePack::RPC::Address.load(obj) }
		remove.map!{|obj| MessagePack::RPC::Address.load(obj) }
		$log.TRACE "RPC set_stage add=#{add} join=#{join} ready=#{ready} fault=#{fault} leave=#{leave} fault=#{fault} remove=#{remove}"
		$rs.mod_route.set_stage(add, join, ready, fault, leave, remove)
	end
end


class Server::ModAlive
	def initialize
		@feeder = TermFeeder.new(4,2)   # FIXME 秒数
		$rs.net.start_timer(1, true, &method(:do_expire))
	end

	def heartbeat(rsrc_tag, dsaddr)
		$rs.mod_route.check_routing_source(dsaddr, rsrc_tag)
		@feeder.order(dsaddr)
	end

	def set_active_nodes(ds_addrs)
		$log.TRACE "set_active_nodes #{ds_addrs.join(', ')}"
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
		@rsrc = Route::RoutingSource.new
		@actives = []
	end

	attr_reader :actives

	def get_routing_source(tag)
		if tag && @rsrc.tag == tag
			return nil
		end
		@rsrc
	end

	def check_routing_source(dsaddr, tag)
		if tag && @rsrc.tag == tag
			return nil
		end
		push_route_ds(dsaddr)
	end

	def set_stage(add, join, ready, fault, leave, remove)
		add.each {|addr, nvids, name|
			vid = Hash.hash_address(addr)
			vids = []
			nvids.times {
				vids << vid
				vid = Hash.hash_vid(vid)
			}
			@rsrc.add_node(addr, name, vids)
		}
		join.each  {|addr| @rsrc.set_join(addr)   }
		ready.each {|addr| @rsrc.set_ready(addr)  }
		fault.each {|addr| @rsrc.set_fault(addr)  }
		leave.each {|addr| @rsrc.set_leave(addr)  }
		remove.each{|addr| @rsrc.remove(addr) }
		route_updated
		$rs.mod_alive.set_active_nodes(self.actives)
		nil
	end

	def ready_detected(ds_addrs)
		$log.DEBUG "ready detected #{ds_addrs.join(', ')}"
		changed = false
		ds_addrs.each {|dsaddr|
			changed = true if @rsrc.set_ready(dsaddr)
		}
		route_updated if changed
	end

	def fault_detected(ds_addrs)
		$log.DEBUG "fault detected #{ds_addrs.join(', ')}"
		changed = false
		ds_addrs.each {|dsaddr|
			changed = true if @rsrc.set_fault(dsaddr)
		}
		route_updated if changed
	end

	def leave_detected(ds_addrs)
		$log.DEBUG "leave detected #{ds_addrs.join(', ')}"
		changed = false
		ds_addrs.each {|dsaddr|
			changed = true if @rsrc.set_leave(dsaddr)
		}
		route_updated if changed
	end

	private
	def route_updated
		@actives = @rsrc.active_nodes
		$log.TRACE "route updated #{@rsrc.inspect}"
		$log.TRACE "active nodes #{@actives.join(', ')}"
		push_route_ds_all
	end

	def push_route_ds(dsaddr)
		s = $rs.net.get_session(*dsaddr)
		s.callback(:push_route, $rs.self_addr, @rsrc) do |err, res|
			# FIXME ignore?
		end
	end

	def push_route_ds_all
		@actives.each {|dsaddr|
			push_route_ds(dsaddr)
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

