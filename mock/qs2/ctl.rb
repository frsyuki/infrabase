require 'rpc'
require 'cclog'
require 'route'
require 'term'

class ControlClient
	def initialize(qs_addrs)
		@qs_addrs = qs_addrs
		@qs = @qs_addrs.map {|qsaddr|
			MessagePack::RPC::Client.new(*qsaddr)
		}
	end

	def add_node(addr, name)
		add = [ [addr, 10, name] ]
		join  = []
		ready = [addr]
		fault = []
		leave = []
		remove = []
		@qs.map {|s|
			s.call(:set_stage, add, join, ready, leave, fault, remove)
		}
	end

	def close
		@qs.each {|s| s.close }
	end
end

if ARGV.size < 2
	puts "usage: #{$0} <qs host:port> <command> [args...]"
	exit 1
end

$log = CCLog.new
$log.code_format = " [code:IBDS.00.00.%05d.RB]"

host, port = ARGV.shift.split(':',2)
port = (port || 9800).to_i
qs_addrs = [MessagePack::RPC::Address.new(host, port)]

cmd = ARGV.shift

c = ControlClient.new(qs_addrs)
begin

	case cmd
	when "add"
		if ARGV.size != 1
			puts "usage: #{$0} <qs host:port> add <ds host:port>"
			exit 1
		end
		host, port = ARGV.shift.split(':',2)
		port = (port || 9900).to_i
		addr = MessagePack::RPC::Address.new(host, port)
		name = "svr%02d" % rand(100)
		c.add_node(addr, name)

	else
		puts "unknown command: #{cmd}"
		exit 1
	end

ensure
	c.close rescue nil
end

