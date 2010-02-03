require 'rubygems'
require 'msgpack/rpc'

if ARGV.length < 2
	puts "Usage: #{$0} <address:port> <cmd>"
	exit 1
end

addr = ARGV.shift
host, port = addr.split(':', 2)
port = port.to_i

client = MessagePack::RPC::Client.new(host, port)

cmd = ARGV.shift

ret = case cmd
when "ais"
	nodes = ARGV
	if nodes.empty?
		client.call(:get_aismap)
	else
		nodes.map! {|addr|
			host, port = addr.split(':', 2)
			[host, port.to_i]
		}
		client.call(:update_aismap, nodes)
	end

else
	puts "unknown command #{cmd}"
	exit 1
end

p ret

