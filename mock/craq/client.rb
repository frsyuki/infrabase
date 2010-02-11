require 'rubygems'
require 'msgpack/rpc'
require 'pp'


class Client
	def initialize(chain)
		@chain = chain
		@sp = MessagePack::RPC::SessionPool.new
	end

	# read-commited read
	def read(key)
		addr = head_for(key)
		s = @sp.get_session(*addr)
		s.call(:read, key)
	end

	# serializable read
	def sread(key)
		addr = head_for(key)
		s = @sp.get_session(*addr)
		s.call(:sread, key)
	end

	def write(key, val)
		addr = head_for(key)
		s = @sp.get_session(*addr)
		s.call(:write, key, val)
	end

	private
	def head_for(key)
		@chain.first
	end

	def random_for(key)
		@chain.choice
	end
end


if ARGV.size < 1
	puts "usage: #{$0} <host:port> ..."
	exit 1
end

hosts = ARGV.map {|addr|
	host, port = addr.split(':', 2)
	MessagePack::RPC::Address.new(host, port.to_i)
}

c = Client.new(hosts)

p c.write("key1", "val1")
p c.read("key1")
p c.write("key2", "val2")
p c.sread("key2")

