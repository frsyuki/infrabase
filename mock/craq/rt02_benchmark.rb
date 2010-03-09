require 'rubygems'
require 'chukan'
require 'msgpack/rpc'

include Chukan
include Chukan::Test

lonum = ARGV.shift
lonum ||= 30000
lonum = lonum.to_i

num  = 2
port = 9090
addrs = (0..num-1).map {|i|
	"127.0.0.1:#{port+i}"
}

sp = MessagePack::RPC::SessionPool.new
chain = addrs.map {|addr| sp.get_session(*addr.split(':')) }

begin
	dss = (0..num-1).map {|i|
		spawn("ruby ds.rb #{addrs.join(' ')} #{i}")
	}

	sleep 0.5

	puts "prepare..."
	(1..lonum).map do |i|
		chain.first.send(:write, "k#{i}", "v#{i}")
	end.each do |as|
		as.join.result
	end

	GC.start
	puts "start..."
	start = Time.now

	(1..lonum).map do |i|
		#chain.last.send(:read, "k#{i}")   # disable load balance
		chain.choice.send(:read, "k#{i}")  # enable  load balance
	end.each do |as|
		as.join.result
	end

	time = Time.now - start
	puts "#{time} sec."
	puts "#{lonum / time} QPS"

ensure
	dss.each {|ds| ds.term }
	dss.each {|ds| ds.join }
end

