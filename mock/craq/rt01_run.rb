require 'rubygems'
require 'chukan'

include Chukan
include Chukan::Test

num  = 3
port = 9090
addrs = (0..num-1).map {|i|
	"127.0.0.1:#{port+i}"
}

begin
	dss = (0..num-1).map {|i|
		spawn("ruby ds.rb #{addrs.join(' ')} #{i}")
	}

	sleep 0.5

	10.times do
		test "io" do
			pr = spawn("ruby client.rb #{addrs.join(' ')}")
			pr.join.exitstatus == 0
		end
	end

ensure
	dss.each {|ds| ds.term }
	dss.each {|ds| ds.join }
end

