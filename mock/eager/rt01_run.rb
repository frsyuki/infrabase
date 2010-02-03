require 'rubygems'
require 'chukan'

include Chukan
include Chukan::Test

ts   = spawn("ruby ts.rb 9090")
ais1 = spawn("ruby ais.rb 8001 x1.tcb")
sleep 0.5
begin

	test "init" do
		pr = spawn("ruby ctl.rb 127.0.0.1:9090 ais 127.0.0.1:8001")
		pr.join.exitstatus == 0
	end
	sleep 0.5

	test "io" do
		pr = spawn("ruby client.rb 127.0.0.1:9090")
		pr.join.exitstatus == 0
	end

ensure
	ts.term
	ais1.term
	ts.join
	ais1.join
end

