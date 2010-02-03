require 'rubygems'
require 'msgpack/rpc'
require 'pp'

class Client
	class Transaction
		class Row; end

		def initialize(c, part)
			@c = c
			@part = part
			@pquery = []
		end

		def row(row_, &block)
			r = Row.new(row_)
			block.call(r)
			@pquery << r.get_query
			self
		end

		def commit
			@c.commit(@part, @pquery)
		end
	end

	class Transaction::Row
		def initialize(row)
			@row = row
			@rquery = []
		end

		def get_query
			[@row, @rquery]
		end

		def set(col, val)
			@rquery << ["set", [col, val]]
		end

		def add(col, val)
			@rquery << ["add", [col, val]]
		end

		def append(col, val)
			@rquery << ["append", [col, val]]
		end

		def get(col)
			@rquery << ["get", [col]]
		end
	end

	def initialize(host, port)
		@host = host
		@port = port
		@client = MessagePack::RPC::Client.new(@host, @port)
	end
	attr_reader :client

	def begin(part, &block)
		tx = Transaction.new(self, part)
		block.call(tx)
	end

	def commit(part, pquery)
		@client.call(:write, part, pquery)
	end
end


if ARGV.size != 1
	puts "usage: #{$0} <address:port>"
	exit 1
end

addr = ARGV.shift
host, port = addr.split(':', 2)
port = port.to_i


c = Client.new(host, port)

c.begin("part1") do |tx|
	tx.row("row1") do |row|
		row.set("col1", "val1")
		row.set("col2", "val2")
		row.set("col3", "val3")
	end
	tx.row("row2") do |row|
		row.set("col1", "val1")
		row.set("col2", "val2")
		row.set("col3", "val3")
	end
	pp tx.commit
end

c.begin("part1") do |tx|
	tx.row("row1") do |row|
		row.get("col1")
		row.get("col2")
		row.get("col3")
	end
	tx.row("row2") do |row|
		row.get("col1")
		row.get("col2")
		row.get("col3")
	end
	pp tx.commit
end

