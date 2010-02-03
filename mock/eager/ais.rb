require 'rubygems'
require 'msgpack/rpc'
require 'tokyocabinet'

module ActiveIndexServer
	class Resource; end
	class ModRPC; end
	class ModStore; end

	def self.init(port, dbpath)
		$rs = Resource.new(dbpath)
		$rs.net.listen '0.0.0.0', port, $rs.mod_rpc
		$rs.net
	end
end
Server = ActiveIndexServer


class Server::Resource
	def initialize(dbpath)
		@net = MessagePack::RPC::Server.new
		@mod_rpc   = Server::ModRPC.new
		@mod_store = Server::ModStore.new(dbpath)
	end
	attr_reader :net
	attr_reader :mod_rpc, :mod_store
end


class Server::ModRPC
	def write(row, rquery)
		$rs.mod_store.write(row, rquery)
	end

	def read(row, rquery)
		$rs.mod_store.read(row, rquery)
	end
end


class Server::ModStore
	def initialize(dbpath)
		@dbpath = dbpath
		@db = TokyoCabinet::BDB.new
		ret = @db.open(dbpath, TokyoCabinet::BDB::OWRITER | TokyoCabinet::BDB::OCREAT)
		unless ret
			raise "can't open database: #{@db.errmsg(@db.ecode)}"
		end
		@op = Operator.new(@db)
	end

	def write(part, pquery)
		puts "write on '#{part}': #{pquery.inspect}"
		begin
			@db.tranbegin
			rets = []
			pquery.each {|row, rquery|
				rquery.each {|method, args|
					rets << @op.__send__("op_#{method}", part, row, *args)
				}
			}
			@db.trancommit
			return rets
		rescue
			@db.tranabort
			raise $!
		end
	end

	def read(part, pquery)
		puts "read on '#{part}': #{pquery.inspect}"
		begin
			@db.tranbegin
			rets = []
			pquery.each {|row, rquery|
				rquery.each {|method, args|
					rets << @op.__send__("op_#{method}", part, row, *args)
				}
			}
			@db.trancommit
			return rets
		rescue
			@db.tranabort
			raise $!
		end
	end

	private
	class Operator
		def initialize(db)
			@db = db
		end

		def op_set(part, row, col, val)
			puts "set #{row} #{col} <- #{val}"
			key = key_of(part, row, col)
			@db[key] = val
			nil
		end

		def op_add(part, row, col, val)
			puts "add #{row} #{col} <- #{val}"
			key = key_of(part, row, col)
			@db.putkeep(key, val)
			nil
		end

		def op_append(part, row, col, val)
			puts "append #{row} #{col} <- #{val}"
			key = key_of(part, row, col)
			@db.putcat(key, val)
			nil
		end

		def op_get(part, row, col)
			puts "get #{row} #{col}"
			key = key_of(part, row, col)
			val = @db[key]
		end

		private
		def key_of(part, row, col)
			"#{part}\0#{row}\0#{col}"
		end
	end
end


if ARGV.size != 2
	puts "usage: #{$0} <port> <dbpath>"
	exit 1
end

port = ARGV.shift.to_i
dbpath = ARGV.shift

net = Server.init(port, dbpath)
net.run

