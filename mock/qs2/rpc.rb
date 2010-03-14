require 'rubygems'
require 'msgpack/rpc'
require 'digest/sha1'
require 'cclog'

module RPCModule
	def send(method, *args)
		__send__(method, *args)
	rescue Exception
		$log.DEBUG "RPC error: #{$!}"
		$!.backtrace.each {|msg|
			$log.DEBUG "  #{msg}"
		}
		raise $!
	end
end

class Hash
	def self.hash_key(key)
		Digest::SHA1.digest(key).unpack('Q')[0]
	end

	def self.hash_generic(obj)
		Digest::SHA1.digest(obj.to_s).unpack('Q')[0]
	end

	def self.hash_address(addr)
		Digest::SHA1.digest(addr.dump.to_s).unpack('Q')[0]
	end

	def self.hash_vid(vid)
		Digest::SHA1.digest([vid].pack('Q')).unpack('Q')[0]
	end
end

