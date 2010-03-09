require 'rubygems'
require 'msgpack/rpc'
require 'digest/sha1'

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

