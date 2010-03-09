
class VPoint
	def initialize(vid, address)
		@vid = vid
		@address = address
	end
	attr_reader :vid, :address

	def <=>(o)
		if @vid == o.vid
			@address <=> o.address
		else
			@vid <=> o.vid
		end
	end

	def to_msgpack(out = '')
		[@vid, @address].to_msgpack(out)
	end

	def self.from_msgpack(obj)
		vid = obj[0]
		address = MessagePack::RPC::Address.load(obj[1])
		VPoint.new(vid, address)
	end

	def to_s
		"#{@address}@#{[@vid].pack('Q').unpack('h*')}"
	end
end

# qsが持つ情報
class RoutingSource
	def initialize(sprep, mprep, ready, fault, tag = nil)
		@sprep = sprep  # vector<VPoint>
		@mprep = mprep  # vector<VPoint>
		@ready = ready  # vector<VPoint>
		@fault = fault  # vector<VPoint>
		if tag
			@tag = tag
		else
			update_tag
		end
	end
	attr_reader :sprep, :mprep, :ready, :fault
	attr_reader :tag

	def set_sprep(vpoint)
		unless @sprep.find(vpoint)
			@sprep.push vpoint
		end
		@mprep.delete(vpoint)
		@ready.delete(vpoint)
		@fault.delete(vpoint)
		update_tag
		self
	end

	def set_mprep(vpoint)
		unless @mprep.find(vpoint)
			@mprep.push vpoint
		end
		@sprep.delete(vpoint)
		@ready.delete(vpoint)
		@fault.delete(vpoint)
		update_tag
		self
	end

	def set_ready(vpoint)
		unless @ready.find(vpoint)
			@ready.push vpoint
		end
		@sprep.delete(vpoint)
		@mprep.delete(vpoint)
		@fault.delete(vpoint)
		update_tag
		self
	end

	def set_fault(vpoint)
		unless @fault.find(vpoint)
			@fault.push vpoint
		end
		@sprep.delete(vpoint)
		@mprep.delete(vpoint)
		@ready.delete(vpoint)
		update_tag
		self
	end

	# FIXME ok?
	def set_fault_addr(addr)
		@sprep.reject! {|vp| @fault.push(vp) if vp.address == addr }
		@mprep.reject! {|vp| @fault.push(vp) if vp.address == addr }
		@ready.reject! {|vp| @fault.push(vp) if vp.address == addr }
		self
	end

	def remove(vpoint)
		@sprep.delete(vpoint)
		@mprep.delete(vpoint)
		@ready.delete(vpoint)
		@fault.delete(vpoint)
		update_tag
		self
	end

	def to_msgpack(out = '')
		[@tag, @sprep, @mprep, @ready, @fault].to_msgpack(out)
	end

	def self.from_msgpack(obj)
		tag = obj[0]
		sprep = obj[1].map {|obj| VPoint.from_msgpack(obj) }
		mprep = obj[2].map {|obj| VPoint.from_msgpack(obj) }
		ready = obj[3].map {|obj| VPoint.from_msgpack(obj) }
		fault = obj[4].map {|obj| VPoint.from_msgpack(obj) }
		RoutingSource.new(sprep, mprep, ready, fault, tag)
	end

	private
	def update_tag
		@tag = Hash.hash_generic(to_msgpack)
	end
end

# routing vector data structure for DS
class RoutingVector
	BUCKET_SIZE = 1<<13

	def initialize
		@vector = []
	end

	def reset(prep, ready, fualt, skip_address)
		# FIXME create RoutingVector
	end

	def find(key)
		raise "no node" if @vector.empty?
		@vector[Hash.hash_key(key) % @vector.size].address
	end
end

# routing table cache for DS
class ReplicateRoutingVector
	def initialize(skip_slave_address)  # RoutingSource
		@skip_slave_address = skip_slave_address
		@master = RoutingVector.new
		@slave  = RoutingVector.new
	end

	def reset(sprep, mprep, ready, fault)
		@master.reset(mprep, ready, fault, nil)
		@slave .reset(sprep, ready, fault, @skip_slave_address)
		nil
	end

	def find_master(key)
		@master.find(key)
	end

	def find_slave(key)
		@slave.find(key)
	end

	def find_master_slave(key)
		return find_master(key), find_slave(key)
	end
end

# routing table manager for DS
# ロック粒度とアップデートの一貫性管理
class RoutingManager
	def initialize(skip_slave_address)
		@route = ReplicateRoutingVector.new(skip_slave_address)
		@qs = {}  # address => RoutingSource
		@active_nodes = []
	end

	def update_qs(qsaddr, rsrc)
		@qs[qsaddr] = rsrc
		update
	end

	def tag_of_qs(qsaddr)
		rs = @qs[qsaddr]
		if rs
			rs.tag
		else
			nil
		end
	end

	def update(majority_border = nil)
		majority_border ||= ((@qs.size-1) / 2) + 1  # /2切り上げ
		sprep = Hash.new {|hash,key| hash[key] = 0 }  # VPoint => num
		mprep = Hash.new {|hash,key| hash[key] = 0 }  # VPoint => num
		ready = Hash.new {|hash,key| hash[key] = 0 }  # VPoint => num
		fault = Hash.new {|hash,key| hash[key] = 0 }  # VPoint => num
		@qs.each_pair {|qsaddr, rsrc|
			rsrc.sprep.each {|vp| sprep[vp] += 1 }
			rsrc.mprep.each {|vp| mprep[vp] += 1 }
			rsrc.ready.each {|vp| ready[vp] += 1 }
			rsrc.fault.each {|vp| fault[vp] += 1 }
		}
		sprep.reject! {|vp, num| num < majority_border }
		mprep.reject! {|vp, num| num < majority_border }
		ready.reject! {|vp, num| num < majority_border }
		fault.reject! {|vp, num| num < majority_border }
		sprep = sprep.keys.sort  # FIXME sort?
		mprep = mprep.keys.sort  # FIXME sort?
		ready = ready.keys.sort  # FIXME sort?
		fault = fault.keys.sort  # FIXME sort?
		@route.reset(sprep, mprep, ready, fault)
		updated(sprep, mprep, ready, fault)
		self
	end

	def find_master(key)
		@route.find_master(key)
	end

	def find_slave(key)
		@route.find_slave(key)
	end

	def find_master_slave(key)
		@route.find_master_slave(key)
	end

	attr_reader :active_nodes

	private
	def updated(sprep, mprep, ready, fault)
		$log.TRACE "routing manager updated: #{sprep.join(', ')} #{mprep.join(', ')} #{ready.join(', ')} #{fault.join(', ')}"
		@active_nodes = (sprep + mprep + ready).map {|vp| vp.address }.uniq.sort
	end
end

