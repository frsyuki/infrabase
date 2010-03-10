
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

	def eql?(o)
		o.class == VPoint && @vid == o.vid && @address == o.address
	end

	def hash
		@vid.hash ^ @address.hash
	end

	def ==(o)
		eql?(o)
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
		changed = false
		unless @sprep.index(vpoint)
			@sprep.push(vpoint)
			changed = true
		end
		changed = true if @mprep.delete(vpoint)
		changed = true if @ready.delete(vpoint)
		changed = true if @fault.delete(vpoint)
		update_tag if changed
		changed
	end

	#def set_mprep(vpoint)
	#	changed = false
	#	unless @mprep.index(vpoint)
	#		@mprep.push(vpoint)
	#		changed = true
	#	end
	#	changed = true if @sprep.delete(vpoint)
	#	changed = true if @ready.delete(vpoint)
	#	changed = true if @fault.delete(vpoint)
	#	update_tag if changed
	#	changed
	#end

	#def set_ready(vpoint)
	#	changed = false
	#	unless @ready.index(vpoint)
	#		@ready.push(vpoint)
	#		changed = true
	#	end
	#	changed = true if @sprep.delete(vpoint)
	#	changed = true if @mprep.delete(vpoint)
	#	changed = true if @fault.delete(vpoint)
	#	update_tag if changed
	#	changed
	#end

	#def set_fault(vpoint)
	#	changed = false
	#	unless @fault.index(vpoint)
	#		@fault.push(vpoint)
	#		changed = true
	#	end
	#	changed = true if @sprep.delete(vpoint)
	#	changed = true if @mprep.delete(vpoint)
	#	changed = true if @ready.delete(vpoint)
	#	update_tag if changed
	#	changed
	#end

	def shift_mprep_addr(addr)
		changed = false
		@sprep.reject! {|vp|
			if vp.address == addr
				@mprep.push(vp)
				changed = true
			end
		}
		@ready.reject! {|vp|
			if vp.address == addr
				@mprep.push(vp)
				changed = true
			end
		}
		@fault.reject! {|vp|
			if vp.address == addr
				@mprep.push(vp)
				changed = true
			end
		}
		update_tag if changed
		changed
	end

	def shift_ready_addr(addr)
		changed = false
		@sprep.reject! {|vp|
			if vp.address == addr
				@ready.push(vp)
				changed = true
			end
		}
		@mprep.reject! {|vp|
			if vp.address == addr
				@ready.push(vp)
				changed = true
			end
		}
		@fault.reject! {|vp|
			if vp.address == addr
				@ready.push(vp)
				changed = true
			end
		}
		update_tag if changed
		changed
	end

	def shift_fault_addr(addr)
		changed = false
		@sprep.reject! {|vp|
			if vp.address == addr
				@fault.push(vp)
				changed = true
			end
		}
		@mprep.reject! {|vp|
			if vp.address == addr
				@fault.push(vp)
				changed = true
			end
		}
		@ready.reject! {|vp|
			if vp.address == addr
				@fault.push(vp)
				changed = true
			end
		}
		update_tag if changed
		!changed.nil?
	end

	def remove(vpoint)
		changed = false
		changed = true if @sprep.delete(vpoint)
		changed = true if @mprep.delete(vpoint)
		changed = true if @ready.delete(vpoint)
		changed = true if @fault.delete(vpoint)
		update_tag if changed
		changed
	end

	def to_msgpack(out = '')
		[@tag, @sprep, @mprep, @ready, @fault].to_msgpack(out)
	end

	def self.from_msgpack(obj)
		tag = obj[0]
		sprep = obj[1].map {|vp| VPoint.from_msgpack(vp) }
		mprep = obj[2].map {|vp| VPoint.from_msgpack(vp) }
		ready = obj[3].map {|vp| VPoint.from_msgpack(vp) }
		fault = obj[4].map {|vp| VPoint.from_msgpack(vp) }
		RoutingSource.new(sprep, mprep, ready, fault, tag)
	end

	private
	def update_tag
		@sprep.sort!
		@mprep.sort!
		@ready.sort!
		@fault.sort!
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
		@sprep = []
		@mprep = []
		@ready = []
		@fault = []
	end

	def update_qs(qsaddr, rsrc)
		if (old = @qs[qsaddr]) && old.tag == rsrc.tag
			return false
		end
		@qs[qsaddr] = rsrc
		$log.TRACE "updating QS #{qsaddr} of #{@qs.size} QSs"
		@qs.each_pair {|qsaddr, rsrc|
			$log.TRACE "  #{qsaddr}:"
			$log.TRACE "    sprep: #{rsrc.sprep.map {|vp| vp.address }.uniq.sort.join(' ')}"
			$log.TRACE "    mprep: #{rsrc.mprep.map {|vp| vp.address }.uniq.sort.join(' ')}"
			$log.TRACE "    ready: #{rsrc.ready.map {|vp| vp.address }.uniq.sort.join(' ')}"
			$log.TRACE "    fault: #{rsrc.fault.map {|vp| vp.address }.uniq.sort.join(' ')}"
		}
		update
	end

	def tag_of_qs(qsaddr)
		if rs = @qs[qsaddr]
			rs.tag
		else
			nil
		end
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
	def update(majority_border = nil)
		#majority_border ||= @qs.size/ 2  # /2切り捨て
		majority_border ||= ((@qs.size-1) / 2) + 1  # /2切り上げ
		sprep = Hash.new {|hash,key| hash[key] = 0 }  # VPoint => num
		mprep = Hash.new {|hash,key| hash[key] = 0 }  # VPoint => num
		ready = Hash.new {|hash,key| hash[key] = 0 }  # VPoint => num
		fault = Hash.new {|hash,key| hash[key] = 0 }  # VPoint => num
		@qs.each_pair {|qsaddr, rsrc|
			#(rsrc.fault + rsrc.ready + rsrc.mprep + rsrc.sprep).each {|vp| sprep[vp] += 1 }
			#(rsrc.fault + rsrc.ready + rsrc.mprep).each {|vp| mprep[vp] += 1 }
			#(rsrc.fault + rsrc.ready).each {|vp| ready[vp] += 1 }
			#(rsrc.fault).each {|vp| fault[vp] += 1 }
			rsrc.sprep.each {|vp| sprep[vp] += 1 }
			rsrc.mprep.each {|vp| sprep[vp] += 1; mprep[vp] += 1 }
			rsrc.ready.each {|vp| sprep[vp] += 1; mprep[vp] += 1; ready[vp] += 1 }
			rsrc.fault.each {|vp| sprep[vp] += 1; mprep[vp] += 1; ready[vp] += 1; fault[vp] += 1 }
		}
		sprep.reject! {|vp, num| num < majority_border }
		mprep.reject! {|vp, num| num < majority_border }
		ready.reject! {|vp, num| num < majority_border }
		fault.reject! {|vp, num| num < majority_border }
		sprep = sprep.keys.sort
		mprep = mprep.keys.sort
		ready = ready.keys.sort
		fault = fault.keys.sort
		sprep.reject! {|vp| (mprep + ready + fault).include?(vp) }
		mprep.reject! {|vp| (ready + fault).include?(vp) }
		ready.reject! {|vp| (fault).include?(vp) }
		if @sprep != sprep || @mprep != mprep || @ready != ready || @fault != fault
			@route.reset(sprep, mprep, ready, fault)
			$log.DEBUG "route changed with border = #{majority_border}:"
			$log.DEBUG "  sprep: #{sprep.map {|vp| vp.address }.uniq.sort.join(' ')}"
			$log.DEBUG "  mprep: #{mprep.map {|vp| vp.address }.uniq.sort.join(' ')}"
			$log.DEBUG "  ready: #{ready.map {|vp| vp.address }.uniq.sort.join(' ')}"
			$log.DEBUG "  fault: #{fault.map {|vp| vp.address }.uniq.sort.join(' ')}"
			@active_nodes = (sprep + mprep + ready).map {|vp| vp.address }.uniq.sort
			@sprep = sprep
			@mprep = mprep
			@ready = ready
			@fault = fault
			true
		else
			false
		end
	end
end

