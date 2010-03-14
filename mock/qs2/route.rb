module Route


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

class Stage
	# ready
	# fault
	# join
	# join  + fault
	# leave + ready
	# leave + fault

	READY = 0b0001
	FAULT = 0b0010
	JOIN  = 0b0100
	LEAVE = 0b1000

	# FIXME stand-by

	def initialize(flag = 0)
		@flag = flag
	end

	def set_ready
		return false if ready?
		@flag |= READY
		@flag &= ~FAULT
		@flag &= ~JOIN
		true
	end

	def set_fault
		return false if fault?
		@flag |= FAULT
		@flag &= ~READY
		true
	end

	def set_join
		return false if join?
		@flag |= JOIN
		@flag &= ~READY
		@flag &= ~LEAVE
		@flag &= ~FAULT
		true
	end

	def set_leave
		return nil if !ready? && !fault?
		return false if leave?
		@flag |= LEAVE
		@flag &= ~JOIN
		true
	end

	def ready?
		@flag & READY != 0
	end

	def fault?
		@flag & FAULT != 0
	end

	def join?
		@flag & JOIN  != 0
	end

	def leave?
		@flag & LEAVE != 0
	end

	def active?
		ready? || join?
	end

	def to_msgpack(out = '')
		@flag.to_msgpack(out)
	end

	def self.from_msgpack(obj)
		Stage.new(obj.to_i)
	end

	def eql?(o)
		o.class == Stage && @flag == o.get
	end

	def hash
		@flag.hash
	end

	def ==(o)
		eql?(o)
	end

	def get; @flag; end
end

class Node
	def initialize(address, name, vids, stage = Stage.new)
		@address = address  # Address
		@name    = name     # String
		@vids    = vids     # [int]
		@stage   = stage    # Stage
		#@groups = groups
	end
	attr_reader :name, :address, :vids, :stage

	def to_msgpack(out = '')
		[@address, @vids, @stage, @name].to_msgpack(out)
	end

	def self.from_msgpack(obj)
		addr  = MessagePack::RPC::Address.load(obj[0])
		vids  = obj[1].to_a
		stage = Stage.from_msgpack(obj[2])
		name  = obj[3].to_s
		Node.new(addr, name, vids, stage)
	end

	def set_ready; @stage.set_ready; end
	def set_fault; @stage.set_fault; end
	def set_join;  @stage.set_join;  end
	def set_leave; @stage.set_leave; end
	def ready?;    @stage.ready?;    end
	def fault?;    @stage.fault?;    end
	def join?;     @stage.join?;     end
	def leave?;    @stage.leave?;    end
	def active?;   @stage.active?;   end

	def eql?(o)
		# FIXME nameを無視
		o.class == Node && @address == o.address &&
			@vids == o.vids && @stage == o.stage
	end

	def hash
		@address.hash ^ @vids.hash ^ @stage.hash
	end

	def ==(o)
		eql?(o)
	end

	def to_s
		str = "[#{@name} #{@vids.size} #{@address}"
		str << " join"  if @stage.join?
		str << " ready" if @stage.ready?
		str << " fault" if @stage.fault?
		str << " leave" if @stage.leave?
		str << "]"
		str
	end
end

class RoutingSource
	def initialize(vec = [], tag = nil)
		@vec = vec
		@map = RoutingSource.vec_to_map(vec)
		@tag = tag
		update_tag unless @tag
	end
	attr_reader :tag

	def each(&block)
		@vec.each(&block)
	end

	def add_node(addr, name, vids)
		if n = @map[addr]
			# FIXME
		end
		@map[addr] = Node.new(addr, name, vids)
		update
		true
	end

	def set_ready(addr)
		if n = @map[addr]
			changed = n.stage.set_ready
			update if changed
			return changed
		end
		nil
	end

	def set_fault(addr)
		if n = @map[addr]
			if !n.stage.ready? && !n.stage.fault?
				@map.delete(addr)
				changed = true
			else
				changed = n.stage.set_fault
			end
			update if changed
			return changed
		end
		nil
	end

	def set_join(addr)
		if n = @map[addr]
			changed = n.stage.set_join
			update if changed
			return changed
		end
		nil
	end

	def set_leave(addr)
		if n = @map[addr]
			changed = n.stage.set_leave
			update if changed
			return changed
		end
		nil
	end

	def remove(addr)
		if @map.delete(addr)
			true
		else
			nil
		end
	end

	def active_nodes
		@vec.select {|node| node.active? }.map {|node| node.address }
	end

	def nodes
		@vec.dup
	end

	def to_msgpack(out = '')
		[@tag, @vec].to_msgpack(out)
	end

	def self.from_msgpack(obj)
		tag = obj[0]
		vec = obj[1].to_a.map {|o| Node.from_msgpack(o) }
		RoutingSource.new(vec, tag)
	end

	private
	def update
		update_vec
		update_tag
		true
	end

	def update_tag
		# FIXME nameは無視？
		@tag = Hash.hash_generic(@vec.to_msgpack)
	end

	def update_vec
		@vec = @map.map {|a,n| n }.sort_by {|n| n.address }
	end

	def self.vec_to_map(vec)
		map = Hash.new
		vec.each {|node| map[node.address] = node }
		map
	end
end

class RoutingTable
	BUCKET_SIZE = 1<<13

	def initialize
		@vector = []
	end

	def reset(nodes)
		# FIXME create RoutingTable
	end

	def find(key, num)
		raise "no node" if @vector.empty?
		idx = Hash.hash_key(key) % @vector.size
		result = []
		i = 0
		while i < @vector.size
			node = @vector[ (idx + i) % @vector.size ]
			unless result.include?(node)
				result << node
				num -= 1
				break if num == 0
			end
			i += 1
		end
		result
	end

	# FIXME optimize for num=1 or num=2
end

class DoubleRoutingTable
	def initialize
		@current = RoutingTable.new
		@future  = RoutingTable.new
	end

	def reset(nodes)
		current = []
		future  = []
		nodes.each {|n|
			if n.ready? || n.fault?
				current << n
			end
			if (n.ready? || n.fault? || n.join?) && !n.leave?
				future << n
			end
		}
		@current.reset(current)
		@future.reset(future)
		nil
	end

	def current_master(key)
		@current.find(key,1)[0]
	end

	def current_slave(key)
		@current.find(key,2)[1]
	end

	def current_master_slave(key)
		@current.find(key,2)
	end
end

class RoutingManager
	def initialize
		@table = DoubleRoutingTable.new
		@qs = {}   # {Address => RoutingSource}
		@actives = []   # [Node]
	end

	def set_qs_addrs(qs_addrs)
		qs_addrs.each {|addr|
			@qs[addr] = RoutingSource.new
		}
		update
	end

	def update_qs(qsaddr, rsrc)
		old = @qs[qsaddr]
		raise "unexpected QS #{qsaddr}" unless old
		return false if old.tag == rsrc.tag
		@qs[qsaddr] = rsrc
		$log.TRACE "updating QS #{qsaddr} of #{@qs.size} QSs"
		@qs.each_pair {|qsaddr, rsrc|
			$log.TRACE "  #{qsaddr}: #{rsrc.nodes.join(' ')}"
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

	private
	def update(majority_border = nil)
		#majority_border ||= @qs.size/ 2  # /2切り捨て
		majority_border ||= ((@qs.size-1) / 2) + 1  # /2切り上げ

		join  = Hash.new {|hash,key| hash[key] = 0 }  # {Address => num}
		ready = Hash.new {|hash,key| hash[key] = 0 }  # {Address => num}
		fault = Hash.new {|hash,key| hash[key] = 0 }  # {Address => num}
		leave = Hash.new {|hash,key| hash[key] = 0 }  # {Address => num}

		# {Address => {vids => num}}
		vidss = Hash.new {|hash,key| hash[key] = Hash.new {|h,k| h[k] = 0 } }

		# Address => String  # 多数決なし 後優先
		names = Hash.new {|hash,key| hash[key] = "" }

		qs_order = @qs.to_a.sort_by {|qsaddr,rsrc| qsaddr }
		qs_order.each do |qsaddr, rsrc|
			rsrc.each {|node|
				addr = node.address
				# join  => +join
				# ready => +join  +ready
				# fault => +ready +fault
				# leave => +leave
				if node.join? || node.ready?
					join[addr]  += 1
				end
				if node.ready? || node.fault?
					ready[addr] += 1
				end
				if node.fault?
					fault[addr] += 1
				end
				if node.leave?
					leave[addr] += 1
				end
				vidss[addr][node.vids] += 1
				names[addr] = node.name
			}
		end

		[join, ready, fault, leave].each {|arr|
			arr.reject! {|addr, num| num < majority_border }
		}

		nodes = []

		vidss.each_pair {|addr, vids_num|
			vids = vids_num.to_a.sort_by {|vids,num| num }.last[0]
			name = names[addr]
			nodes << Node.new(addr, name, vids)
		}
		nodes.sort_by {|node| node.address }

		nodes.each {|node|
			# 優先順位 join -> ready -> fault -> leave
			addr = node.address
			node.set_join  if join.include?(addr)
			node.set_ready if ready.include?(addr)
			node.set_fault if fault.include?(addr)
			node.set_leave if leave.include?(addr)
		}

		actives = []
		nodes.each {|node|
			if node.active?
				@actives.push(node)
			end
		}
		@actives = actives.sort_by {|node| node.address }

		if nodes != @nodes  # nameを無視して比較
			@nodes = nodes
			$log.DEBUG "route changed with border = #{majority_border}:"
			@nodes.each {|node|
				$log.DEBUG "  #{node}"
			}
			return true
		else
			@nodes = nodes
			return false
		end
	end
end


end  # module Route

