
class Term
	def initialize(value)
		@value = value
	end
	attr_accessor :value
end

class TermEater
	def initialize
		@qs = {}
	end

	def feed(qsaddr, term)
		if @qs.include?(qsaddr)
			@qs[qsaddr].value = term
		else
			@qs[qsaddr] = Term.new(term)
		end
	end

	# edge triger
	def pass_next(majority_border = nil)
		return false if @qs.empty?
		before_expired = expired?(majority_border)
		@qs.each_pair {|qsaddr, term|
			if term.value > 0
				term.value -= 1
			end
		}
		if !before_expired && expired?(majority_border)
			return true  # expired
		else
			return false
		end
	end

	def expired?(majority_border = nil)
		return false if @qs.empty?
		majority_border ||= @qs.size/2  # /2切り捨て
		expired = 0
		@qs.each_pair {|qsaddr, term|
			if term.value <= 0
				expired += 1
			end
		}
		if expired > majority_border
			return true  # expired
		else
			return false
		end
	end
end

class TermFeeder
	# DS: period秒で障害検出
	# QS: period+detect秒で障害検出
	def initialize(period, detect)
		@period = period
		@initval = period + detect
		@ds = Hash.new {|hash,key| hash[key] = Term.new(@initval) }
	end

	def order(dsaddr)
		term = @ds[dsaddr]
		if term.value > 0
			term.value = @initval
			return @period
		else
			return nil
		end
	end

	def reset(dsaddr)
		@ds[dsaddr].value = @initval
		nil
	end

	# edge triger
	def pass_next
		expired = []
		@ds.each_pair {|dsaddr, term|
			if term.value > 0
				term.value -= 1
				if term.value == 0
					expired << dsaddr
				end
			end
		}
		expired
	end

	def get_expired
		expired = []
		@ds.each_pair {|dsaddr, term|
			if term.value <= 0
				expired << dsaddr
			end
		}
		expired
	end
end

