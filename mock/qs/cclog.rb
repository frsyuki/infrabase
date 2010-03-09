
#$cclog_out   = STDOUT
#$cclog_color = true
#$cclog_Ename = "CLe"
#$cclog_Ever  = "00"
#$cclog_Erel  = "00"
#$cclog_Eenv  = "RB"
#$cclog_Eformat = "%05d"

class CCLog
	module TTYColor
		RESET   = "\033]R"
		CRE     = "\033[K"
		CLEAR   = "\033c"
		NORMAL  = "\033[0;39m"
		RED     = "\033[1;31m"
		GREEN   = "\033[1;32m"
		YELLOW  = "\033[1;33m"
		BLUE    = "\033[1;34m"
		MAGENTA = "\033[1;35m"
		CYAN    = "\033[1;36m"
		WHITE   = "\033[1;37m"
	end

	def initialize(out = $stdout, use_color = true)
		if use_color
			@color_trace = TTYColor::BLUE
			@color_debug = TTYColor::WHITE
			@color_info  = TTYColor::GREEN
			@color_warn  = TTYColor::YELLOW
			@color_error = TTYColor::MAGENTA
			@color_fatal = TTYColor::RED
			@color_reset = TTYColor::NORMAL
		else
			@color_trace = ''
			@color_debug = ''
			@color_info  = ''
			@color_warn  = ''
			@color_error = ''
			@color_fatal = ''
			@color_reset = ''
		end
		@code_format = " [code:CCLe.00.00.%05d.RB]"  # NAME.RELEASE.VERSION.CODE.ENV"
		@out = $stdout
	end
	attr_accessor :code_format
	attr_accessor :out

	def code(*args)
		#" [code:#{$cclog_Ename}.#{$cclog_Erel}.#{$cclog_Ever}.#{$cclog_Eformat%id}.#{$cclog_Eenv}]"
		@code_format % args
	end

	def TRACE(*args)
		puts "#{@color_trace}#{caller_line(1,true)}: #{args.join('')}#{@color_reset}"
	end

	def DEBUG(*args)
		puts "#{@color_debug}#{caller_line(1,true)}: #{args.join('')}#{@color_reset}"
	end

	def INFO(*args)
		puts "#{@color_info}#{caller_line(1,true)}: #{args.join('')}#{@color_reset}"
	end

	def WARN(*args)
		puts "#{@color_warn}#{caller_line(1)}: #{args.join('')}#{@color_reset}"
	end

	def ERROR(*args)
		puts "#{@color_error}#{caller_line(1)}: #{args.join('')}#{@color_reset}"
	end

	def FATAL(*args)
		puts "#{@color_fatal}#{caller_line(1)}: #{args.join('')}#{@color_reset}"
	end

	def caller_line(level, debug = false)
		line = caller(level+1)[0]
		if match = /^(.+?):(\d+)(?::in `(.*)')?/.match(line)
			if debug
				"#{match[1]}:#{match[2]}:#{match[3]}"
			else
				"#{match[1]}:#{match[2]}"
			end
		else
			""
		end
	end

	def puts(msg)
		@out.puts(msg)
		@out.flush
		nil
	end
end

