
require 'rubygems'
require 'eventmachine'

module Metaq
  #Metaq message
  class Message
    attr_accessor :id,:topic,:data,:attribute,:partition
    def initialize(topic, data, attribute=nil)
      @topic = topic
      @data = data
      @attribute = attribute
      @id = 0
      @flag = 0
      @partition = -1
    end

    def encode(partition, opaque)
      payload = encode_payload()
      payload="" if payload.nil?
      vlen = payload.size
      return "put #{@topic} #{partition} #{vlen} #{@flag} #{opaque}\r\n#{payload}"
    end

    private

    def encode_payload()
      return @data if @attribute.nil?
      attr_len = [@attribute.length].pack("N")
      @flag= @flag & 0xFFFFFFFE | 1
      return "#{attr_len}#{@attribute}#{@data}"
    end

  end

  class ParserError < StandardError
  end

  class SendResult
    attr_accessor :success,:partition,:offset,:error

    def initialize(success,partition,offset,error=nil)
      @success = success
      @partition = partition
      @offset = offset
      @error = error
    end

    def inspect()
      return "SendResult[success=#{@success},partition=#{@partition},offset=#{@offset},error=#{@error}]"
    end
  end

  ##Internal classes
  class JavaInt
    MIN = -2147483648
    MAX = 2147483647

    attr_accessor :value

    def initialize()
      @value=MIN
    end

    def inc_and_get()
      @value+=1
      if @value>=MAX
        @value=MIN
      end
      return @value
    end

  end

  class HttpStatus
    BadRequest = 400
    NotFound = 404
    Forbidden = 403
    Unauthorized = 401
    InternalServerError = 500
    ServiceUnavilable = 503
    GatewayTimeout = 504
    Success = 200
    Moved = 301
  end

  class Partition
    attr_accessor :broker_id,:partition

    def initialize(broker_id, partition)
      @broker_id = broker_id
      @partition = partition
    end

    def inspect()
      return "#{@broker_id}-#{@partition}"
    end

    def  <=>(other)
      if @broker_id != other.broker_id
        return @broker_id <=> other.broker_id
      else
        return @partition <=> other.partition
      end
    end
  end

  class Broker
    attr_accessor :broker_id,:broker_uri

    def initialize(broker_id, broker_uri)
      @broker_id = broker_id
      @broker_uri = broker_uri
    end

    def insepct()
      return @broker_uri
    end
  end

  class Response
    attr_accessor :status, :length, :opaque, :msg
    def initialize(status, length, opaque)
      @status = status
      @opaque = opaque
      @length = length
    end
  end

  # A socket connection to metaq broker
  # thanks to https://github.com/astro/remcached/
  class Connection < EventMachine::Connection
    def self.connect(host, port=8123, &connect_callback)
      df = EventMachine::DefaultDeferrable.new
      df.callback &connect_callback

      EventMachine.connect(host, port, self) do |me|
        me.instance_eval {
          @host, @port = host, port
          @connect_deferrable = df
        }
      end
    end

    def connected?
      @connected
    end

    def reconnect
      @connect_deferrable = EventMachine::DefaultDeferrable.new
      super @host, @port
      @connect_deferrable
    end

    def post_init
      @recv_buf = ""
      @recv_state = :header
      @connected = false
      @keepalive_timer = nil
    end

    def connection_completed
      @connected = true
      @connect_deferrable.succeed(self)

      @last_receive = Time.now
      @keepalive_timer = EventMachine::PeriodicTimer.new(1, &method(:keepalive))
    end

    RECONNECT_DELAY = 5
    RECONNECT_JITTER = 5
    def unbind
      @keepalive_timer.cancel if @keepalive_timer

      @connected = false
      EventMachine::Timer.new(RECONNECT_DELAY + rand(RECONNECT_JITTER),
                          method(:reconnect))
    end

    RECEIVE_TIMEOUT = 10
    KEEPALIVE_INTERVAL = 5
    def keepalive
      if @last_receive + RECEIVE_TIMEOUT <= Time.now
        p :timeout
        close_connection
      elsif @last_receive + KEEPALIVE_INTERVAL <= Time.now
        send_keepalive
      end
    end

    def send_packet(pkt)
      send_data pkt
    end

    Cdelimiter = "\r\n".freeze
    def receive_data(data)
      @recv_buf += data
      @last_receive = Time.now
      done = false
      while not done

        if @recv_state == :header && index = @recv_buf.index(Cdelimiter)
          line = @recv_buf.slice!(0,index+2)
          tmps = line.split(" ")
          raise ParserError if tmps.length != 4
          @received = Response.new(tmps[1].to_i, tmps[2].to_i, tmps[3].to_i)
          @recv_state = :body

        elsif @recv_state == :body && @recv_buf.length >= @received.length
          @received.msg = @recv_buf.slice!(0,@received.length)
          receive_packet(@received)
          @received = nil
          @recv_state = :header
        else
          done = true
        end
      end
    end
  end

  class Client < Connection
    def post_init
      super
      @opaque_counter = JavaInt.new
      @pending = {}
      @keepalive_failures = 0
    end

    def unbind
      super
      @pending.each do |opaque, callback|
        callback.call :status => Errors::DISCONNECTED
      end
      @pending = {}
    end

    def send_request(msg, &callback)
      raise ArgumentError unless callback
      partition = 0
      opaque = @opaque_counter.inc_and_get
      pkt = msg.encode(partition,opaque)
      @pending[opaque] = callback
      send_packet pkt
    end

    def cast_send_result(response)
      if response.status == HttpStatus::Success:
          tmps = response.msg.split(" ")
        partition = tmps[2].to_i
        offset = tmps[3].to_i
        msg_id = tmps[1].to_i
        return SendResult.new(true, partition, offset)
      else
        return SendResult.new(false, -1, -1, response.msg)
      end
    end

    def receive_packet(response)
      pending_callback = @pending[response.opaque]
      pending_opaque = response.opaque
      if pending_callback
        @pending.delete pending_opaque
        begin
          pending_callback.call(cast_send_result(response))
        rescue Exception => e
          $stderr.puts "#{e.class}: #{e}\n" + e.backtrace.join("\n")
        end
      end
    end

    def send_keepalive
      opaque = @opaque_counter.inc_and_get
      send_packet "version #{opaque}\r\n" do | ret|
        if not ret.success
          @keepalive_failures += 1
          if @keepalive_failures > 5
            p "Heartbeat failed for #{@keepalive_failures} times"
          end
        else
          @keepalive_failures = 0
        end
      end
    end

  end

end

if __FILE__ == $0
  EM.run do
    msg = Metaq::Message.new("meta-test","hello","fuck")
    producer = Metaq::Client.connect("localhost")
    producer.send_request(msg) do | result|
      p result
    end
  end
end
