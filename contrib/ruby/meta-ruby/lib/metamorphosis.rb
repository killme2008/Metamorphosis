
require 'rubygems'
require 'eventmachine'

module Metaq
  #Metaq message
  class Message
    attr_accessor :id,:topic,:data,:attribute,:partition
    def initialize(topic,data,attribute=nil)
      @topic=topic
      @data=data
      @attribute=attribute
      @id=0
      @flag=0
      @partition=-1
    end
  end

  ##Internal classes
  class JavaInt
    MIN=-2147483648
    MAX=2147483647

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

    def initialize(broker_id,partition)
      @broker_id=broker_id
      @partition=partition
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

    def initialize(broker_id,broker_uri)
      @broker_id=broker_id
      @broker_uri=broker_uri
    end

    def insepct()
      return @broker_uri
    end
  end

  
end

if __FILE__ == $0
  ji=Metaq::JavaInt.new

  p ji.inc_and_get
end
