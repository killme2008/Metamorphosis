##  @description : metaq client for ruby
##  @author:  dennis zhuang <killme2008@gmail.com>
##  @license: MIT licensed.

require 'rubygems'
require 'uri'
require 'socket'
require 'zookeeper'
require 'thread'

module Metaq

  #Metaq message
  class Message
    attr_accessor :id, :topic, :data, :attribute, :partition, :flag
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

  class ConnectionDeadError < StandardError
  end

  class NetworkError < StandardError
  end

  class MetaError < StandardError
  end

  class SendResult
    attr_accessor :success,:partition,:offset,:error

    def initialize(success,partition,offset,error=nil)
      @success = success
      @partition = partition
      @offset = offset
      @error = error
    end

    def to_s
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

    def to_s
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

    def to_s
      return "Broker[uri=#{@broker_uri}]"
    end

  end


  DEAD_RETRY = 5  # number of seconds before retrying a dead server.
  SOCKET_TIMEOUT = 10  #  number of seconds before sockets timeout.
  class Connection
    attr_accessor :uri, :socket

    def initialize(uri,dead_retry=DEAD_RETRY,socket_timeout=SOCKET_TIMEOUT,debug=true)
      @uri = uri
      @dead_retry = dead_retry
      @socket = nil
      @socket_timeout = socket_timeout
      @debug = debug
      @deaduntil = 0

      get_socket()
    end

    def close()
      if @socket
        @socket.close
        @socket = nil
      end
    end

    def send_packet(pkt)
      sock=get_socket()
      raise ConnectionDeadError if sock.nil?
      raise NetworkError if sock.send(pkt,0) != pkt.size
    end

    def readline
      sock=get_socket
      raise ConnectionDeadError if sock.nil?
      return sock.readline
    end

    def recv(len)
      sock=get_socket
      raise ConnectionDeadError if sock.nil?
      return sock.read(len)
    end

    def mark_dead(reason)
      debug("#{@uri}: #{reason}.  marking dead.")
      @deaduntil = Time.now() + @dead_retry
      close()
    end

    def debug(msg)
      if @debug
        STDERR.puts "[DEBUG]Connection[#{@uri}]: #{msg}"
      end
    end

    def get_socket()
      return nil if check_dead()
      return @socket if @socket

      begin
        uri=URI.parse(@uri)
        s=TCPSocket.open(uri.host,uri.port)
      rescue Exception => e
        mark_dead("connect failed:#{e.message}")
        return nil
      end
      debug("Connect to #{@uri} successfully.")
      @socket = s
      return @socket
    end

    def check_dead()
      return true if @deaduntil != 0  and @deaduntil > Time. now()
      @deaduntil = 0
      return false
    end

  end

  #Returns a round robin selector
  def self.get_round_robin_selector
    ji = JavaInt.new
    Proc.new do | topic, partitions, msg|
      raise MetaError.new("There is no available parition for topic #{topic} right now") if partitions.nil? or partitions.size == 0
      partitions[ ji.inc_and_get() % partitions.size]
    end
  end

  class MessageProducer
    attr_accessor :topic

    def initialize(topic, opts={})
      @topic = topic
      opts.each do |key, value|
        instance_variable_set "@#{key}", value
      end
      @opaque_counter = JavaInt.new
      @zk_servers ||= "localhost:2181"
      @partition_selector ||= Metaq.get_round_robin_selector
      @zk_timeout ||= 10000
      @zk_root ||= "/meta"
      @dead_retry ||= DEAD_RETRY
      @socket_timeout ||= SOCKET_TIMEOUT
      @debug ||= true

      raise MetaError,"Topic is nil" if @topic.nil?
      raise MetaError,"zookeeper servers is nil" if @zk_servers.nil? or @zk_servers.empty?

      @zk = Zookeeper.new(@zk_servers)
      @lock = Mutex.new
      @broker_topic_path = "#{@zk_root}/brokers/topics"
      @broker_ids_of_path = "#{@zk_root}/brokers/ids"
      @broker_hash = Hash.new
      @conn_hash = Hash.new
      @partition_list = Array.new
      update_broker_infos()
    end

    def send(msg)
      raise MetaError.new("Message is nil") if msg.nil?
      topic = msg.topic
      raise MetaError.new("Expect topic #{@topic},but was #{topic}") if topic != @topic
      data = msg.data
      raise MetaError.new("message data is none") if data.nil?
      partition = @partition_selector.call(topic, @partition_list, msg)
      raise MetaError.new("There is no avaiable partition for topic #{topic}") if partition.nil?
      conn = @conn_hash[partition.broker_id]
      raise MetaError.new("There is no avaiable server right now for topic #{topic}and partition #{partition.partition}") if conn.nil?
      opaque = @opaque_counter.inc_and_get()
      cmd = msg.encode(partition.partition,opaque)

      begin
        conn.send_packet(cmd)
        head = conn.readline()
        tmps = head.split(" ")
        status = tmps[1].to_i
        bodylen = tmps[2].to_i
        resp_opaque = tmps[3].to_i
        body = conn.recv(bodylen)
        if  body.nil? or body.size != bodylen
          conn.mark_dead("Response format error,expect body length is #{bodylen},but is #{body.size}")
          return SendResult.new(false,nil, -1, error="network error")
        end
        if resp_opaque != opaque
          conn.mark_dead("Response opaque is not equals to request opaque")
          return SendResult.new(false, nil, -1, error="network error")
        end
        if status == HttpStatus::Success
          tmps = body.split(" ")
          msg.id = tmps[0].to_i
          offset = tmps[2].to_i
          msg.partition = partition
          return SendResult.new(true, partition, offset)
        else
          return SendResult.new(false, nil, -1, error=body)
        end
      rescue ConnectionDeadError
        conn.mark_dead("Connection was broken")
        return SendResult.new(false, nil, -1, error="connection was broken")
      rescue  Exception => e
        return SendResult.new(false, nil, -1, error=e.message)
      end
    end

    def to_s
      "MessageProducer[topic =#{topic},  brokers=#{@broker_hash}"
    end

    def close()
      safe_zk_close()
      @conn_hash.values do |conn|
        conn.close()
      end
    end

    ##private methods
    private

    def safe_zk_close()
      begin
        @zk.close() if @zk
      rescue
        #ignore
      end
    end

    def safe_zk_get(path, count=0)
      begin
        return @zk.get({ :path => path })
      rescue Exception => e
        if count >= 3
          raise e
        else
          safe_zk_close()
          @zk = Zookeeper.new(@zk_servers)
          return safe_zk_get(path, count.succ)
        end
      end
    end

    def safe_zk_get_children(path, watcher, count=0)
      begin
        return @zk.get_children({:path => path, :watcher => watcher })
      rescue Exception => e
        if count >= 3
          raise e
        else
          safe_zk_close()
          @zk = Zookeeper.new(@zk_servers)
          return safe_zk_get_children(path, watcher, count.succ)
        end
      end
    end

    def debug(msg)
      STDERR.puts("[meta-producer-debug]:#{msg}")  if @debug
    end

    def update_broker_infos()
      @lock.synchronize do
        debug("begin to update broker infos from zookeeper with topic #{@topic}")
        topic_path = "#{@broker_topic_path}/#{@topic}"
        watcher = nil
        children = safe_zk_get_children(topic_path, watcher)[:children]
        broker_hash = Hash.new
        partition_list = Array.new
        if children and children.size > 0
          children.select{ |child| child =~ /.*-m$/ }.each do |child|
            broker_id = child[0,child.index("-m")].to_i
            broker = get_broker_by_id(broker_id)
            partition_list.concat(get_parts(child, broker_id))
            broker_hash[broker_id] = broker
            partition_list.sort!
          end
        end
        update_conn_hash(broker_hash)
        @broker_hash = broker_hash
        @partition_list = partition_list
        debug("New brokers for topic #{@topic}:#{@broker_hash}")
        debug("New partition list for topic#{topic}:#{@partition_list}")
        debug("End to update broker infos from zookeeper with topic #{topic}")
      end
    end

    def update_conn_hash(new_broker_hash)
      for broker_id in @broker_hash.keys()
        #broker is both in old brokers and new brokers
        if new_broker_hash[broker_id]
          #if broker uri changed
          if new_broker_hash[broker_id].broker_uri != @broker_hash[broker_id].broker_uri
            conn = @conn_hash[broker_id]
            #close old connection
            close_conn(conn)
            new_uri = new_broker_hash[broker_id].broker_uri
            #connect to new broker
            debug("connecting to #{new_uri}")
            @conn_hash[broker_id] = Connection.new(new_uri, @dead_retry, @socket_timeout, @debug)
          end
        else
          #Broker is not in new brokers,close it.
          conn = @conn_hash[broker_id]
          close_conn(conn)
        end
      end

      new_broker_hash.keys().each do | broker_id|
        #A new broker,we must connect it.
        if @broker_hash[broker_id].nil?
          new_uri = new_broker_hash[broker_id].broker_uri
          debug("connecting to #{new_uri}")
          @conn_hash[broker_id] = Connection.new(new_uri, @dead_retry, @socket_timeout, @debug)
        end
      end

    end

    def close_conn(conn)
      if conn
        @conn_hash.delete broker_id
        debug("Closing #{conn.uri}")
        conn.close()
      end
    end

    def get_parts(child,broker_id)
      n_parts = safe_zk_get("#{@broker_topic_path}/#{@topic}/#{child}")[:data].to_i
      (0..n_parts-1).collect do | n |
        Partition.new(broker_id, n)
      end
    end

    def get_broker_by_id(broker_id)
      broker_uri,_ = safe_zk_get("#{@broker_ids_of_path}/#{broker_id}/master")[:data]
      return Broker.new(broker_id, broker_uri)
    end

  end

end

if __FILE__ == $0
  producer =Metaq::MessageProducer.new("meta-test",{ :zk_root => "/avos-fetch-meta" })
  msg =Metaq::Message.new("meta-test","hello world")
  p producer.send(msg)
  producer.close
end
