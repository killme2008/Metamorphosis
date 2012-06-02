$:.unshift File.join(File.dirname(__FILE__),'..','lib')
require 'test/unit'
require 'metaq'
include Metaq
class MessageTest < Test::Unit::TestCase
  def test_encode
    msg = Message.new("test", nil)
    assert "test" == msg.topic
    assert msg.data.nil?
    assert msg.attribute.nil?
    assert "put test 1 0 0 99\r\n" == msg.encode(1,99)
    msg = Message.new("test","hello world","attribute")
    assert "attribute" == msg.attribute
    encoded = msg.encode(1,99)
    assert 1 == msg.flag
    head, body = encoded.split("\r\n")
    assert "put test 1 24 1 99" == head
    attr_len = body[0,4].unpack("N")[0]
    data = body[4 .. -1]
    assert "attribute".size == attr_len
    assert "attribute" == data[0, attr_len]
    assert "hello world" == data[attr_len .. -1]
  end
end
class JavaIntTest < Test::Unit::TestCase
  def test_inc_and_get
    ji=JavaInt.new
    assert JavaInt::MIN + 1 == ji.inc_and_get()
    assert JavaInt::MIN + 2 == ji.inc_and_get()
    ji.value = JavaInt::MAX
    assert JavaInt::MIN == ji.inc_and_get()
  end
end

class PartitionTest < Test::Unit::TestCase
  def test_cmp
    p1 = Partition.new(0, 1)
    assert 0 == p1.broker_id
    assert 1 == p1.partition
    assert "0-1" == p1.to_s

    #test compare
    p2 = Partition.new(0, 3)
    assert "0-3" == p2.to_s
    p3 = Partition.new(1, 0)
    assert "1-0" == p3.to_s
    print p1 <=> p2
    assert (p1 <=> p2) < 0
    assert (p1 <=> p3) < 0
    assert (p2 <=> p3) < 0
  end
end

class PartitionSelectorTest < Test::Unit::TestCase
  def test_round_robin_selector
    f = Metaq.get_round_robin_selector()
    assert f.is_a? Proc
    begin
      f.call("test",nil,nil)
    rescue MetaError => e1
      #do nothing
    rescue Exception => e2
      fail("expected an MetaError")
    end
    p1,p2,p3 = Partition.new(0, 0), Partition.new(0, 1), Partition.new(0, 2)
    partitions = [ p1,p2,p3 ]
    assert p3 == f.call("test",partitions,nil)
    assert p1 == f.call("test",partitions,nil)
    assert p2 == f.call("test",partitions,nil)
  end
end

class ConnTest < Test::Unit::TestCase
  def test_all
    conn = Connection.new("meta://localhost:8123")
    assert (not conn.check_dead())
    assert conn.get_socket()
    assert conn.socket
    conn.send_packet("unknown\r\n")
    begin
      conn.readline()
      fail("Connection was broken")
    rescue Exception =>e
      print e
    end
    conn.mark_dead("Closed by broker")
    assert conn.check_dead()
    assert (not conn.socket)
    assert (not conn.get_socket())
    sleep(6)
    assert (not conn.check_dead())
    assert conn.get_socket()
    conn.send_packet("put meta-test 0 5 0 99\r\nhello")
    line=conn.readline()
    assert line
    _, status, vlen, opaque = line.split(" ")
    assert status == "200"
    assert opaque == "99"
    resp_body = conn.recv(vlen.to_i)
    print resp_body
    conn.close()
  end
end

