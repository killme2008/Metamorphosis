from metaq.producer import Message,MessageProducer,JavaInt,Partition,_Error,get_round_robin_selector,Conn

import unittest
import struct
import time

class JavaIntTest(unittest.TestCase):
    def runTest(self):
        ji=JavaInt()
        assert JavaInt._MIN + 1 == ji.increase_and_get()
        assert JavaInt._MIN + 2 == ji.increase_and_get()
        ji.value = JavaInt._MAX
        assert JavaInt._MIN == ji.increase_and_get()

class PartitionTest(unittest.TestCase):
    def runTest(self):
        p1 = Partition(0, 1)
        assert 0 == p1.broker_id
        assert 1 == p1.partition
        assert "0-1" == str(p1)

        #test compare
        p2 = Partition(0, 3)
        assert "0-3" == str(p2)
        p3 = Partition(1, 0)
        assert "1-0" == str(p3)
        print Partition.partition_comp(p1,p2)
        assert Partition.partition_comp(p1,p2) < 0
        assert Partition.partition_comp(p1,p3) < 0
        assert Partition.partition_comp(p2,p3) < 0

class PartitionSelectorTest(unittest.TestCase):
    def runTest(self):
        f = get_round_robin_selector()
        assert callable(f)
        try:
            f("test",None,None)
        except _Error:
            pass
        else:
            fail("expected an _Error")
        p1,p2,p3 = Partition(0, 0), Partition(0, 1), Partition(0, 2)
        partitions = [ p1,p2,p3 ]
        assert p3 == f("test",partitions,None)
        assert p1 == f("test",partitions,None)
        assert p2 == f("test",partitions,None)

class MessageTest(unittest.TestCase):
    def runTest(self):
        msg = Message("test", None)
        assert "test" == msg.topic
        assert msg.data is None
        assert msg.attribute is None
        assert "put test 1 0 0 99\r\n" == msg.encode(1,99)
        msg = Message("test","hello world","attribute")
        assert "attribute" == msg.attribute
        encoded = msg.encode(1,99)
        assert 1 == msg.flag
        head, body = encoded.split("\r\n")
        body_len = len(body) 
        assert "put test 1 24 1 99" == head
        attr_len, data= struct.unpack("i%ss" % (body_len - 4), body)
        assert len("attribute") == attr_len
        assert "attribute" == data[0 : attr_len]
        assert "hello world" == data[attr_len :]

class ConnTest(unittest.TestCase):
    def runTest(self):
        conn = Conn("meta://localhost:8123")
        assert not conn._check_dead()
        assert conn._get_socket()
        assert conn.socket
        assert conn.fd
        conn.send_msg("unknown\r\n")
        try:
            conn.readline()
        except Exception,e:
            pass
        else:
            fail("connection must be closed by broker")
        
        conn.mark_dead("Closed by broker")
        assert conn._check_dead()
        assert not conn.socket
        assert not conn._get_socket()
        time.sleep(6)
        assert not conn._check_dead()
        assert conn._get_socket()
        conn.send_msg("put meta-test 0 5 0 99\r\nhello")
        line=conn.readline()
        assert line
        assert line == "result 200 24 99\r\n"
        _, status, vlen, opaque = line.split(" ")
        assert status == "200"
        assert vlen == "24"
        assert opaque == "99\r\n"
        resp_body = conn.recv(24)
        print resp_body
        conn.close()

class MessageProducerTest(unittest.TestCase):
    def runTest(self):
        p = MessageProducer("meta-test")
        print p._conn_dict
        print p._broker_dict
        print p._partition_list
        assert len(p._conn_dict) == 1
        assert len(p._broker_dict) == 1
        assert len(p._partition_list) == 1
        assert p._partition_list[0].broker_id == 0
        assert p._partition_list[0].partition == 0
        send_rt = p.send(Message("meta-test","hello"))
        assert send_rt.success
        assert not send_rt.error
    
        #close connection by hand
        p._conn_dict[0].close()
        send_rt = p.send(Message("meta-test","hello"))
        assert send_rt.success
        assert not send_rt.error

        zk = p.zk
        topic_path = "%s/%s"%(p._broker_topic_path, "meta-test")
        print p._safe_zk_get_children(topic_path, None)
        zk.delete("%s/0-m" % topic_path)
        time.sleep(1)
        assert len(p._conn_dict) == 0
        assert len(p._broker_dict) == 0
        assert len(p._partition_list) == 0
        zk.create("%s/0-m" % topic_path, "1")
        time.sleep(1)
        assert len(p._conn_dict) == 1
        assert len(p._broker_dict) == 1
        assert len(p._partition_list) == 1
        assert p._partition_list[0].broker_id == 0
        assert p._partition_list[0].partition == 0
        send_rt = p.send(Message("meta-test","hello"))
        assert send_rt.success
        assert not send_rt.error
        
        p.close()
        

if __name__ == "__main__":
    unittest.main()
