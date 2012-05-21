#Author :dennis <killme2008@gmail.com>
#Description:  a python client for metaq
"A metamorphosis client for python"
import socket
import struct
import sys
import time
import threading
from zkclient import ZKClient, zookeeper, watchmethod
from urlparse import urlparse

_DEAD_RETRY = 5  # number of seconds before retrying a dead server.
_SOCKET_TIMEOUT = 10  #  number of seconds before sockets timeout.

class _Error(Exception):
    pass

class _ConnectionDeadError(Exception):
    pass

class Partition:
    "A broker partition"
    def __init__(self, broker_id, partition):
        self.broker_id = broker_id
        self.partition = partition

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "%s-%s" % (self.broker_id, self.partition)

    def  partition_comp(x,y):    
        if x.broker_id <> y.broker_id:
            return cmp(x.broker_id, y.broker_id)
        else:
            return cmp(x.partition, y.partition)

def get_round_robin_selector():
    """
    Returns a round-robin partition selector"
    """
    ji = JavaInt()
    def round_robin_select(topic, partitions, msg):
        if partitions is None or len(partitions) == 0 :
            raise _Error("There is no available parition for topic %s right now" % (topic))
        return partitions[ji.increase_and_get() % len(partitions)]
    return round_robin_select

class Broker:
    "A metaq broker"
    def __init__(self, broker_id, broker_uri):
        self.broker_id = broker_id
        self.broker_uri = broker_uri

    def __str__(self):
        return self.broker_uri

    def __repr__(self):
        return self.__str__()

class Message:
    "A metaq message"

    def __init__(self, topic, data, attribute=None):
        """
            Create a new message with topic and data,attribute is not supported right now.
            @param topic: the topic of the message
            @param data: the payload of the message
        """
        self.id = None
        self.topic = topic
        self.data = data
        self.attribute = attribute
        self.flag = 0
        self.partition = -1

    def _encode_payload(self):
        if self.attribute is None:
            return self.data
        else:
            attr_len = struct.pack(">i",len(self.attribute))
            self.flag= self.flag & 0xFFFFFFFE | 1
            return "%s%s%s" % (attr_len, self.attribute, self.data)

    def encode(self, partition, opaque):
         """ 
             Encode message to put command
             @param partition: the partition of the message will be sent
             @param opaque:  request's opaque
         """
         payload = self._encode_payload()
         if payload is None:
             payload = ""
         vlen = len(payload)
         return "put %s %d %d %d %d\r\n%s" % (self.topic, partition, vlen, self.flag,opaque, payload)

class JavaInt:
    "Java integer"
    _MIN = -2147483648
    _MAX = 2147483647

    def __init__(self):
        self.value = JavaInt._MIN

    def increase_and_get(self):
        self.value += 1
        if self.value >= JavaInt._MAX:
            self.value = JavaInt._MIN
        return self.value
         
class HttpStatus:
     BadRequest = 400
     NotFound = 404
     Forbidden = 403
     Unauthorized = 401
     InternalServerError = 500
     ServiceUnavilable = 503
     GatewayTimeout = 504
     Success = 200
     Moved = 301

class Conn:

    def __init__(self, uri, dead_retry=_DEAD_RETRY, socket_timeout=_SOCKET_TIMEOUT, debug=True):
        self.uri = uri
        self.debug = debug
        self.socket = None
        self.fd = None
        self.dead_retry = dead_retry
        self.socket_timeout = socket_timeout
        self.deaduntil = 0
        self.connect()

    def _config_socket(self, s):
        s.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY,1)
        if hasattr(s, 'settimeout'): 
            s.settimeout(self.socket_timeout)

    def send_msg(self,msg):
        if not self._get_socket():
            raise _ConnectionDeadError("Connection was broken:%s"% (self.uri))
        self.socket.sendall(msg)

    def recv(self, rlen):
        if not self._get_socket():
            raise _ConnectionDeadError("Connection was broken:%s"% (self.uri))
        return self.fd.read(rlen)

    def readline(self):
        if not self._get_socket():
            raise _ConnectionDeadError("Connection was broken:%s"% (self.uri))
        line = self.fd.readline()
        if line == '':
            raise _ConnectionDeadError("Connection was broken:%s"% (self.uri))
        return line

    def _check_dead(self):
        if self.deaduntil and self.deaduntil > time.time():
            return True
        self.deaduntil = 0
        return False

    def connect(self):
        if self._get_socket():
            return True
        return False

    def debuglog(self, str):
        if self.debug:
            sys.stderr.write("MessageProducer: %s\n" % str)

    def mark_dead(self, reason):
        self.debuglog("%s: %s.  marking dead." % (self.uri, reason))
        self.deaduntil = time.time() + self.dead_retry
        self.close()

    def _get_socket(self):
        if self._check_dead():
            return None
        if self.socket:
            return self.socket
        s = socket.socket()
        self._config_socket(s)
        parse_rt=urlparse(self.uri)
        try:
            s.connect((parse_rt.hostname, parse_rt.port))
        except socket.timeout, msg:
            self.mark_dead("connect: %s" % msg)
            return None
        except socket.error, msg:
            if isinstance(msg, tuple): msg = msg[1]
            self.mark_dead("connect: %s" % msg[1])
            return None
        self.debuglog("Connect to %s successfully." % self.uri)
        self.socket = s
        self.fd = s.makefile()
        return self.socket

    def close(self):
        if self.socket:
            self.socket.close()
            self.fd.close()
            self.socket = None
            self.fd = None

class SendResult:
    "A send message result"

    def __init__(self, success, partition, offset, error=None):
        self.success = success
        self.partition = partition
        self.offset = offset
        self.error = error

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "SendResult[success=%s, partition=%s, offset=%s, error=%s]" % (self.success, self.partition, self.offset, self.error)

class MessageProducer:
    def __init__(self, topic, zk_servers="localhost:2181", partition_selector=get_round_robin_selector(), zk_timeout=10000,zk_root="/meta",
                 dead_retry=_DEAD_RETRY,  socket_timeout=_SOCKET_TIMEOUT, debug=True):
        """
         Create a new message producer to send messages to metamorphosis broker
         @param topic:  the topic to be sent by this producer
         @param zk_servers: the zookeeper server list,in the form of 'server1:port,server2:port...'
         @param partition_selector:  the function to determin which partion the message will be sent
         @param zk_timeout:   zookeeper timeout in mills,default is 10000
         @param zk_root:   the metamorphosis broker root path in zookeeper,default is '/meta'
         @param dead_retry:number of seconds before retrying a blacklisted  server. Default to 30 s.
         @param socket_timeout: timeout in seconds for all calls to a server. Defaults  to 3 seconds.
         @param debug:   whether to debug producer,default is True.
         """
        self.partition_selector = partition_selector
        self.topic = topic
        self.debug = debug
        self.dead_retry = dead_retry
        self.socket_timeout = socket_timeout
        self.zk_root = zk_root
        self.zk_servers = zk_servers
        self.zk_timeout = zk_timeout
        self._broker_topic_path = "%s/brokers/topics" % (self.zk_root)
        self._broker_ids_of_path = "%s/brokers/ids" % (self.zk_root)
        self.zk = ZKClient(zk_servers, timeout=zk_timeout)
        self._opaque = JavaInt()
        self._broker_dict = {}
        self._conn_dict = {}
        self._partition_list = []
        self._lock = threading.Lock()
        if self.topic is None:
            raise _Error("Topic is none")
        if self.zk_root is None:
            raise _Error("Zookeeper root path is none")
        if self.zk_servers is None:
            raise _Error("Zookeeper servers is none")
        if self.dead_retry is None or self.dead_retry < 0:
            raise _Error("Invalid dead retry times %s" % self.dead_retry)
        self._update_broker_infos()
        
    def _safe_zk_close(self):
        try:
            if self.zk is not None:
                self.zk.close()
        except:
            pass

    def _safe_zk_get(self, path, count=0):
        "Get path's data from zookeeper in safe mode"
        try:
            return self.zk.get(path)
        except Exception, e:
            if count > 3:
                raise
            else:
                self._safe_zk_close()
                self.zk = ZKClient(self.zk_servers, timeout=self.zk_timeout)
                return self._safe_zk_get(path, count+1)

    def _safe_zk_get_children(self, path, watcher, count=0):
        "Get path's children from zookeeper in safe mode"
        try:
            return self.zk.get_children(path, watcher)
        except Exception, e:
            if count > 3:
                raise
            else:
                self._safe_zk_close()
                self.zk = ZKClient(self.zk_servers, timeout=self.zk_timeout)
                return self._safe_zk_get_children(path, watcher, count+1)

    def _debug(self,msg):
        if self.debug:
            sys.stderr.write("[meta-producer-debug]:%s\n" % (msg))

    def _update_broker_infos(self):
        """ Update broker infos from zookeeper"""
        self._lock.acquire()
        try:
            self._debug("begin to update broker infos from zookeeper with topic %s" % (self.topic))
            @watchmethod
            def watcher(event):
                self._update_broker_infos()
            topic_path = "%s/%s"%(self._broker_topic_path, self.topic)
            children = self._safe_zk_get_children(topic_path, watcher)
            broker_dict = {}
            partition_list = []
            if children is not None:
                for child in children:
                    if child is not None and child.endswith("-m"):
                        broker_id = int(child[0:child.index("-m")])
                        broker = self._get_broker_by_id(broker_id)
                        partition_list.extend(self._get_parts(child, broker_id))
                        broker_dict[broker_id] = broker
                        partition_list.sort(cmp=Partition.partition_comp)
            self._update_conn_dict(broker_dict)
            self._broker_dict = broker_dict
            self._partition_list = partition_list
            self._debug("New broker dict for topic %s:%s" % (self.topic, str(self._broker_dict)))
            self._debug("New partition list for topic %s:%s" % (self.topic, str(self._partition_list)))
            self._debug("End to update broker infos from zookeeper with topic %s" % (self.topic))
        finally:
            self._lock.release()

    def _update_conn_dict(self,new_broker_dict):
        for broker_id in self._broker_dict.keys():
            #broker is both in old dict and new dict
            if new_broker_dict.get(broker_id) <> None:
                #if broker uri changed
                if new_broker_dict.get(broker_id).broker_uri <> self._broker_dict.get(broker_id).broker_uri:
                    conn = self._conn_dict.get(broker_id)
                    #close old connection
                    if conn is not None:
                        del self._conn_dict[broker_id]
                        self._debug("Closing %s" % (conn.uri))
                        conn.close()
                    new_uri = new_broker_dict.get(broker_id).broker_uri
                    self._debug("broker uri changed,old=%s,new=%s,broker_id=%s" % (self._broker_dict.get(broker_id).broker_uri,new_uri, broker_id))
                    #connect to new broker
                    self._debug("connecting to %s" % (new_uri))
                    self._conn_dict[broker_id] = Conn(new_uri, self.dead_retry, self.socket_timeout, self.debug)
            else:
                #Broker is not in new dict,close it.
                conn = self._conn_dict.get(broker_id)
                if conn is not None:
                    del self._conn_dict[broker_id]
                    self._debug("Closing %s" % (conn.uri))
                    conn.close()

        for broker_id in new_broker_dict.keys():
            #A new broker,we must connect it.
            if self._broker_dict.get(broker_id) is None:
                new_uri = new_broker_dict.get(broker_id).broker_uri
                self._debug("connecting to %s" % (new_uri))
                self._conn_dict[broker_id] = Conn(new_uri, self.dead_retry, self.socket_timeout, self.debug)

    def _get_parts(self,child,broker_id):
        n_parts = int(self._safe_zk_get("%s/%s/%s" % (self._broker_topic_path,self.topic,child))[0])
        rt = []
        for n in range(0, n_parts):
            rt.append(Partition(broker_id, n))
        return rt
            
    def _get_broker_by_id(self,broker_id):
        broker_uri,_ = self._safe_zk_get("%s/%s/master" % (self._broker_ids_of_path, broker_id))
        return Broker(broker_id, broker_uri)

    def send(self,msg):
        """ 
        Send message to broker
        @param msg:  message to be sent,it's topic must be equals to producer's topic,and it's data must not be none
        """
        if msg is None:
            raise _Error("Message is none") 
        topic = msg.topic
        if topic <> self.topic:
            raise _Error("Expect topic %s,but was %s" % (self.topic, topic))
        data = msg.data
        if data == None:
            raise _Error("message data is none")
        partition = self.partition_selector(topic, self._partition_list, msg)
        if partition is None:
            raise _Error("There is no avaiable partition for topic %s" % (topic))
        conn = self._conn_dict.get(partition.broker_id)
        if conn is None:
            raise _Error("There is no avaiable server right now for topic % and partition %d" % (topic, partition.partition))
        opaque = self._opaque.increase_and_get()
        cmd = msg.encode(partition.partition,opaque)

        def _unsafe_send(cmd, message):
            try:
                conn.send_msg(cmd)
                head = conn.readline()
                _, status, bodylen, resp_opaque = head.split(" ")
                status = int(status)
                bodylen = int(bodylen)
                resp_opaque = int(resp_opaque)
                body = conn.recv(bodylen)
                if  len(body) <> bodylen:
                    conn.mark_dead("Response format error,expect body length is %s,but is %s" % (bodylen,len(body)))
                    return SendResult(False, None, -1, error="network error")
                if resp_opaque <> opaque:
                    conn.mark_dead("Response opaque is not equals to request opaque")
                    return SendResult(False, None, -1, error="network error")
                if status == HttpStatus.Success:
                    msg_id, _, offset = body.split(" ")
                    message.id = long(msg_id)
                    message.partition = partition
                    return SendResult(True, partition, long(offset))
                else:
                    return SendResult(False, None, -1, error=body)
            except (_Error, socket.error), msg:
                if isinstance(msg, tuple): msg = msg[1]
                conn.mark_dead(msg)
                return SendResult(False, None, -1, error=msg)
        
        try:
            return _unsafe_send(cmd, msg)
        except _ConnectionDeadError:
            # retry once
            try:
                if conn.connect():
                    return _unsafe_send(cmd, msg)
                return SendResult(False, None, -1, error="Connection was broken")
            except (_ConnectionDeadError, socket.error), msg:
                conn.mark_dead(msg)
                return SendResult(False, None, -1, error=msg)

    def close(self):
        """ Close message producer"""
        if self.zk is not None:
            self.zk.close()
        for conn in self._conn_dict.values():
            conn.close()

if __name__ == '__main__':
    p = MessageProducer("avos-fetch-tasks")
    message = Message("avos-fetch-tasks","http://www.taobao.com")
    print p.send(message)
    p.close()
