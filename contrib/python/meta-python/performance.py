from metaq.producer import Message,MessageProducer
from time import time
p=MessageProducer("avos-fetch-tasks")
message=Message("avos-fetch-tasks","http://www.taobao.com")
start=time()
for i in range(0,10000):
    sent=p.send(message)
    if not sent.success:
        print "send failed"
finish=time()
secs=finish-start
print "duration:%s seconds" % (secs)
print "tps:%s msgs/second" % (10000/secs)
p.close()
