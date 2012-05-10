from metaq.producer import Message,MessageProducer
p=MessageProducer("avos-fetch-tasks")
message=Message("avos-fetch-tasks","http://www.taobao.com")
print p.send(message)
print p.send(message)
print p.send(message)
p.close()
