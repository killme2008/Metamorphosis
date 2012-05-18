from metaq.producer import Message,MessageProducer
producer = MessageProducer("meta-test")
message = Message("meta-test", "http://www.taobao.com")
print producer.send(message)
producer.close()
