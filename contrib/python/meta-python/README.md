#介绍

淘宝开源MQ--[metaq](https://github.com/killme2008/Metamorphosis)的python客户端，目前只支持发送消息功能。

#安装

		python setup.py install

#使用

使用很简单:

		from metaq.producer import Message,MessageProducer,SendResult
		p=MessageProducer("avos-fetch-tasks")
		message=Message("avos-fetch-tasks","http://www.taobao.com")
		print p.send(message)
		p.close()

如果你的zookeeper不是`localhost:2181`，可设置zk_servers属性:
		
			p=MessageProducer("avos-fetch-tasks",zk_servers="192.168.1.100:2191,192.168.1.101:2181")

更多信息参考metamorphosis.py

#协议
[The MIT License](http://www.opensource.org/licenses/mit-license.html)
