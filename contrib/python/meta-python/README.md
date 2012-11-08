#介绍

淘宝开源MQ--[metaq](https://github.com/killme2008/Metamorphosis)的python客户端，目前只支持发送消息功能。


#安装

首先确保你的机器安装了zookeeper的c客户端，并正确设置了头文件目录，可使用本目录下的install_zk.sh脚本自动安装

	   curl https://raw.github.com/killme2008/Metamorphosis/master/contrib/python/meta-python/install_zk.sh | sudo sh

在安装后执行如下命令:

	    python setup.py install

如果你使用pypi的话，更简单:

        sudo pip install metaq

升级:

        sudo pip install --upgrade metaq

#使用

使用很简单:

		from metaq.producer import Message,MessageProducer,SendResult
		p=MessageProducer("avos-fetch-tasks")
		message=Message("avos-fetch-tasks","http://www.taobao.com")
		print p.send(message)
		p.close()

如果你的zookeeper不是`localhost:2181`，可设置zk_servers属性:
		
			p=MessageProducer("avos-fetch-tasks",zk_servers="192.168.1.100:2191,192.168.1.101:2181")

更多信息参考metaq/producer.py.

MessageProducer的有效参数包括:

         @param topic:  the topic to be sent by this producer
         @param zk_servers: the zookeeper server list,in the form of 'server1:port,server2:port...'
         @param partition_selector:  the function to determin which partion the message will be sent
         @param zk_timeout:   zookeeper timeout in mills,default is 10000
         @param zk_root:   the metamorphosis broker root path in zookeeper,default is '/meta'
         @param dead_retry:number of seconds before retrying a blacklisted  server. Default to 30 s.
         @param socket_timeout: timeout in seconds for all calls to a server. Defaults  to 3 seconds.
         @param idle_timeout: timeout in seconds for marking connection is idle to send heartbeats,default is 5 seconds.
         @param debug:   whether to debug producer,default is True.


#协议
[The MIT License](http://www.opensource.org/licenses/mit-license.html)
