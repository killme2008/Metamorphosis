#介绍

淘宝开源MQ--[metaq](https://github.com/killme2008/Metamorphosis)的python客户端，目前只支持发送消息功能。


#安装

* 源码安装，下载本目录的代码，执行如下命令:

		sudo rake install


#使用

		  require 'rubygems'
		  require 'metaq'

     	  producer =Metaq::MessageProducer.new("meta-test",{ :zk_root => "/avos-fetch-meta" })
		  msg =Metaq::Message.new("meta-test","hello world")
	      p producer.send(msg)
          producer.close

#协议
[The MIT License](http://www.opensource.org/licenses/mit-license.html)
