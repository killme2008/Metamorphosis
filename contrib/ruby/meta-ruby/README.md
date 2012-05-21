#介绍

淘宝开源MQ--[metaq](https://github.com/killme2008/Metamorphosis)的python客户端，目前只支持发送消息功能。


#安装

TODO

#使用

		  require 'rubygems'
		  require 'eventmachine'
		  require 'metaq'

		  EM.run do
		      msg = Metaq::Message.new("meta-test","hello","fuck")
		      producer = Metaq::Client.connect("localhost")
          	  producer.send_request(msg) do | result|
      	          p result
			  end
          end


#协议
[The MIT License](http://www.opensource.org/licenses/mit-license.html)
