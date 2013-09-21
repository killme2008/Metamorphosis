using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Net.Sockets;

namespace Metaq.client
{
    public class Producer
    {
        public void send(Message message)
        {

            byte[] data = new byte[4096];
            IPHostEntry gist = Dns.GetHostByName("localhost");
            //得到所访问的网址的IP地址
            IPAddress ip = gist.AddressList[0];
            IPEndPoint ipEnd = new IPEndPoint(ip, 8123);
            //使用tcp协议 stream类型 （IPV4）
            Socket socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                socket.Connect(ipEnd);
            }
            catch (SocketException e)
            {
                Console.Write(e.ToString());
                return;
            }

            byte[] ms = message.encode(0,1);
            //发送
            socket.Send(ms);
            int recv = 0;
            string result = "";
            //do
            {
                recv = socket.Receive(data);
                //如果请求的页面meta中指定了页面的encoding为gb2312则需要使用对应的Encoding来对字节进行转换
                //list.Text += (Encoding.UTF8.GetString(data, 0, recv));
                result += (Encoding.UTF8.GetString(data, 0, recv));
            } //while (recv != 0);

            Console.WriteLine(result);
        
            //禁用上次的发送和接受
            socket.Shutdown(SocketShutdown.Both);
            socket.Close();

        }
    }
}
