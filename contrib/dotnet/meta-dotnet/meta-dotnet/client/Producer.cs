using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Net.Sockets;
using Metaq.cluster;

namespace Metaq.client
{
    public class Producer
    {
        ZKClient zk;
        Socket socket;
        String host = "127.0.0.1";
        int port = 8123;
        public Producer(string zkconfig, string topic)
        {
            zk = new ZKClient(zkconfig);
            zk.push(topic);
            Broker broker = zk.findBroker();
            if (broker != null)
            {
                string[] uri = broker.brokerUri.Split(':');
                host = uri[0];
                port = Convert.ToInt32(uri[1]);
            }

            socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                socket.Connect(host, port);
            }
            catch (SocketException e)
            {
                Console.Write(e.ToString());
            }

        }

        public void Send(Message message)
        {

            byte[] data = new byte[4096];

            byte[] ms = message.encode(0,1);
            
            socket.Send(ms);
            int recv = 0;
            string result = "";
            //do
            {
                recv = socket.Receive(data);
                result += (Encoding.UTF8.GetString(data, 0, recv));
            } //while (recv != 0);

            Console.WriteLine(result);
           
        }

        public void Close()
        {
            socket.Shutdown(SocketShutdown.Both);
            socket.Close();
            zk.Close();
        }
    }
}
