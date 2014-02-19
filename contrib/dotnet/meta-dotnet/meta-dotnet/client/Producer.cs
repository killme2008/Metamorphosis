using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Net.Sockets;
using Metaq.cluster;
using System.Diagnostics;

namespace Metaq.client
{
    public class Producer
    {
        ZKClient zk;
        Socket socket;
        String host = "127.0.0.1";
        int port = 8123;

        Broker broker;
        public Producer(string zkconfig, string topic)
        {
            zk = new ZKClient(zkconfig);
            zk.push(topic);
            broker = zk.findBroker();
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
                Debug.Write(e.ToString());
            }

        }

        public void Send(Message message)
        {

            byte[] data = new byte[4096];
            int n = 0;
            if (broker.partitions.Count > 1)
            {
                n = Utils.GenerateSequence() % broker.partitions.Count;
                Debug.WriteLine("select partition: " + n);
            }
            byte[] ms = message.encode(n,1);
            
            socket.Send(ms);
            int recv = 0;
            string result = "";
            //do
            {
                recv = socket.Receive(data);
                result += (Encoding.UTF8.GetString(data, 0, recv));
            } //while (recv != 0);

            Debug.WriteLine(result);
           
        }

        public void Close()
        {
            socket.Shutdown(SocketShutdown.Both);
            socket.Close();
            zk.Close();
        }
    }
}
