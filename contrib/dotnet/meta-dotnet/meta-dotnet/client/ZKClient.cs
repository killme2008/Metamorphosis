using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;
using Metaq.cluster;
using ZooKeeperNet;
using System.Diagnostics;

namespace Metaq.client
{
    public class ZKClient
    {
        private static readonly object syncRoot = new object();
        public  Hashtable brokers = new Hashtable();
        //public  Hashtable partitions = new Hashtable();

        ZooKeeper zkWatchCreator = null;
        public ZKClient(string config){
             zkWatchCreator = new ZooKeeper(config, new TimeSpan(0, 0, 0, 5000), null);
        }

        public void push(string topic)
        {
            lock (syncRoot)
            {
                brokers.Clear();
                this.getChildren("/meta/brokers/topics/" + topic).ForEach((s) =>
                {
                    var n = Convert.ToInt32(this.getData("/meta/brokers/topics/" + topic + "/" + s));// partition number 
                    var id = Convert.ToInt32(s.Split('-')[0]);
                    var broker_full_uri = this.getData("/meta/brokers/ids/" + id + "/master");
                    var broker_uri = broker_full_uri.Replace("meta://", "");
                    Broker broker = new Broker(id, broker_uri);
                    for (int index = 0; index < n; index++)
                    {
                        broker.partitions.Add(new Partition(id,index));
                    }
                    brokers.Add(id, broker);
                    Debug.WriteLine("========= brokers list ==========");
                    Debug.WriteLine("broker: " + id + " -> " + broker_uri);
                });

            }

        }

        public Broker findBroker()
        {
            return this.brokers[0] as Broker;
        }
        

        public List<string> getChildren(string path)
        {
            List<string> list = new List<string>();
            
            zkWatchCreator.Exists(path, true);

            IEnumerable<string> it = zkWatchCreator.GetChildren(path, false);
            foreach (string s in it)
            {
                //Console.WriteLine(s);
                list.Add(s);
            }
            
            return list;
        }

        public string getData(string path)
        {
            List<string> list = new List<string>();

            zkWatchCreator.Exists(path, true);

            byte[] bs = zkWatchCreator.GetData(path, false,null);

            return Encoding.UTF8.GetString(bs);
        }

        
        public void Close()
        {
            zkWatchCreator.Dispose();
        }
    }
}
