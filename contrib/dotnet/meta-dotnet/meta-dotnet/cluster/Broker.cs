using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Metaq.cluster
{
    public class Broker
    {
        public Broker(int brokerId, string brokerUri)
        {
            this.brokerId = brokerId;
            this.brokerUri = brokerUri;
        }
        public int brokerId { set; get; }
        public string brokerUri { set; get; }

    }
}
