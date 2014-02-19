using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Metaq.cluster
{
    public class Partition
    {
        public Partition(int brokerId, int partition)
        {
            this.brokerId = brokerId;
            this.partition = partition;
        }
        public int brokerId { set; get; }
        public int partition { set; get; }

        public bool Equals(Partition other)
        {
            return this.brokerId == other.brokerId && this.partition == other.partition;
        }

        override public string ToString()
        {
            return string.Format("{0}-{1}",brokerId,partition);
        }
    }
}
