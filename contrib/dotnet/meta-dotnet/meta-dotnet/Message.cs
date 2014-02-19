using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Metaq
{
    public class Message
    {
        public static Message create(string topic, string data, string attribute = null)
        {
            return new Message(topic, Encoding.GetEncoding("utf-8").GetBytes(data), attribute);
        }

        //:id, :topic, :data, :attribute, :partition, :flag
        public Message(string topic, byte[] data, string attribute = null)
        {
            this.topic = topic;
            this.data = data;
            this.attribute = attribute;
            this.flag = 0;
            this.partition = -1;
        }

        public int id { set;get; }
        public string topic { set; get; }
        public byte[] data { set; get; }
        public string attribute { set; get; }
        public int partition { set; get; }
        public int flag { set; get; }

        public byte[] encode(int partition, int opaque)
        {
            byte[] payload = this.encode_payload();
            // remove a null check
            int vlen = payload.Length;
            string str_header = string.Format("put {0} {1} {2} {3} {4}\r\n", topic, partition, vlen, flag, opaque);
            byte[] header = Encoding.GetEncoding("utf-8").GetBytes(str_header);
            return combine(header,payload);
        }

        private byte[] encode_payload()
        {
            if (attribute == null) return data;
            byte[] attr = Encoding.GetEncoding("utf-8").GetBytes(attribute);
            byte[] attr_len = pack(attr.Length);
            this.flag = (int)(flag  & 0xFFFFFFFE) | 1;
            int size = 4 + attr.Length + data.Length;
            byte[] payload = new byte[size];
            return combine(attr_len,attr,data);
        }

        private byte[] combine(params byte[][] arr_params)
        {
            Stream s = new MemoryStream();
            int count = 0;
            foreach (byte[] bs in arr_params)
            {
                s.Write(bs,0,bs.Length);
                count += bs.Length;
            }
            byte[] result = new byte[count];
            s.Position = 0;
            s.Read(result, 0, count);
            s.Close();
            return result;
        }


        public static byte[] pack(int n)
        {
            byte[] bs = new byte[4];
            bs = BitConverter.GetBytes(n);
            return reverse4(bs);
        }

        public static byte[] reverse4(byte[] bs)
        {
            byte t = bs[0];
            bs[0]=bs[3];
            bs[3] = t;
            t = bs[1];
            bs[1] = bs[2];
            bs[2] = t;
            return bs;
        }

        public static string byteArry2String(byte[] bs)
        {
            StringBuilder sb = new StringBuilder();
            foreach (byte b in bs)
            {
                sb.Append("\\x" + b.ToString("X2"));
            }
            return sb.ToString();
        }

    }
}
