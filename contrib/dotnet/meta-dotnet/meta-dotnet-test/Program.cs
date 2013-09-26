using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Metaq.client;

namespace Metaq
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("====>test pack 3:");
            Console.WriteLine(Message.byteArry2String(Message.pack(3)));

            Console.WriteLine("====>test encode message:");
            Message message = Message.create("test", "hello");
            Console.WriteLine(Message.byteArry2String(message.encode(1,99)));

            Console.WriteLine("====>test encode message:");
            Message message1 = Message.create("test", "hello1", "AAA");
            Console.WriteLine(Message.byteArry2String(message1.encode(1, 99)));

            Producer producer = new Producer("127.0.0.1:2181", "test");
            producer.Send(message);
            producer.Send(message1);
            producer.Close();
             
            Console.ReadKey();
        }
    }
}
