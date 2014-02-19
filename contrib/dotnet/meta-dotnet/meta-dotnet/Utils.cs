using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Metaq
{
    public class Utils
    {
        private static readonly object syncRoot = new object();
        private static int _GenerateSequence = 0;
        public static int GenerateSequence()
        {
            lock(syncRoot){
                return _GenerateSequence++;
            }
        }

    }
}
