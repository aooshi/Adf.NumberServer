using System;

namespace Adf.NumberServer
{
    public class DataItem
    {
        //public string key;
        public ulong value;
        public int block;

        public byte[] key_buffer;
        public byte key_length;
    }
}
