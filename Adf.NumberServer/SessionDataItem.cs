using System;

namespace Adf.NumberServer
{
    class SessionDataItem
    {
        public byte[] returnData;
        public BlockData blockData;
        public MemcachedSession session;
    }
}
