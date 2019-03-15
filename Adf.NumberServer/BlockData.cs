using System;
using System.Collections.Generic;
using System.Text;

namespace Adf.NumberServer
{
    class BlockData
    {
        /*
         * flag| block    | value    | key len   | key data      |
         * byte| int(32)    | int64(8) | byte      | string (*)    |
         * 
         * content max length = 1 + 4 + 8 + 1 + 250 = 264
         * 
         * replication session id = 8 
         * fixed length = 264 + 8 = 272
         * 
         * crlf = 2;
         * fixed legnth = 272 + 2 = 274;
         * 
         */

        public const int BLOCK_LENGTH = 274;
        public const int BLOCK_CR_POS = 272;
        public const int BLOCK_LF_POS = 273;
        public const int BLOCK_SESSION_ID_POS = 264;

        public readonly byte[] buffer;
        //public int position = 0;
        //
        public BlockData()
        {
            /*
             * 1. key max length,  250
             * 2. block max length = 264
             * flag| block      | value    | key len   | key data      |
             * byte| int(32)    | int64(8) | byte      | string (*)    |
             * 3. session id = 8 to replication
             * 
             * fixed total: 264+8 = 272
             * 
             */


            this.buffer = new byte[BLOCK_LENGTH];
        }

        public static bool IsEmptyBlock(byte[] blockBuffer)
        {
            bool empty = true;

            for (int i = 0; i < BlockData.BLOCK_LENGTH; i++)
            {
                if (blockBuffer[i] != 0)
                {
                    empty = false;
                    break;
                }
            }

            return empty;
        }
    }
}