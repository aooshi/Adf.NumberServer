using System;
using System.Text;

namespace Adf.NumberServer
{
    class JournalPacket
    {
        public static void Encode(BlockData bd, byte cmd, DataItem di)
        {
            /*
             * flag| block    | value    | key len   | key data      |
             * byte| int(32)    | int64(8) | byte      | string (*)    |
             * 
             * max length = 1 + 4 + 8 + 1 + 250 = 264
             * 
             */

            //jb.position = 0;

            //cmd
            bd.buffer[0] = cmd;
            //jb.position += 1;

            //block
            Adf.BaseDataConverter.ToBytes(di.block, bd.buffer, 1);
            //jb.position += 4;

            //value
            Adf.BaseDataConverter.ToBytes(di.value, bd.buffer, 5);
            //jb.position += 8;

            bd.buffer[13] = di.key_length;
            //jb.position += 1;

            //key data
            //Encoding.ASCII.GetBytes(di.key, 0, keyLength, jb.data, jb.position);
            Array.Copy(di.key_buffer, 0, bd.buffer, 14, di.key_length);

            //
            bd.buffer[BlockData.BLOCK_CR_POS] = 13;
            bd.buffer[BlockData.BLOCK_LF_POS] = 10;
        }


        //the method is synchronously for calls.
        public static DataItem Decode(byte[] dataBuffer, ref byte flag)
        {
            /*
             * flag | block    | value    | key len   | key data      |
             * byte| int(32)    | int64(8) | byte      | string (*)    |
             * 
             * max length = 1 + 4 + 8 + 1 + 250 = 264
             * 
             */

            if (dataBuffer.Length == BlockData.BLOCK_LENGTH && dataBuffer[BlockData.BLOCK_CR_POS] == 13 && dataBuffer[BlockData.BLOCK_LF_POS] == 10)
            {
                var di = new DataItem();
                //var position = 0;

                flag = dataBuffer[0];
                //position += 1;

                di.block = Adf.BaseDataConverter.ToInt32(dataBuffer, 1);
                //position += 4;

                di.value = Adf.BaseDataConverter.ToUInt64(dataBuffer, 5);
                //position += 8;

                var keyLength = dataBuffer[13];
                //position += 1;

                //di.key = Encoding.ASCII.GetString(dataBuffer, position, keyLength);
                di.key_length = keyLength;
                di.key_buffer = new byte[keyLength];
                Array.Copy(dataBuffer, 14, di.key_buffer, 0, keyLength);

                return di;

            }

            return null;
        }
    }
}