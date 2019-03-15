using System;
using System.IO;
using System.Collections.Generic;

namespace Adf.NumberServer
{
    class DataManager : IDisposable
    {
        /*
         * PROTOCOL VERSION 1
         * 
         * header
         * ----------------------
         * | 1  .. .....   1024
         * ----------------------
         * block 1
         * ----------------------
         * | 1  .. .....   274
         * ----------------------
         * block 2
         * ----------------------
         * | 1  .. .....   274
         * ----------------------
         * block ...
         * ----------------------
         * | 1  .. .....   274
         * ----------------------
         * block N
         * ----------------------
         * | 1  .. .....   274
         * ----------------------
         * 
         *
         * */

        private const int PROTOCOL_VERSION_1 = 1;
        private const int STORE_RETRY_THRESHOLD = 3;

        LogManager logManager;
        string dataPath;
        string filePath;
        //
        int maxBlock = 0;
        Queue<int> freeBlockQueue;
        //
        FileStream fileStream;

        public long journalVersion;

        public readonly object SyncObject = new object();

        public DataManager(LogManager logManager, string dataPath)
        {
            this.logManager = logManager;
            this.dataPath = dataPath;
            this.filePath = System.IO.Path.Combine(dataPath, "main.data");
            //
            this.maxBlock = 0;
            this.freeBlockQueue = new Queue<int>(32);
            //
            this.Open();
        }

        private void Open()
        {
            FileStream fs = null;
            if (File.Exists(this.filePath) == false)
            {
                fs = File.Create(this.filePath);
            }
            else
            {
                fs = File.Open(this.filePath, FileMode.Open, FileAccess.ReadWrite);
            }

            //check for init
            if (fs.Length < 1024)
            {
                //set header

                //.. set protocal version
                var buffer = new byte[32];
                Adf.BaseDataConverter.ToBytes(PROTOCOL_VERSION_1, buffer, 0);
                fs.Write(buffer, 0, 4);

                //.. set journal version
                Adf.BaseDataConverter.ToBytes(this.journalVersion, buffer, 0);
                fs.Write(buffer, 0, 8);
            }
            else
            {
                var headerBuffer = new byte[1024];
                fs.Read(headerBuffer, 0, headerBuffer.Length);

                //
                this.journalVersion = Adf.BaseDataConverter.ToInt64(headerBuffer, 4);
            }

            //set header position
            //fs.Seek(1024, SeekOrigin.Begin);

            //
            this.fileStream = fs;
        }

        private void Reopen()
        {
            FileStream fs = this.fileStream;

            if (fs != null)
            {
                try
                {
                    fs.Dispose();
                }
                catch { }
            }

            if (File.Exists(this.filePath) == false)
            {
                var error = this.filePath + " not find.";

                this.logManager.Error.WriteTimeLine(error);

                throw new IOException(error);
            }

            fs = File.Open(this.filePath, FileMode.Open, FileAccess.ReadWrite);


            var headerBuffer = new byte[1024];
            fs.Read(headerBuffer, 0, headerBuffer.Length);

            //
            this.journalVersion = Adf.BaseDataConverter.ToInt64(headerBuffer, 4);

            //set header position
            //fs.Seek(1024, SeekOrigin.Begin);

            //
            this.fileStream = fs;
        }

        /// <summary>
        /// 清空数据
        /// </summary>
        public void Empty()
        {
            //关闭现有文件流
            try
            {
                this.fileStream.Close();
                this.fileStream.Dispose();
            }
            catch { }

            //删除
            File.Delete(this.filePath);

            //打开一个新数据
            this.Open();
        }

        //用户初始导入
        public void LoadData(BlockAction action)
        {
            //导入主体数据

            if (this.fileStream.Length <= 1024)
                return;

            this.fileStream.Seek(1024, SeekOrigin.Begin);

            byte flag = 0;
            byte[] blockBuffer = new byte[BlockData.BLOCK_LENGTH];
            int read = 0;
            int blockIndex = 1;

            for (; blockIndex <= int.MaxValue; blockIndex++)
            {
                if (fileStream.Position == fileStream.Length)
                {
                    //end
                    break;
                }
                read = Adf.StreamHelper.Receive(fileStream, blockBuffer, BlockData.BLOCK_LENGTH);
                if (read == 0)
                {
                    //end
                    break;
                }
                else if (read == BlockData.BLOCK_LENGTH && blockBuffer[BlockData.BLOCK_CR_POS] == 13 && blockBuffer[BlockData.BLOCK_LF_POS] == 10)
                {
                    //decode
                    var item = JournalPacket.Decode(blockBuffer, ref flag);
                    if (item.block != blockIndex)
                    {
                        //block error

                        string msg = string.Format("read a block, but block index error. position {0} in {1}", fileStream.Position - read, this.filePath);

                        this.logManager.Error.WriteTimeLine(msg);
                        this.logManager.Error.Flush();

                        throw new IOException(msg);
                    }

                    if (flag == BlockFlag.FREE)
                    {
                        this.freeBlockQueue.Enqueue(item.block);
                    }

                    action(flag, item);
                }
                else if (read != BlockData.BLOCK_LENGTH)
                {
                    string msg = string.Format("read a block, but block data length invalid. position {0} in {1}", fileStream.Position - read, this.filePath);

                    //block error
                    this.logManager.Error.WriteTimeLine(msg);
                    this.logManager.Error.Flush();

                    throw new IOException(msg);
                }
                else if (BlockData.IsEmptyBlock(blockBuffer) == true)
                {
                    //is unused block
                    this.freeBlockQueue.Enqueue(blockIndex);
                }
                else
                {
                    //block error

                    string msg = string.Format("read a block, but block data invalid. position {0} in {1}", fileStream.Position - read, this.filePath);

                    //block error
                    this.logManager.Error.WriteTimeLine(msg);
                    this.logManager.Error.Flush();

                    throw new IOException(msg);
                }
            }

            this.maxBlock = blockIndex;
        }

        public void StoreHeader(long journalVersion)
        {
            int protocolVerison = PROTOCOL_VERSION_1;
            this.StoreHeader(protocolVerison, journalVersion, 0);
        }

        private void StoreHeader(int protocolVerison, long journalVersion, int retryCounter)
        {
            var buffer = new byte[1024];

            //.. set protocal version
            Adf.BaseDataConverter.ToBytes(protocolVerison, buffer, 0);

            //.. set journal version
            Adf.BaseDataConverter.ToBytes(journalVersion, buffer, 4);

            try
            {
                this.fileStream.Seek(0, SeekOrigin.Begin);
                this.fileStream.Write(buffer, 0, buffer.Length);
                this.fileStream.Flush();
            }
            catch (IOException exception)
            {
                var error = "header store failure " + exception.Message;

                if (retryCounter > STORE_RETRY_THRESHOLD)
                {
                    this.logManager.Error.WriteTimeLine(error);
                    this.logManager.Error.Flush();

                    throw;
                }
                else
                {
                    this.Reopen();

                    this.logManager.Warning.WriteTimeLine(error);

                    this.StoreHeader(protocolVerison, journalVersion, retryCounter + 1);
                }
            }
        }
        
        public void Store(int block, byte blockFlag, byte[] buffer)
        {
            this.Store(block, blockFlag, buffer, 0);
        }

        private void Store(int block, byte blockFlag, byte[] buffer, int retryCounter)
        {
            var offset = ((block - 1) * BlockData.BLOCK_LENGTH) + 1024;

            try
            {
                this.fileStream.Seek(offset, SeekOrigin.Begin);
                this.fileStream.Write(buffer, 0, buffer.Length);
                this.fileStream.Flush();

                if (blockFlag == BlockFlag.FREE)
                {
                    lock (this.freeBlockQueue)
                    {
                        this.freeBlockQueue.Enqueue(block);
                    }
                }
            }
            catch (IOException exception)
            {
                var error = "data store failure " + exception.Message;

                if (retryCounter > STORE_RETRY_THRESHOLD)
                {
                    this.logManager.Error.WriteTimeLine(error);
                    this.logManager.Error.Flush();

                    throw;
                }
                else
                {
                    this.Reopen();

                    this.logManager.Warning.WriteTimeLine(error);

                    this.Store(block, blockFlag, buffer, retryCounter + 1);
                }
            }
        }

        /// <summary>
        /// get a free block index
        /// </summary>
        /// <returns></returns>
        public int GetFreeBlock()
        {
            int block = 0;

            lock (this.freeBlockQueue)
            {
                if (this.freeBlockQueue.Count == 0)
                {
                    block = ++this.maxBlock;
                }
                else
                {
                    block = this.freeBlockQueue.Dequeue();
                }
            }

            return block;
        }

        public long GetLength()
        {
            var length = 0L;

            //set position
            for (int i = 0; i < int.MaxValue; i++)
            {
                try
                {
                    length = this.fileStream.Length;
                    break;
                }
                catch
                {
                    if (i > 3)
                    {
                        throw new IOException("cannot open data file.");
                    }

                    this.Reopen();
                    System.Threading.Thread.Sleep(300);
                }
            }
            
            //
            return length;
        }

        public void CopyTo(Stream outputStream)
        {
            var length = 0L;
            var buffer = new byte[4096];
            var read = 0;
            var readAll = 0;

            //set position
            for (int i = 0; i < int.MaxValue; i++)
            {
                try
                {
                    this.fileStream.Seek(0, SeekOrigin.Begin);
                    length = this.fileStream.Length;
                    break;
                }
                catch
                {
                    if (i > 3)
                    {
                        throw new IOException("cannot open data file.");
                    }

                    this.Reopen();
                    System.Threading.Thread.Sleep(300);
                }
            }

            //read
            while (readAll < length)
            {
                read = this.fileStream.Read(buffer, 0, 4096);
                if (read == 0)
                {
                    //is stream closed
                    throw new IOException("data file stream is closed.");
                }

                readAll += read;
                outputStream.Write(buffer, 0, read);
            }
        }

        public void Dispose()
        {
            try
            {
                this.fileStream.Flush();
            }
            catch { }
            //
            try
            {
                this.fileStream.Close();
                this.fileStream.Dispose();
            }
            catch { }
            //
            this.freeBlockQueue.Clear();
        }
    }
}