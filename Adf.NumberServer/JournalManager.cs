using System;
using System.IO;
using System.Collections.Generic;

namespace Adf.NumberServer
{
    class JournalManager : IDisposable
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
         * | 1  .. .....   512
         * ----------------------
         * block 2
         * ----------------------
         * | 1  .. .....   512
         * ----------------------
         * block ...
         * ----------------------
         * | 1  .. .....   512
         * ----------------------
         * block N
         * ----------------------
         * | 1  .. .....   512
         * ----------------------
         * 
         *
         * */


        private const int PROTOCOL_VERSION_1 = 1;

        LogManager logManager;
        //
        string dataPath;
        private List<long> journalList = new List<long>(8);
        private FileStream fileStream = null;
        private long fileIndex = 0;
        //
        private long journalRotate = 0;
        //
        private object writeLock = new object();

        //
        System.Threading.EventWaitHandle margeHandle;
        System.Threading.Thread margeThread;
        //

        public bool Enable = false;
        private bool disposed = false;

        public bool replicationRequestingFlag = false;

        public bool replicationEnable = false;

        private MemcachedSession replicationSession = null;

        //private Adf.QueueTask<SessionDataBuffer> logQueue;

        public long GetJournalRotate
        {
            get { return this.journalRotate; }
        }

        public long GetJournalLength
        {
            get
            {
                var length = 0L;
                try { length = this.fileStream.Length; }
                catch { }
                return length;
            }
        }
        
        public JournalManager(LogManager logManager, string dataPath)
        {
            this.logManager = logManager;
            this.dataPath = dataPath;
            //
            this.InitializeConfiguration();
            this.initializeJournal();
            this.InitializeMarge();
            //
            //this.logQueue = new QueueTask<SessionDataBuffer>(this.LogQueueProcessor);
        }

        private void InitializeMarge()
        {
            this.margeHandle = new System.Threading.ManualResetEvent(false);
            this.margeThread = new System.Threading.Thread(this.MargeProcessor);
            this.margeThread.IsBackground = true;
            this.margeThread.Start();
        }

        private void initializeJournal()
        {
            this.journalList = this.GetJournalIndexs(this.dataPath);
        }

        private void InitializeConfiguration()
        {
            //default 1gb
            var rotate = Adf.ConfigHelper.GetSettingAsInt("JournalRotate", 1024);
            if (rotate < 100 || rotate > 2048)
            {
                throw new ConfigException("JournalRotate configuration invalid, value allow 100 - 2048");
            }
            this.journalRotate = rotate * 1024 * 1024;
        }

        private long GetNewJournalIndex()
        {
            long index = 0;

            lock (this.journalList)
            {
                if (this.journalList.Count == 0)
                {
                    index = 1;
                }
                else
                {
                    //取得最末尾数据

                    index = this.journalList[this.journalList.Count - 1];
                    index++;
                }

                this.journalList.Add(index);
            }

            return index;
        }

        private long GetLastJournalIndex()
        {
            long index = 0;

            lock (this.journalList)
            {
                if (this.journalList.Count == 0)
                {
                    index = 1;
                }
                else
                {
                    //取得最末尾数据

                    index = this.journalList[this.journalList.Count - 1];
                }
            }

            return index;
        }

        public void LoadJournal(BlockAction action)
        {
            //导入日志数据
            for (int i = 0; i < this.journalList.Count; i++)
            {
                var fileIndex = this.journalList[i];
                var filePath = System.IO.Path.Combine(this.dataPath, fileIndex + ".journal");

                this.logManager.Message.WriteTimeLine("Load journal " + filePath);
                
                this.LoadJournalData(action, filePath);
            }
        }

        public void NewJournal(SessionDataItem sdi, byte cmd, DataItem item)
        {
            if (this.Enable == true)
            {
                //data package
                JournalPacket.Encode(sdi.blockData, cmd, item);

                if (this.replicationEnable == true)
                {
                    //serial to db last 8byte
                    //data buffer + 8byte session id
                    Adf.BaseDataConverter.ToBytes(sdi.session.id
                        , sdi.blockData.buffer
                        , BlockData.BLOCK_SESSION_ID_POS);

                    lock (this.writeLock)
                    {
                        //store
                        this.NewJournal(sdi.blockData.buffer, 0);

                        //replication
                        this.replicationSession.Send(sdi.blockData.buffer);
                    }
                }
                else
                {
                    lock (this.writeLock)
                    {
                        //store
                        this.NewJournal(sdi.blockData.buffer, 0);
                    }

                    //callback
                    if (sdi.returnData != null)
                    {
                        sdi.session.isExecuting = false;
                        sdi.session.Send(sdi.returnData);
                    }
                }
            }
            else if (sdi.returnData != null)
            {
                sdi.session.isExecuting = false;
                sdi.session.Send(sdi.returnData);
            }
        }

        public void NewJournal(byte[] buffer)
        {
            lock (this.writeLock)
            {
                this.NewJournal(buffer, 0);
            }
        }

        private void NewJournal(byte[] buffer, int retryCounter)
        {
            bool success = true;

            var fs = this.fileStream;

            try
            {
                fs.Write(buffer, 0, BlockData.BLOCK_LENGTH);
                fs.Flush();
            }
            catch (IOException)
            {
                success = false;
            }

            if (success == false)
            {
                if (this.disposed == true)
                {
                    //skip
                }
                else if (retryCounter == 0)
                {
                    this.ReopenJournal();
                    this.NewJournal(buffer, retryCounter + 1);
                }
                else
                {
                    //int sleep = (int)(System.Math.Pow(2, retryCounter) * 10);
                    //if (sleep > 0)
                    //{
                    //    //0,20,40,80 ...
                    //    System.Threading.Thread.Sleep(sleep);
                    //}

                    int sleep = 1000;
                    System.Threading.Thread.Sleep(sleep);

                    this.ReopenJournal();
                    this.NewJournal(buffer, retryCounter + 1);
                }
            }
        }

        private void Rotate()
        {
            //close old
            try
            {
                this.fileStream.Dispose();
            }
            catch { }

            //create new index
            this.GetNewJournalIndex();

            //open journal
            this.OpenJournal();
        }

        private void ReopenJournal()
        {
            lock (this.writeLock)
            {
                //close old
                try
                {
                    this.fileStream.Dispose();
                }
                catch { }


                //
                try
                {
                    this.OpenJournal();
                }
                catch { }
            }
        }

        public void Open()
        {
            var fileIndex = this.GetLastJournalIndex();
            var filePath = System.IO.Path.Combine(this.dataPath, fileIndex + ".journal");

            if (File.Exists(filePath) == false)
            {
                using (File.Create(filePath)) { }
            }

            var fs = this.OpenJournal(fileIndex);
            //
            this.fileIndex = fileIndex;
            this.fileStream = fs;
        }

        private void OpenJournal()
        {
            var fileIndex = this.GetLastJournalIndex();
            var fs = this.OpenJournal(fileIndex);
            //
            this.fileIndex = fileIndex;
            this.fileStream = fs;
        }

        private FileStream OpenJournal(long fileIndex)
        {
            var filePath = System.IO.Path.Combine(this.dataPath, fileIndex + ".journal");
            //
            FileStream fs = File.Open(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite);
            if (fs.Length < 1024)
            {
                //set header

                //.. set protocal version
                var buffer = new byte[32];
                Adf.BaseDataConverter.ToBytes(PROTOCOL_VERSION_1, buffer, 0);
                fs.Write(buffer, 0, 4);

                //set header position
                fs.Seek(1024, SeekOrigin.Begin);
            }
            else
            {
                fs.Seek(fs.Length, SeekOrigin.Begin);
            }

            this.logManager.Message.WriteTimeLine("open journal " + filePath);

            return fs;
        }

        private void MargeProcessor()
        {
            long fileIndex = 0;

            while (true)
            {
                this.margeHandle.WaitOne(60000);

                lock (this.writeLock)
                {
                    try
                    {
                        //轮转检测
                        if (this.fileStream.Length > this.journalRotate)
                        {
                            this.Rotate();
                        }
                    }
                    catch
                    {
                        //
                    }

                    if (this.replicationRequestingFlag == true)
                    {
                        //disabled marge
                        continue;
                    }
                }

                fileIndex = 0;

                lock (this.journalList)
                {
                    if (this.journalList.Count > 1)
                    {
                        //取出首个
                        fileIndex = this.journalList[0];
                    }

                    if (fileIndex == 0)
                        continue;

                    //open journal
                    var filePath = System.IO.Path.Combine(this.dataPath, fileIndex + ".journal");
                    
                    var margeCount = JournalManager.MargeJournalToData(this.logManager, filePath);
                    if (margeCount == -1)
                    {
                        continue;
                    }

                    //clear
                    try
                    {
                        File.Delete(filePath);
                        this.journalList.RemoveAt(0);
                    }
                    catch (System.Threading.ThreadAbortException) { return; }
                    catch (Exception exception)
                    {
                        this.logManager.Exception(new DataFileException(exception, "clear expired file error," + exception.Message));
                        continue;
                    }
                    this.logManager.Message.WriteTimeLine("marge journal total " + margeCount + " to main.data, from " + filePath);
                    this.logManager.Message.WriteTimeLine("marge journal end");
                } // un  lock (this.journalList)
            }
        }

        /// <summary>
        /// marge journal to data
        /// </summary>
        /// <param name="logManager"></param>
        /// <param name="journalFilePath"></param>
        /// <returns>return -1 is failure</returns>
        public static int MargeJournalToData(LogManager logManager, string journalFilePath)
        {
            logManager.Message.WriteTimeLine("marge journal to main.data from " + journalFilePath);

            const int FAIL = - 1;

            byte flag = 0;
            int blockIndex = 1;
            var dictionary = new Dictionary<int, byte[]>();
            try
            {
                using (var fs = File.OpenRead(journalFilePath))
                {
                    if (fs.Length > 1024)
                    {
                        //set header

                        //.. set protocal version
                        //var buffer = new byte[32];
                        //Adf.BaseDataConverter.ToBytes(PROTOCOL_VERSION_1, buffer, 0);
                        //fs.Write(buffer, 0, 4);

                        //set header position
                        fs.Seek(1024, SeekOrigin.Begin);

                        //
                        int read = 0;

                        //
                        while (true)
                        {
                            byte[] blockBuffer = new byte[BlockData.BLOCK_LENGTH];
                            read = Adf.StreamHelper.Receive(fs, blockBuffer, BlockData.BLOCK_LENGTH);
                            if (read == 0)
                            {
                                //end
                                break;
                            }
                            else if (read == BlockData.BLOCK_LENGTH && blockBuffer[BlockData.BLOCK_CR_POS] == 13 && blockBuffer[BlockData.BLOCK_LF_POS] == 10)
                            {
                                //flag = blockBuffer[0];
                                blockIndex = Adf.BaseDataConverter.ToInt32(blockBuffer, 1);
                                //
                                dictionary[blockIndex] = blockBuffer;
                            }
                            else if (read != BlockData.BLOCK_LENGTH)
                            {
                                string msg = string.Format("marge: read a block, but block data length invalid. position {0} in {1}, skip this block.", fs.Position - read, journalFilePath);

                                //block error
                                logManager.Warning.WriteTimeLine(msg);
                            }
                            else if (BlockData.IsEmptyBlock(blockBuffer) == true)
                            {
                                //is unused block
                                string msg = string.Format("marge: read a empty block, position {0} in {1}, skip this block.", fs.Position - read, journalFilePath);

                                //block error
                                logManager.Warning.WriteTimeLine(msg);
                            }
                            else
                            {
                                //block error

                                string msg = string.Format("marge: read a block, but block data invalid. position {0} in {1}, skip this block.", fs.Position - read, journalFilePath);

                                //block error
                                logManager.Warning.WriteTimeLine(msg);
                            }
                        }
                    }
                }
            }
            catch (System.Threading.ThreadAbortException) { return FAIL; }
            catch (Exception exception)
            {
                logManager.Exception(new DataFileException(exception, "marge read journal error," + exception.Message));
                return FAIL;
            }

            //store
            lock (Program.DataManager.SyncObject)
            {
                try
                {
                    foreach (var item in dictionary)
                    {
                        byte[] buffer = item.Value;
                        blockIndex = item.Key;
                        flag = buffer[0];
                        //
                        Program.DataManager.Store(blockIndex, flag, buffer);
                    }
                }
                catch (System.Threading.ThreadAbortException) { return FAIL; }
                catch (Exception exception)
                {
                    logManager.Exception(new DataFileException(exception, "marge data store error," + exception.Message));
                    return FAIL;
                }
            }

            return dictionary.Count;
        }

        private void LoadJournalData(BlockAction action, string filePath)
        {
            int rows = 0;

            using (var fileStream = File.OpenRead(filePath))
            {
                if (fileStream.Length < 1024)
                    return;

                //jump header
                fileStream.Seek(1024, SeekOrigin.Begin);

                byte flag = 0;
                byte[] blockBuffer = new byte[BlockData.BLOCK_LENGTH];
                int read = 0;

                while (true)
                {
                    if (fileStream.Length == fileStream.Position)
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

                        action(flag, item);

                        rows++;
                    }
                    else if (read != BlockData.BLOCK_LENGTH)
                    {
                        string msg = string.Format("read a block, but block data length invalid. position {0} in {1}", fileStream.Position - read, filePath);

                        //block error
                        this.logManager.Error.WriteTimeLine(msg);
                        this.logManager.Error.Flush();

                        throw new IOException(msg);
                    }
                    else if (BlockData.IsEmptyBlock(blockBuffer) == true)
                    {
                        //is unused block
                        string msg = string.Format("read a empty block, position {0} in {1}, skip this block.", fileStream.Position - read, filePath);

                        //block error
                        this.logManager.Warning.WriteTimeLine(msg);
                        this.logManager.Error.Flush();

                        throw new IOException(msg);
                    }
                    else
                    {
                        //block error

                        string msg = string.Format("read a block, but block data invalid. position {0} in {1}", fileStream.Position - read, filePath);

                        //block error
                        this.logManager.Error.WriteTimeLine(msg);
                        this.logManager.Error.Flush();

                        throw new IOException(msg);
                    }
                }
            }

            this.logManager.Message.WriteTimeLine("Load Journal "+ rows +" rows");
        }

        public void Replicate(MemcachedSession session)
        {
            lock (this.writeLock)
            {
                byte[] data = null;

                if (this.replicationEnable == true)
                {
                    data = System.Text.Encoding.ASCII.GetBytes("CLIENT_ERROR have a replication connection.\r\n");
                    session.Send(data);
                    return;
                }


                var remoteEP = session.GetRemotePoint();

                //init srs
                var srs = new StreamReadState(session.stream, new byte[10], this.ReplicationReadCallback);

                //begin receive
                Adf.StreamHelper.Receive(srs);

                //
                this.replicationSession = session;

                //log
                this.logManager.Message.WriteTimeLine("replication session " + session.id + " new connection from " + remoteEP.ToString());

                //manual rotate
                this.Rotate();

                //
                var maxIndex = this.journalList.Count - 2;
                var build = new System.Text.StringBuilder();
                build.Append("DATA\r\n");
                for (int i = 0; i <= maxIndex; i++)
                {
                    //取出首个
                    build.Append(this.journalList[i]);
                    build.Append("\r\n");
                }
                build.Append("END\r\n");

                //
                data = System.Text.Encoding.ASCII.GetBytes(build.ToString());
                session.Send(data);

                //disable marge
                this.replicationRequestingFlag = true;
                //
                this.replicationEnable = true;
            }
        }

        public void ReplicateDataCompleted()
        {
            lock (this.writeLock)
            {
                this.replicationRequestingFlag = false;
            }
        }

        private void ReplicationReadCallback(StreamReadState srs)
        {
            /*
             * 
             * session id   int64(8)
             * __________
             * crlf         (2)
             * __________
             *  
             * session id   int64(8)
             * __________
             * crlf         (2)
             * __________
             * 
             * session id   int64(8)
             * __________
             * crlf         (2)
             * __________
             * 
             * */

            if (srs.Success == true && srs.Buffer[8] == 13 && srs.Buffer[9] == 10)
            {
                var sessionId = Adf.BaseDataConverter.ToInt64(srs.Buffer);
                var session = Program.Listen.GetSession(sessionId);
                if (session != null)
                {
                    var sdi = session.sdi;

                    if (sdi.returnData != null)
                    {
                        session.isExecuting = false;
                        session.Send(sdi.returnData);
                    }
                }

                try
                {
                    srs.Reset();
                    Adf.StreamHelper.Receive(srs);
                }
                catch
                {
                    this.ReplicationClose();
                }
            }
            else
            {
                //close replication stream
                this.ReplicationClose();
            }
        }

        private void ReplicationClose()
        {
            lock (this.writeLock)
            {
                var session = this.replicationSession;
                if (session != null)
                {
                    var sessionId = session.id;

                    try
                    {
                        session.Dispose();
                    }
                    catch { }

                    //
                    this.replicationSession = null;

                    //
                    this.logManager.Message.WriteTimeLine("replication session " + sessionId + " closed");
                }

                this.replicationEnable = false;

                this.replicationRequestingFlag = false;
            }
        }

        private List<long> GetJournalIndexs(string dataPath)
        {
            var journalIndexs = new List<long>();
            //
            var dataDirectory = new DirectoryInfo(dataPath);
            if (dataDirectory.Exists)
            {
                var journalFiles = dataDirectory.GetFiles("*.journal");
                var index = 0L;
                foreach (var fileInfo in journalFiles)
                {
                    var segments = fileInfo.Name.Split('.');
                    if (segments.Length == 2)
                    {
                        index = 0;
                        if (long.TryParse(segments[0], out index) && index > -1)
                        {
                            //journalSortFiles.Add(index, fileInfo.FullName);
                            journalIndexs.Add(index);
                        }
                        else
                        {
                            throw new DataFileException("journal file invalid, " + fileInfo.Name);
                        }
                    }
                }
            }

            //sort
            journalIndexs.Sort((a, b) =>
            {
                if (a == b)
                    return 0;

                if (a > b)
                    return 1;

                return -1;
            });

            return journalIndexs;
        }

        public long GetLength(long journalNumber)
        {
            //open journal
            var filePath = System.IO.Path.Combine(this.dataPath, journalNumber + ".journal");
            var length = 0L;

            //set position
            using (var fs = File.OpenRead(filePath))
            {
                length = fs.Length;
            }

            //
            return length;
        }

        public void CopyTo(long journalNumber, Stream outputStream)
        {
            var length = 0L;
            var buffer = new byte[4096];
            var read = 0;
            var readAll = 0;
            var filePath = System.IO.Path.Combine(this.dataPath, journalNumber + ".journal");

            //open journal file
            using (var fs = File.OpenRead(filePath))
            {
                //set position
                fs.Seek(0, SeekOrigin.Begin);

                //read length
                length = fs.Length;

                //read
                while (readAll < length)
                {
                    read = fs.Read(buffer, 0, 4096);
                    if (read == 0)
                    {
                        //is stream closed
                        throw new IOException("data file stream is closed.");
                    }

                    readAll += read;
                    outputStream.Write(buffer, 0, read);
                }
            }
        }

        public string GetFileName()
        {
            return this.fileIndex + ".journal";
        }

        /// <summary>
        /// 数空现有数据与内容
        /// </summary>
        public void Empty()
        {
            lock (this.writeLock)
            {
                //empty data
                Program.DataManager.Empty();

                //jounal rotate
                this.Rotate();
            }

            //empty journal
            lock (this.journalList)
            {
                //no access last item
                while (this.journalList.Count > 1)
                {
                    var filePath = System.IO.Path.Combine(this.dataPath, this.journalList[0] + ".journal");
                    File.Delete(filePath);
                    this.journalList.RemoveAt(0);
                }
            }
        }

        public virtual void Dispose()
        {
            this.disposed = true;

            //this.logQueue.WaitCompleted();
            //this.logQueue.Dispose();
            //
            try
            {
                if (this.fileStream != null)
                {
                    this.fileStream.Flush();
                    this.fileStream.Dispose();
                }
            }
            catch (ObjectDisposedException)
            {
            }

            //
            try
            {
                this.margeThread.Abort();
            }
            catch { }
        }
    }
}