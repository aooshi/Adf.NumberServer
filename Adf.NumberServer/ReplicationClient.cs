using System;
using System.Net.Sockets;
using System.Text;
using System.Collections.Generic;
using System.IO;

namespace Adf.NumberServer
{
    public class ReplicationExceptionEventArgs : System.EventArgs
    {
        public readonly string Message;

        public ReplicationExceptionEventArgs(string message)
            : base()
        {
            this.Message = message;
        }
    }

    class ReplicationClient : IDisposable
    {
        Socket syncSocket;
        NetworkStream syncStream;
        
        Encoding encoding;
        LogManager logManager;

        List<long> journalList;

        String dataPath;
        
        public event EventHandler<ReplicationExceptionEventArgs> IOError;

        public ReplicationClient(LogManager logManager, string dataPath, string host, int port)
        {
            this.logManager = logManager;
            this.dataPath = dataPath;

            this.syncSocket = new Socket(AddressFamily.InterNetwork
                , SocketType.Stream
                , ProtocolType.Tcp);

            this.syncSocket.NoDelay = true;

            this.syncSocket.Connect(host, port);
            this.syncStream = new NetworkStream(this.syncSocket, false);
            

            this.encoding = Encoding.ASCII;
            this.journalList = new List<long>();
        }

        public void Request()
        {
            this.logManager.Message.WriteTimeLine("replication client: request new replicate.");

            var command = "extra_replicate\r\n";
            var buffer = this.encoding.GetBytes(command);

            this.syncStream.Write(buffer, 0, buffer.Length);
            var line = Adf.StreamHelper.ReadLine(this.syncStream, this.encoding);
            if ("DATA".Equals(line) == false)
            {
                this.logManager.Message.WriteTimeLine("replication client: response a failure, " + line + ".");
                return;
            }

            //
            this.journalList.Clear();
            //
            var num = 0L;
            while (true)
            {
                line = Adf.StreamHelper.ReadLine(this.syncStream, this.encoding);
                if (line == "END")
                {
                    break;
                }
                else if (long.TryParse(line, out num))
                {
                    this.journalList.Add(num);
                }
                else
                {
                    this.logManager.Message.WriteTimeLine("replication client: response a invalid data, " + line + ".");
                    return;
                }
            }
            
            //
            foreach (var item in this.journalList)
            {
                this.logManager.Message.WriteTimeLine("replication client: wait sync remote " + item + ".journal.");
            }

            //empty lock node
            Program.Journal.Empty();

            //receive block

            //init srs
            var srs = new StreamReadState(this.syncStream, new byte[BlockData.BLOCK_LENGTH], this.ReadCallback);

            //begin receive
            Adf.StreamHelper.Receive(srs);
        }

        private void ReadCallback(StreamReadState srs)
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

            if (srs.Success == true && srs.Buffer[BlockData.BLOCK_CR_POS] == 13 && srs.Buffer[BlockData.BLOCK_LF_POS] == 10)
            {
                Program.Journal.NewJournal(srs.Buffer);
                //
                this.syncStream.BeginWrite(srs.Buffer, BlockData.BLOCK_SESSION_ID_POS, 10, this.SendCallback, null);
                //
                srs.Reset();
                Adf.StreamHelper.Receive(srs);
            }
            else
            {
                //close replication stream
                if (srs.Exception != null)
                {
                    this.TriggerIOError("replicate sync failure, " + srs.Exception.Message);
                }
                else
                {
                    this.TriggerIOError("replicate sync failure");
                }
            }
        }

        private void SendCallback(IAsyncResult ar)
        {
            try
            {
                this.syncStream.EndWrite(ar);
            }
            catch (IOException exception)
            {
                this.TriggerIOError("replicate sync failure, " + exception.Message);
            }
            catch (ObjectDisposedException)
            {
            }
        }

        private void TriggerIOError(string message)
        {
            var action = this.IOError;
            if (action != null)
            {
                action(this, new ReplicationExceptionEventArgs(message));
            }
        }

        public bool GetJournal()
        {
            var journalNumber = 0L;
            //
            for (int i = 0, l = this.journalList.Count; i < l; i++)
            {
                journalNumber = this.journalList[i];
                //
                var getResult = this.GetJournal(journalNumber);
                if (getResult == false)
                {
                    return false;
                }
            }
            //
            this.logManager.Message.WriteTimeLine("replication client: journals " + Adf.ConvertHelper.ArrayToString<long>(this.journalList, ",", item => { return item + ".journal"; }) + " completed.");
            //
            return true;
        }

        private bool GetJournal(long journalNumber)
        {
            //extra_getjournal {journal number}
            //ex:
            //  extra_getjournal 1
            //  extra_getjournal 2
            //  extra_getjournal 3

            this.logManager.Message.WriteTimeLine("replication client: copy remote " + journalNumber + ".journal to local");

            using (var socket = new Socket(AddressFamily.InterNetwork
                , SocketType.Stream
                , ProtocolType.Tcp))
            {
                socket.NoDelay = true;
                socket.Connect(this.syncSocket.RemoteEndPoint);
                using (var stream = new NetworkStream(socket, false))
                {
                    var command = "extra_getjournal " + journalNumber + "\r\n";
                    var buffer = this.encoding.GetBytes(command);

                    stream.Write(buffer, 0, buffer.Length);

                    //read
                    string line = Adf.StreamHelper.ReadLine(stream, this.encoding);
                    string[] items = line.Split(' ');
                    long bytes = 0;

                    if (items.Length == 2 && items[0] == "VALUE" && long.TryParse(items[1], out bytes))
                    {
                        this.logManager.Message.WriteTimeLine("replication client: copy remote " + bytes + " bytes " + journalNumber + ".journal begin.");

                        var relayFilePath = System.IO.Path.Combine(this.dataPath, "journal.relay.data");
                        using (var fs = new FileStream(relayFilePath, FileMode.Create, FileAccess.Write))
                        {
                            var readAll = 0;
                            var read = 0;
                            buffer = new byte[4096];
                            while (readAll < bytes)
                            {
                                read = stream.Read(buffer, 0, 4096);
                                if (read == 0)
                                {
                                    this.logManager.Message.WriteTimeLine("replication client: " + command + " failure, data copy no finish.");
                                    return false;
                                }
                                readAll += read;
                                fs.Write(buffer, 0, read);
                            }
                        }
                        //
                        this.logManager.Message.WriteTimeLine("replication client: copy remote " + journalNumber + ".journal " + bytes + " bytes to journal.relay.data success.");
                        this.logManager.Message.WriteTimeLine("replication client: marge journal.relay.data to main.data.");
                        //marge journal to data
                        var margeCount = JournalManager.MargeJournalToData(this.logManager, relayFilePath);
                        if (margeCount == -1)
                        {
                            return false;
                        }
                        //
                        File.Delete(relayFilePath);
                        //
                        this.logManager.Message.WriteTimeLine("replication client: marge journal.relay.data total " + margeCount + " rows to main.data success.");
                    }
                    else
                    {
                        this.logManager.Message.WriteTimeLine("replication client: " + command + " failure," + line);
                        return false;
                    }
                }
            }
            return true;
        }

        public bool GetMain()
        {
            this.logManager.Message.WriteTimeLine("replication client: request main data");

            //extra_getmain

            using (var socket = new Socket(AddressFamily.InterNetwork
                , SocketType.Stream
                , ProtocolType.Tcp))
            {
                socket.NoDelay = true;
                socket.Connect(this.syncSocket.RemoteEndPoint);
                using (var stream = new NetworkStream(socket, false))
                {
                    var command = "extra_getmain\r\n";
                    var buffer = this.encoding.GetBytes(command);
                    stream.Write(buffer, 0, buffer.Length);

                    //read
                    string line = Adf.StreamHelper.ReadLine(stream, this.encoding);
                    string[] items = line.Split(' ');
                    long bytes = 0;

                    if (items.Length == 2 && items[0] == "VALUE" && long.TryParse(items[1], out bytes))
                    {
                        this.logManager.Message.WriteTimeLine("replication client: copy remote " + bytes + " bytes main.data begin.");

                        var relayFilePath = System.IO.Path.Combine(this.dataPath, "main.relay.data");
                        using (var fs = new FileStream(relayFilePath, FileMode.Create, FileAccess.Write))
                        {
                            var readAll = 0;
                            var read = 0;
                            buffer = new byte[4096];
                            while (readAll < bytes)
                            {
                                read = stream.Read(buffer, 0, 4096);
                                if (read == 0)
                                {
                                    this.logManager.Message.WriteTimeLine("replication client: " + command + " failure, data copy no finish.");
                                    return false;
                                }
                                readAll += read;
                                fs.Write(buffer, 0, read);
                            }
                        }
                        //
                        this.logManager.Message.WriteTimeLine("replication client: copy remote main.data " + bytes + " bytes to main.relay.data success.");
                        //
                        Program.DataManager.Dispose();
                        var mainFilePath = System.IO.Path.Combine(this.dataPath, "main.data");
                        if (File.Exists(mainFilePath))
                            File.Delete(mainFilePath);
                        File.Move(relayFilePath, mainFilePath);
                        //
                        this.logManager.Message.WriteTimeLine("replication client: move main.relay.data to main.data success.");
                        //re-init data manager
                        Program.DataManager = new DataManager(this.logManager, this.dataPath);
                        //
                        this.logManager.Message.WriteTimeLine("replication client: re-open main.data success.");
                    }
                    else
                    {
                        this.logManager.Message.WriteTimeLine("replication client: " + command + " failure," + line);
                        return false;
                    }
                }
            }

            return true;
        }

        public bool Sync()
        {
            this.logManager.Message.WriteTimeLine("replication client: request replicate sync");

            using (var socket = new Socket(AddressFamily.InterNetwork
                , SocketType.Stream
                , ProtocolType.Tcp))
            {
                socket.NoDelay = true;
                socket.Connect(this.syncSocket.RemoteEndPoint);
                using (var stream = new NetworkStream(socket, false))
                {
                    var command = "extra_replicate_sync\r\n";
                    var buffer = this.encoding.GetBytes(command);

                    stream.Write(buffer, 0, buffer.Length);
                    //read
                    string line = Adf.StreamHelper.ReadLine(stream, this.encoding);
                    if (line != "OK")
                    {
                        this.logManager.Message.WriteTimeLine("replication client: request replicate sync failure, " + line + ".");
                        return false;
                    }
                }
            }

            return true;
        }

        private void Disconnection()
        {
            try { this.syncStream.Close(); }
            catch { }

            try { this.syncSocket.Close(); }
            catch { }
        }

        public void Dispose()
        {
            this.Disconnection();
        }
    }
}