using System;
using System.Net.Sockets;
using System.Net;
using System.IO;
using System.Text;
using System.Collections;

namespace Adf.NumberServer
{
    class MemcachedSession : IDisposable
    {
        //private static readonly byte[] CRLF = new byte[2] { 13, 10 };
        private static readonly byte[] EMPTY = new byte[0];
        private static readonly byte[] DATA_ERROR = null;
        private static readonly byte[] DATA_STORED = null;
        private static readonly byte[] DATA_NOT_STORED = null;
        private static readonly byte[] DATA_NOT_FOUND = null;
        private static readonly byte[] DATA_END = null;
        private static readonly byte[] DATA_OK = null;
        private static readonly byte[] DATA_DELETED = null;
        private static readonly byte[] DATA_TOUCHED = null;
        //
        private const int BUFFER_SIZE = 2048;
        //
        public readonly NetworkStream stream;
        public readonly long id;

        private readonly Socket socket;
        //
        private byte[] buffer = new byte[BUFFER_SIZE];
        private int bufferPosition = 0;

        public bool isExecuting = false;
        public readonly SessionDataItem sdi;

        //private bool isAutoCreate = false;
        
        static MemcachedSession()
        {
            DATA_ERROR = Encoding.ASCII.GetBytes("ERROR\r\n");
            DATA_STORED = Encoding.ASCII.GetBytes("STORED\r\n");
            DATA_NOT_STORED = Encoding.ASCII.GetBytes("NOT_STORED\r\n");
            DATA_NOT_FOUND = Encoding.ASCII.GetBytes("NOT_FOUND\r\n");
            DATA_END = Encoding.ASCII.GetBytes("END\r\n");
            DATA_OK = Encoding.ASCII.GetBytes("OK\r\n");
            DATA_DELETED = Encoding.ASCII.GetBytes("DELETED\r\n");
            DATA_TOUCHED = Encoding.ASCII.GetBytes("TOUCHED\r\n");
        }

        public MemcachedSession(long id, Socket socket)
        {
            this.id = id;
            this.socket = socket;
            this.stream = new NetworkStream(socket, false);
            //
            this.sdi = new SessionDataItem();
            this.sdi.blockData = new BlockData();
            this.sdi.session = this;
        }

        public IPEndPoint GetRemotePoint()
        {
            IPEndPoint ep = null;
            try
            {
                ep = (IPEndPoint)this.socket.RemoteEndPoint;
            }
            catch
            {
            }
            return ep;
        }

        public void Read()
        {
            this.bufferPosition = 0;
            //
            try
            {
                this.stream.BeginRead(this.buffer, 0, 1, this.ReadCallback, null);
            }
            catch (IOException)
            {
                Program.ClosedQueue.Add(this.id);
            }
            catch (ObjectDisposedException)
            {
            }
        }

        private void ReadCallback(IAsyncResult ar)
        {
            int read = 0;
            try
            {
                read = this.stream.EndRead(ar);
            }
            catch (IOException)
            {
                Program.ClosedQueue.Add(this.id);
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            //
            if (read == 0)
            {
                Program.ClosedQueue.Add(this.id);
                return;
            }

            //\n, and new read
            if (this.buffer[0] == 10)
            {
                this.Read();
                return;
            }

            //
            this.bufferPosition = 1;
            try
            {
                Adf.StreamHelper.ReadLine(this.stream, this.buffer, ref this.bufferPosition);
            }
            catch (Exception)
            {
                Program.ClosedQueue.Add(this.id);
                return;
            }

            //
            try
            {
                this.ParseCommand();
            }
            catch (IOException)
            {
                Program.ClosedQueue.Add(this.id);
                return;
            }
            catch (ObjectDisposedException)
            {
                Program.ClosedQueue.Add(this.id);
                return;
            }
            catch (Exception exception)
            {
                Program.LogManager.Exception(exception);
                //
                Program.ClosedQueue.Add(this.id);
                return;
            }
        }

        private void ParseCommand()
        {
            var line = System.Text.Encoding.ASCII.GetString(this.buffer, 0, this.bufferPosition);
            if (line == "")
            {
                this.Read();
                return;
            }

            if (this.isExecuting == true)
            {
                var errorBuffer = Encoding.ASCII.GetBytes("ERROR there is already a command in progress.\r\n");
                this.Send(errorBuffer);
                this.Read();
                return;
            }

            //if (Program.CommandLogWriter.Enable)
            //{
            //    Program.CommandLogWriter.WriteTimeLine(line);
            //}

            var items = line.Split(' ');
            var cmd = items[0];
            if (cmd == "get")
            {
                this.Get(items);
            }
            else if (cmd == "incr")
            {
                this.Incr(items);
            }
            else if (cmd == "decr")
            {
                this.Decr(items);
            }
            else if (cmd == "set")
            {
                this.Set(items);
            }
            else if (cmd == "delete")
            {
                this.Delete(items);
            }
            else if (cmd == "add")
            {
                this.Add(items);
            }
            else if (cmd == "replace")
            {
                this.Replace(items);
            }
            else if (cmd == "append")
            {
                this.ClearStore(items);
            }
            else if (cmd == "prepend")
            {
                this.ClearStore(items);
            }
            //else if (cmd == "touch")
            //{
            //    //this.Touch(items);
            //}
            else if (cmd == "stats")
            {
                this.Stats(items);
            }
            //else if (cmd == "flush_all")
            //{
            //    this.FlushAll();
            //}
            else if (cmd == "version")
            {
                this.Version(items);
            }
            else if (cmd == "quit")
            {
                this.Quit(items);
                return;
            }
            else if (cmd == "gets")
            {
                this.Get(items);
            }
            else if (cmd == "extra_replicate")
            {
                this.Replicate(items);
                return;
            }
            else if (cmd == "extra_getjournal")
            {
                this.GetJournal(items);
            }
            else if (cmd == "extra_getmain")
            {
                this.GetMain(items);
            }
            else if (cmd == "extra_replicate_sync")
            {
                this.ReplicateSync(items);
            }
            else
            {
                this.Send(DATA_ERROR);
            }

            //
            this.Read();
        }

        private void Get(string[] items)
        {
            //get <key>*\r\n
            //using (var m = new MemoryStream(32))
            //{
            //    for (int i = 1, l = items.Length; i < l; i++)
            //    {
            //        if (items[i] != "")
            //        {
            //            this.Get(items[i], m);
            //        }
            //    }

            //    m.Write(DATA_END, 0, DATA_END.Length);
            //    this.Send(m);
            //}

            int l = items.Length;
            if (l == 2)
            {
                this.Get(items[1]);
            }
            else if (l == 4 && items[1] == "*")
            {
                this.GetItems(items);
            }
            else
            {
                for (int i = 1; i < l; i++)
                {
                    if (items[i] != "")
                    {
                        this.Get(items[i]);
                    }
                }
            }

            this.Send(DATA_END);
        }

        private void GetItems(string[] items)
        {
            //get * * 10
            //  * 第1个指示获取所有
            //  * 第2个指示返回前匹配项,
            //      例： get * ab 10  则返回最多10项 匹配ab 或 所有以 ab* 开始的项， 匹配cdef, cdb, cd, 不匹配  bcd, bab
            //           get * cd 20  则返回最多20项 匹配cd 或 所有以 cd* 开始的项， 匹配cdef, cdb, cd, 不匹配  bcd, ccd
            //  10 指示最多获取 10 条数据

            /*
            服务器用以下形式发送每项内容：
            VALUE <key> <flags> <bytes>\r\n
            <data block>\r\n
            - <key> 是所发送的键名
            - <flags> 是存储命令所设置的记号
            - <bytes> 是随后数据块的长度，*不包括* 它的界定符“\r\n”
            - <data block> 是发送的数据
             
             所有项目传送完毕后，服务器发送以下字串：
            "END\r\n"
             */

            var size = 0;
            var counter = 0;
            var prefix = items[2];
            //
            if (int.TryParse(items[3], out size) == false)
            {
                this.Send(DATA_ERROR);
                return;
            }

            if (prefix == "")
            {
                this.Send(DATA_ERROR);
                return;
            }

            if (size == 0)
            {
                size = int.MaxValue;
            }

            //
            if (prefix == "*")
            {
                for (int i = 0, l = Program.NumberManagers.Length;
                    i < l;
                    i++)
                {
                    var m = Program.NumberManagers[i];
                    m.GetItems(item =>
                    {
                        var key = Encoding.ASCII.GetString(item.key_buffer, 0, item.key_length);

                        var data = Encoding.ASCII.GetBytes(item.value.ToString() + "\r\n");
                        var buffer = Encoding.ASCII.GetBytes("VALUE " + key + " 0 " + (data.Length - 2) + "\r\n");

                        this.Send(buffer);
                        this.Send(data);

                        //add counter
                        counter++;

                    }, ref counter, size);
                }
            }
            else
            {
                int prefixLength = prefix.Length;

                for (int i = 0, l = Program.NumberManagers.Length;
                    i < l;
                    i++)
                {
                    var m = Program.NumberManagers[i];
                    m.GetItems(item =>
                    {
                        if (item.key_length >= prefixLength)
                        {
                            var key = Encoding.ASCII.GetString(item.key_buffer, 0, prefixLength);
                            if (key == prefix)
                            {
                                key = Encoding.ASCII.GetString(item.key_buffer, 0, item.key_length);

                                var data = Encoding.ASCII.GetBytes(item.value.ToString() + "\r\n");
                                var buffer = Encoding.ASCII.GetBytes("VALUE " + key + " 0 " + (data.Length - 2) + "\r\n");

                                this.Send(buffer);
                                this.Send(data);
                                
                                //add counter
                                counter++;
                            }
                        }

                    }, ref counter, size);
                }
            }
        }

        private void Get(string key)
        {
            /*
            服务器用以下形式发送每项内容：
            VALUE <key> <flags> <bytes>\r\n
            <data block>\r\n
            - <key> 是所发送的键名
            - <flags> 是存储命令所设置的记号
            - <bytes> 是随后数据块的长度，*不包括* 它的界定符“\r\n”
            - <data block> 是发送的数据
             
             所有项目传送完毕后，服务器发送以下字串：
            "END\r\n"
             */

            DataItem dataItem = null;
            //
            var hashCode = HashHelper.GetHashCode(key);
            var cacheManger = NumberManager.GetManager(key, hashCode);
            //lock (cacheManger.SyncObject)
            //{
            //    dataItem = cacheManger.Get(key, hashCode);
            //}
            //no lock

            dataItem = cacheManger.Get(key, hashCode);

            //
            if (dataItem != null)
            {
                var data = Encoding.ASCII.GetBytes(dataItem.value.ToString() + "\r\n");
                var buffer = Encoding.ASCII.GetBytes("VALUE " + key + " 0 " + (data.Length - 2) + "\r\n");
                //m.Write(buffer, 0, buffer.Length);
                //if (data.Length > 0)
                //{
                //    m.Write(data, 0, data.Length);
                //}
                //m.WriteByte(13);
                //m.WriteByte(10);

                this.Send(buffer);
                this.Send(data);
                //this.Send(CRLF);
            }
        }

        //private void Get(string key, MemoryStream m)
        //{
        //    /*
        //    服务器用以下形式发送每项内容：
        //    VALUE <key> <flags> <bytes>\r\n
        //    <data block>\r\n
        //    - <key> 是所发送的键名
        //    - <flags> 是存储命令所设置的记号
        //    - <bytes> 是随后数据块的长度，*不包括* 它的界定符“\r\n”
        //    - <data block> 是发送的数据
             
        //     所有项目传送完毕后，服务器发送以下字串：
        //    "END\r\n"
        //     */

        //    DataItem dataItem = null;
        //    //
        //    var hashCode = HashHelper.GetHashCode(key);
        //    var cacheManger = NumberManager.GetManager(key, hashCode);
        //    lock (cacheManger.SyncObject)
        //    {
        //        dataItem = cacheManger.Get(key, hashCode);
        //    }
        //    //
        //    if (dataItem != null)
        //    {
        //        var data = Encoding.ASCII.GetBytes(dataItem.value.ToString());
        //        var buffer = Encoding.ASCII.GetBytes("VALUE " + key + " 0 " + data.Length + "\r\n");
        //        m.Write(buffer, 0, buffer.Length);
        //        if (data.Length > 0)
        //        {
        //            m.Write(data, 0, data.Length);
        //        }
        //        m.WriteByte(13);
        //        m.WriteByte(10);
        //    }
        //}
        
        //清除不支持的存储命令传入数据
        private void ClearStore(string[] items)
        {
            //<command name> <key> <flags> <exptime> <bytes> [noreply]\r\n

            var itemLength = items.Length;
            var noreply = itemLength == 6 && items[5] == "noreply";

            if (itemLength == 5 || itemLength == 6)
            {
                try
                {
                    var key = items[1];
                    //var flags = Convert.ToUInt16(items[2]);
                    //var exptime = Convert.ToInt64(items[3]);
                    var bytes = Convert.ToInt32(items[4]);
                    //
                    var data = bytes == 0 ? EMPTY : Adf.StreamHelper.Receive(this.stream, bytes);
                    //clear end \r\n
                    var clearCRLFofEnd = new byte[128];
                    var clearPosition = 0;
                    Adf.StreamHelper.ReadLine(this.stream, clearCRLFofEnd, ref clearPosition);
                }
                catch (Exception exception)
                {
                    if (noreply == false)
                    {
                        var buffer = Encoding.ASCII.GetBytes("CLIENT_ERROR " + exception.GetType().Name + "\r\n");
                        this.Send(buffer);
                    }
                    return;
                }

                //response
                if (noreply == false)
                {
                    this.Send(DATA_ERROR);
                }
            }
            else
            {
                if (noreply == false)
                {
                    this.Send(DATA_ERROR);
                }
            }
        }

        private void Add(string[] items)
        {
            //<command name> <key> <flags> <exptime> <bytes> [noreply]\r\n

            var itemLength = items.Length;
            var noreply = itemLength == 6 && items[5] == "noreply";
            var added = false;

            if (itemLength == 5 || itemLength == 6)
            {
                string valueString = "";
                string key = "";
                ulong valueInteger = 0;
                try
                {
                    key = items[1];
                    var flags = Convert.ToUInt16(items[2]);
                    var exptime = Convert.ToInt64(items[3]);
                    var bytes = Convert.ToInt32(items[4]);
                    //
                    var data = bytes == 0 ? EMPTY : Adf.StreamHelper.Receive(this.stream, bytes);
                    //clear end \r\n
                    var clearCRLFofEnd = new byte[128];
                    var clearPosition = 0;
                    Adf.StreamHelper.ReadLine(this.stream, clearCRLFofEnd, ref clearPosition);
                    //2
                    valueString = Encoding.ASCII.GetString(data);
                    valueInteger = 0L;
                    if (ulong.TryParse(valueString, out valueInteger) == true)
                    {
                        var hashCode = HashHelper.GetHashCode(key);
                        var cacheManger = NumberManager.GetManager(key, hashCode);
                        DataItem item = cacheManger.Get(key, hashCode);
                        //
                        if (item == null)
                        {
                            lock (cacheManger.SyncObject)
                            {
                                //return:  1  replace, 2 add
                                //result = cacheManger.Set(key, hashCode, flags, exptime, data);

                                item = cacheManger.Get(key, hashCode);
                                if (item == null)
                                {
                                    item = new DataItem();
                                    item.value = valueInteger;
                                    //item.key = key;
                                    item.block = Program.DataManager.GetFreeBlock();
                                    item.key_buffer = Encoding.ASCII.GetBytes(key);
                                    item.key_length = (byte)item.key_buffer.Length;
                                    //
                                    added = cacheManger.Add(key, hashCode, item);
                                }
                            }
                        }
                        //
                        if (added == true)
                        {
                            this.isExecuting = true;
                            //
                            lock (item)
                            {
                                this.sdi.returnData = noreply == false ? DATA_STORED : null;

                                Program.Journal.NewJournal(this.sdi, BlockFlag.DATA, item);
                            }
                        }
                        else if (noreply == false)
                        {
                            this.Send(DATA_NOT_STORED);
                        }
                    }
                    else
                    {
                        if (noreply == false)
                        {
                            var buffer = Encoding.ASCII.GetBytes("CLIENT_ERROR value non-numeric\r\n");
                            this.Send(buffer);
                        }
                        return;
                    }
                }
                catch (Exception exception)
                {
                    this.isExecuting = false;

                    if (noreply == false)
                    {
                        var buffer = Encoding.ASCII.GetBytes("CLIENT_ERROR " + exception.GetType().Name + "\r\n");
                        this.Send(buffer);
                    }
                    //Program.ClosedQueue.Add(this.id);
                    return;
                }

                //
                //if (noreply == false)
                //{
                //    if (result == 0)
                //    {
                //        this.Send(DATA_NOT_STORED);
                //    }
                //    else
                //    {
                //        this.Send(DATA_STORED);
                //    }
                //}

            }
            else
            {
                if (noreply == false)
                {
                    this.Send(DATA_ERROR);
                }
            }
        }

        private void Set(string[] items)
        {
            //<command name> <key> <flags> <exptime> <bytes> [noreply]\r\n

            var itemLength = items.Length;
            var noreply = itemLength == 6 && items[5] == "noreply";

            if (itemLength == 5 || itemLength == 6)
            {
                string valueString = "";
                string key = "";
                ulong valueInteger = 0;
                try
                {
                    key = items[1];
                    var flags = Convert.ToUInt16(items[2]);
                    var exptime = Convert.ToInt64(items[3]);
                    var bytes = Convert.ToInt32(items[4]);
                    //
                    var data = bytes == 0 ? EMPTY : Adf.StreamHelper.Receive(this.stream, bytes);
                    //clear end \r\n
                    var clearCRLFofEnd = new byte[128];
                    var clearPosition = 0;
                    Adf.StreamHelper.ReadLine(this.stream, clearCRLFofEnd, ref clearPosition);
                    //2
                    valueString = Encoding.ASCII.GetString(data);
                    valueInteger = 0L;
                    if (ulong.TryParse(valueString, out valueInteger) == true)
                    {
                        var hashCode = HashHelper.GetHashCode(key);
                        var cacheManger = NumberManager.GetManager(key, hashCode);
                        var item = cacheManger.Get(key, hashCode);
                        //
                        if (item == null)
                        {
                            lock (cacheManger.SyncObject)
                            {
                                //return:  1  replace, 2 add
                                //result = cacheManger.Set(key, hashCode, flags, exptime, data);

                                item = cacheManger.Get(key, hashCode);
                                if (item == null)
                                {
                                    item = new DataItem();
                                    item.value = valueInteger;
                                    //item.key = key;
                                    item.block = Program.DataManager.GetFreeBlock();
                                    item.key_buffer = Encoding.ASCII.GetBytes(key);
                                    item.key_length = (byte)item.key_buffer.Length;
                                    cacheManger.Add(key, hashCode, item);
                                }
                            }
                        }
                        //
                        this.isExecuting = true;
                        //
                        lock (item)
                        {
                            item.value = valueInteger;

                            this.sdi.returnData = noreply == false ? DATA_STORED : null;

                            Program.Journal.NewJournal(this.sdi, BlockFlag.DATA, item);
                        }

                    }
                    else
                    {
                        if (noreply == false)
                        {
                            var buffer = Encoding.ASCII.GetBytes("CLIENT_ERROR value non-numeric\r\n");
                            this.Send(buffer);
                        }
                        return;
                    }
                }
                catch (Exception exception)
                {
                    this.isExecuting = false;

                    if (noreply == false)
                    {
                        var buffer = Encoding.ASCII.GetBytes("CLIENT_ERROR " + exception.GetType().Name + "\r\n");
                        this.Send(buffer);
                    }
                    //Program.ClosedQueue.Add(this.id);
                    return;
                }

                //
                //if (noreply == false)
                //{
                //    if (result == 0)
                //    {
                //        this.Send(DATA_NOT_STORED);
                //    }
                //    else
                //    {
                //        this.Send(DATA_STORED);
                //    }
                //}

            }
            else
            {
                if (noreply == false)
                {
                    this.Send(DATA_ERROR);
                }
            }
        }

        private void Replace(string[] items)
        {
            //<command name> <key> <flags> <exptime> <bytes> [noreply]\r\n

            var itemLength = items.Length;
            var noreply = itemLength == 6 && items[5] == "noreply";

            if (itemLength == 5 || itemLength == 6)
            {
                string valueString = "";
                string key = "";
                ulong valueInteger = 0;
                try
                {
                    key = items[1];
                    var flags = Convert.ToUInt16(items[2]);
                    var exptime = Convert.ToInt64(items[3]);
                    var bytes = Convert.ToInt32(items[4]);
                    //
                    var data = bytes == 0 ? EMPTY : Adf.StreamHelper.Receive(this.stream, bytes);
                    //clear end \r\n
                    var clearCRLFofEnd = new byte[128];
                    var clearPosition = 0;
                    Adf.StreamHelper.ReadLine(this.stream, clearCRLFofEnd, ref clearPosition);
                    //2
                    valueString = Encoding.ASCII.GetString(data);
                    valueInteger = 0L;
                    if (ulong.TryParse(valueString, out valueInteger) == true)
                    {
                        var hashCode = HashHelper.GetHashCode(key);
                        var cacheManger = NumberManager.GetManager(key, hashCode);
                        var item = cacheManger.Get(key, hashCode);
                        //
                        if (item == null)
                        {
                            lock (cacheManger.SyncObject)
                            {
                                item = cacheManger.Get(key, hashCode);
                            }
                        }
                        //
                        if (item == null)
                        {
                            if (noreply == false)
                            {
                                this.Send(DATA_NOT_STORED);
                            }
                        }
                        else
                        {
                            this.isExecuting = true;
                            //
                            lock (item)
                            {
                                item.value = valueInteger;

                                this.sdi.returnData = noreply == false ? DATA_STORED : null;

                                Program.Journal.NewJournal(this.sdi, BlockFlag.DATA, item);
                            }
                        }
                    }
                    else
                    {
                        if (noreply == false)
                        {
                            var buffer = Encoding.ASCII.GetBytes("CLIENT_ERROR value non-numeric\r\n");
                            this.Send(buffer);
                        }
                        return;
                    }
                }
                catch (Exception exception)
                {
                    this.isExecuting = false;

                    if (noreply == false)
                    {
                        var buffer = Encoding.ASCII.GetBytes("CLIENT_ERROR " + exception.GetType().Name + "\r\n");
                        this.Send(buffer);
                    }
                    //Program.ClosedQueue.Add(this.id);
                    return;
                }

                //
                //if (noreply == false)
                //{
                //    if (result == 0)
                //    {
                //        this.Send(DATA_NOT_STORED);
                //    }
                //    else
                //    {
                //        this.Send(DATA_STORED);
                //    }
                //}

            }
            else
            {
                if (noreply == false)
                {
                    this.Send(DATA_ERROR);
                }
            }
        }

        private void Delete(string[] items)
        {
            //old version:  delete <key> <time>\r\n

            //new version:  delete <key> [noreply]\r\n

            //同时支持两咱格式

            /*
            原始命令：
            命令“delete”允许从外部删除内容：
            delete <key> <time>\r\n
            - <key> 是客户端希望服务器删除的内容的键名
            - <time> 是一个单位为秒的时间（或代表直到某一刻的Unix时间），在该时间内服务器会拒绝对于此键名的“add”和“replace”命令。此时内容被放入delete队列，无法再通过“get”得到该内容，也无法是用“add”和“replace”命令（但是“set”命令可用）。直到指定时间，这些内容被最终从服务器的内存中彻底清除。
            <time>参数 是可选的，缺省为0（表示内容会立刻清除，并且随后的存储命令均可用）
             
             */

            var length = items.Length;
            var noreply = length == 3 && items[2] == "noreply";
            //
            if (length == 2 || length == 3)
            {
                DataItem item = null;

                try
                {
                    var key = items[1];
                    //
                    var hashCode = HashHelper.GetHashCode(key);
                    var cacheManger = NumberManager.GetManager(key, hashCode);
                    lock (cacheManger.SyncObject)
                    {
                        item = cacheManger.Get(key, hashCode);
                        if (item != null)
                        {
                            cacheManger.Remove(key, hashCode);
                        }
                    }
                    //
                    if (item != null)
                    {
                        this.isExecuting = true;

                        lock (item)
                        {
                            this.sdi.returnData = noreply == false ? DATA_DELETED : null;

                            Program.Journal.NewJournal(this.sdi, BlockFlag.FREE, item);
                        }
                    }
                    else
                    {
                        if (noreply == false)
                        {
                            this.Send(DATA_NOT_FOUND);
                        }
                    }
                }
                catch (Exception exception)
                {
                    this.isExecuting = false;

                    if (noreply == false)
                    {
                        var buffer = Encoding.ASCII.GetBytes("CLIENT_ERROR " + exception.GetType().Name + "\r\n");
                        this.Send(buffer);
                    }
                    //Program.ClosedQueue.Add(this.id);
                    return;
                }
                //
                //if (noreply == false)
                //{
                //    if (result == false)
                //    {
                //        this.Send(DATA_NOT_FOUND);
                //    }
                //    else
                //    {
                //        this.Send(DATA_DELETED);
                //    }
                //}
            }
            else
            {
                if (noreply == false)
                {
                    this.Send(DATA_ERROR);
                }
            }
        }

        private void Incr(string[] items)
        {
            //incr <key> <value> [noreply]\r\n

            //回复为以下集中情形：
            //- "NOT_FOUND\r\n"
            //指示该项内容的值，不存在。
            //- <value>\r\n ，<value>是 增加/减少 。

            var length = items.Length;
            var noreply = length == 4 && items[3] == "noreply";

            if (length == 3 || length == 4)
            {
                try
                {
                    var key = items[1];
                    var increment = 0L;
                    //
                    if (long.TryParse(items[2], out increment) == true)
                    {
                        var hashCode = HashHelper.GetHashCode(key);
                        var cacheManger = NumberManager.GetManager(key, hashCode);
                        var item = cacheManger.Get(key, hashCode);
                        //
                        if (item == null)
                        {
                            //lock (cacheManger.SyncObject)
                            //{
                            //    item = cacheManger.Get(key, hashCode);
                            //    if (item == null)
                            //    {
                            //        item = new DataItem();
                            //        item.value = 0;
                            //        //item.key = key;
                            //        item.block = Program.DataManager.GetFreeBlock();
                            //        item.key_buffer = Encoding.ASCII.GetBytes(key);
                            //        item.key_length = (byte)item.key_buffer.Length;
                            //        cacheManger.Add(key, hashCode, item);
                            //    }
                            //}

                            if (noreply == false)
                            {
                                this.Send(DATA_NOT_FOUND);
                            }
                            return;
                        }
                        //
                        this.isExecuting = true;
                        //
                        lock (item)
                        {
                            //delta
                            if (increment > 0)
                            {
                                item.value += (ulong)increment;
                            }
                            else
                            {
                                item.value -= (ulong)Math.Abs(increment);
                            }

                            //this.sdi.returnData = noreply == false ? DATA_STORED : null;
                            this.sdi.returnData = noreply == false ? Encoding.ASCII.GetBytes(item.value.ToString() + "\r\n") : null;

                            Program.Journal.NewJournal(this.sdi, BlockFlag.DATA, item);
                        }
                    }
                    else
                    {
                        if (noreply == false)
                        {
                            var buffer = Encoding.ASCII.GetBytes("CLIENT_ERROR value non-numeric\r\n");
                            this.Send(buffer);
                        }
                        return;
                    }
                }
                catch (Exception exception)
                {
                    this.isExecuting = false;

                    if (noreply == false)
                    {
                        var buffer = Encoding.ASCII.GetBytes("CLIENT_ERROR " + exception.GetType().Name + "\r\n");
                        this.Send(buffer);
                    }
                    //Program.ClosedQueue.Add(this.id);
                    return;
                }
            }
            else
            {
                if (noreply == false)
                {
                    this.Send(DATA_ERROR);
                }
            }
        }

        private void Decr(string[] items)
        {
            //decr <key> <value> [noreply]\r\n

            //回复为以下集中情形：
            //- "NOT_FOUND\r\n"
            //指示该项内容的值，不存在。
            //- <value>\r\n ，<value>是 增加/减少 。

            var length = items.Length;
            var noreply = length == 4 && items[3] == "noreply";

            if (length == 3 || length == 4)
            {
                try
                {
                    var key = items[1];
                    var increment = 0L;
                    //
                    if (long.TryParse(items[2], out increment) == true)
                    {
                        var hashCode = HashHelper.GetHashCode(key);
                        var cacheManger = NumberManager.GetManager(key, hashCode);
                        var item = cacheManger.Get(key, hashCode);
                        //
                        if (item == null)
                        {
                            //lock (cacheManger.SyncObject)
                            //{
                            //    item = cacheManger.Get(key, hashCode);
                            //    if (item == null)
                            //    {
                            //        item = new DataItem();
                            //        item.value = 0;
                            //        //item.key = key;
                            //        item.block = Program.DataManager.GetFreeBlock();
                            //        item.key_buffer = Encoding.ASCII.GetBytes(key);
                            //        item.key_length = (byte)item.key_buffer.Length;
                            //        cacheManger.Add(key, hashCode, item);
                            //    }
                            //}


                            if (noreply == false)
                            {
                                this.Send(DATA_NOT_FOUND);
                            }
                            return;
                        }
                        //
                        this.isExecuting = true;
                        //
                        lock (item)
                        {
                            if (increment > 0)
                            {
                                var delta = (ulong)increment;
                                if (item.value > delta)
                                {
                                    item.value -= delta;
                                }
                                else
                                {
                                    //not allow decr to less than zero.
                                    item.value = 0;
                                }
                            }
                            else
                            {
                                item.value += (ulong)Math.Abs(increment);
                            }

                            //this.sdi.returnData = noreply == false ? DATA_STORED : null;
                            this.sdi.returnData = noreply == false ? Encoding.ASCII.GetBytes(item.value.ToString() + "\r\n") : null;

                            Program.Journal.NewJournal(this.sdi, BlockFlag.DATA, item);
                        }
                    }
                    else
                    {
                        if (noreply == false)
                        {
                            var buffer = Encoding.ASCII.GetBytes("CLIENT_ERROR value non-numeric\r\n");
                            this.Send(buffer);
                        }
                        return;
                    }
                }
                catch (Exception exception)
                {
                    this.isExecuting = false;

                    if (noreply == false)
                    {
                        var buffer = Encoding.ASCII.GetBytes("CLIENT_ERROR " + exception.GetType().Name + "\r\n");
                        this.Send(buffer);
                    }
                    //Program.ClosedQueue.Add(this.id);
                    return;
                }
            }
            else
            {
                if (noreply == false)
                {
                    this.Send(DATA_ERROR);
                }
            }
        }

  //      private void Touch(string[] items)
  //      {
  //          //touch <key> <exptime> [noreply]\r\n

  //          var length = items.Length;
  //          var noreply = length == 4 && items[3] == "noreply";
  //          //
  //          if (length == 3 || length == 4)
  //          {
  //              bool result = false;
  //              /* <exptime> is expiration time. Works the same as with the update commands
  //(set/add/etc). This replaces the existing expiration time. If an existing
  //item were to expire in 10 seconds, but then was touched with an
  //expiration time of "20", the item would then expire in 20 seconds.*/

  //              try
  //              {
  //                  var key = items[1];
  //                  var expires = Convert.ToInt64(items[2]);

  //                  //
  //                  var hashCode = HashHelper.GetHashCode(key);
  //                  var cacheManger = NumberManager.GetManager(key, hashCode);
  //                  lock (cacheManger.SyncObject)
  //                  {
  //                      result = cacheManger.Expires(key, hashCode, expires);
  //                  }
  //              }
  //              catch (Exception exception)
  //              {
  //                  if (noreply == false)
  //                  {
  //                      var buffer = Encoding.ASCII.GetBytes("CLIENT_ERROR " + exception.GetType().Name + "\r\n");
  //                      this.Send(buffer);
  //                  }
  //                  //Program.ClosedQueue.Add(this.id);
  //                  return;
  //              }
  //              //
  //              if (noreply == false)
  //              {
  //                  if (result == false)
  //                  {
  //                      this.Send(DATA_NOT_FOUND);
  //                  }
  //                  else
  //                  {
  //                      this.Send(DATA_TOUCHED);
  //                  }
  //              }
  //          }
  //          else
  //          {
  //              if (noreply == false)
  //              {
  //                  this.Send(DATA_ERROR);
  //              }
  //          }
  //      }

        private void Stats(string[] items)
        {
            if (items.Length == 1)
            {
            }
            else
            {
                this.Send(DATA_ERROR);
                return;
            }

            /*
             STAT pid 2348
            STAT uptime 5794443
            STAT time 1496826343
            STAT version 1.2.6
            STAT pointer_size 32
            STAT curr_items 3
            STAT total_items 46
            STAT bytes 165
            STAT curr_connections 3
            STAT total_connections 21
            STAT connection_structures 5
            STAT cmd_get 59
            STAT cmd_set 47
            STAT get_hits 57
            STAT get_misses 2
            STAT evictions 0
            STAT bytes_read 1746
            STAT bytes_written 5741
            STAT limit_maxbytes 67108864
            STAT threads 1
            END
             */

            //STAT <name> <value>\r\n
            //服务器用以下一行来终止这个清单：
            //END\r\n

            var curr_items = 0;

            for (int i = 0; i < Program.HASH_POOL_SIZE; i++)
            {
                curr_items += Program.NumberManagers[i].Count;
            }

            var now = Adf.UnixTimestampHelper.ToInt64Timestamp();

            //
            var table = new System.Collections.SortedList();
            table.Add("pid", System.Diagnostics.Process.GetCurrentProcess().Id);
            table.Add("uptime", now - Program.UPTIME);
            table.Add("time", now);
            table.Add("version", Program.VERSION);
            table.Add("hash_pool_size", Program.HASH_POOL_SIZE);

            table.Add("curr_connections", Program.Listen.GetSessionCount());
            table.Add("curr_items", curr_items);
            //table.Add("auto_create", (this.isAutoCreate ? "enabled" : "disabled"));

            table.Add("serv_state", Program.ServiceContext.ServiceState.ToString());

            if (Program.ServiceContext.HAEnable == true)
            {
                table.Add("ha_enable", "1");
                table.Add("ha_master", Program.ServiceContext.GetMaster());
            }
            else
            {
                table.Add("ha_enable", "0");
            }

            table.Add("journal_enable", Program.Journal.Enable ? "1" : "0");
            if (Program.Journal.Enable)
            {
                table.Add("journal_file", Program.Journal.GetFileName());
                table.Add("journal_rotate", Program.Journal.GetJournalRotate);
                table.Add("journal_length", Program.Journal.GetJournalLength);
            }

            table.Add("replication_enable", Program.Journal.replicationEnable ? "1" : "0");
            if (Program.Journal.replicationEnable)
            {
                table.Add("replication_requesting", Program.Journal.replicationRequestingFlag ? "1" : "0");
            }

            //
            var build = new StringBuilder();
            var enumerator = table.GetEnumerator();
            while (enumerator.MoveNext())
            {
                build.AppendFormat("STAT {0} {1}\r\n", enumerator.Key, enumerator.Value);
            }
            build.Append("END\r\n");

            //
            var buffer = Encoding.ASCII.GetBytes(build.ToString());
            this.Send(buffer);
        }

        //private void FlushAll()
        //{
        //    for (int i = 0; i < Program.HASH_POOL_SIZE; i++)
        //    {
        //        lock (Program.NumberManagers[i].SyncObject)
        //        {
        //            //Program.NumberManagers[i].Clear();
        //            Program.NumberManagers[i] = new NumberManager();
        //        }
        //    }

        //    //OK\r\n
        //    this.Send(DATA_OK);
        //}

        private void Version(string[] items)
        {
            //VERSION <version>\r\n
            var buffer = Encoding.ASCII.GetBytes("VERSION " + Program.VERSION + "\r\n");
            this.Send(buffer);
        }

        private void Quit(string[] items)
        {
            Program.ClosedQueue.Add(this.id);
        }

//        private void UnknowCommand(string[] items)
//        {
//            /*
//错误字串
//每一个由客户端发送的命令，都可能收到来自服务器的错误字串回复。这些错误字串会以三种形式出现：
//- "ERROR\r\n"
//意味着客户端发送了不存在的命令名称。
//- "CLIENT_ERROR <error>\r\n"
//意味着输入的命令行里存在一些客户端错误，例如输入未遵循协议。<error>部分是人类易于理解的错误解说……
//- "SERVER_ERROR <error>\r\n"
//意味着一些服务器错误，导致命令无法执行。<error>部分是人类易于理解的错误解说。在一些严重的情形下（通常应该不会遇到），服务器将在发送这行错误后关闭连接。这是服务器主动关闭连接的唯一情况。
//在后面每项命令的描述中，这些错误行不会再特别提到，但是客户端必须考虑到这些它们存在的可能性
//             */
//            this.Send(DATA_ERROR);
//        }

        //private void Send(MemoryStream m)
        //{
        //    this.stream.BeginWrite(m.GetBuffer(), 0, (int)m.Length, this.SendCallback, null);
        //}

        //public void Send(byte[] buffer)
        //{
        //    try
        //    {
        //        this.stream.Write(buffer, 0, buffer.Length);
        //    }
        //    catch(IOException)
        //    {
        //        Program.ClosedQueue.Add(this.id);
        //    }
        //    catch (ObjectDisposedException)
        //    {
        //    }
        //}

        ///// <summary>
        ///// 发送异步待写出数据，并进行新的命令读取
        ///// </summary>
        //private void SendWaitAndNewRead()
        //{
        //    if (this.waitSend != null)
        //    {
        //        this.Send(this.waitSend);
        //    }
        //    this.Read();
        //}

        private readonly object sendSync = new object();

        public void Send(byte[] buffer)
        {
            try
            {
                lock (this.sendSync)
                {
                    this.stream.BeginWrite(buffer, 0, buffer.Length, this.SendCallback, null);
                }
            }
            catch (IOException)
            {
                Program.ClosedQueue.Add(this.id);
            }
            catch (ObjectDisposedException)
            {
            }
        }

        private void SendCallback(IAsyncResult ar)
        {
            try
            {
                this.stream.EndWrite(ar);
            }
            catch (IOException)
            {
                Program.ClosedQueue.Add(this.id);
            }
            catch (ObjectDisposedException)
            {
            }
        }

        private void Replicate(string[] items)
        {
            //extra_replicate

            try
            {
                Program.Journal.Replicate(this);
            }
            catch (Exception exception)
            {
                Program.LogManager.Exception(exception);
                //
                var buffer = Encoding.ASCII.GetBytes("SERVER_ERROR extra_replicate request failure, please check server logs.\r\n");
                this.Send(buffer);
                //
                Program.ClosedQueue.Add(this.id);
            }
        }

        private void GetMain(string[] items)
        {
            //extra_getmain
                //get data
                lock (Program.DataManager.SyncObject)
                {
                    try
                    {
                        var length = Program.DataManager.GetLength();

                        // VALUE <bytes> 
                        var buffer = Encoding.ASCII.GetBytes("VALUE " + length + "\r\n");
                        this.stream.Write(buffer, 0, buffer.Length);
                        //
                        Program.DataManager.CopyTo(this.stream);
                    }
                    catch (IOException)
                    {
                        //ignore file io exception, only follow network exception
                        Program.ClosedQueue.Add(this.id);
                        return;
                    }
                    catch (ObjectDisposedException)
                    {
                        //ignore
                        return;
                    }
                    catch (Exception exception)
                    {
                        Program.LogManager.Exception(exception);
                        //
                        var buffer = Encoding.ASCII.GetBytes("SERVER_ERROR extra_getmain failure, please check server logs.\r\n");
                        this.Send(buffer);
                        //
                        Program.ClosedQueue.Add(this.id);
                        //
                        return;
                    }
                }

            //new read
            this.Read();
        }

        private void GetJournal(string[] items)
        {
            var journalNumber = 0L;

            //extra_getjournal {journal number}
            //ex:
            //  getdata 0 -- 0 is data
            //  getdata 1
            //  getdata 2
            //  getdata 3

            if (items.Length != 2 || long.TryParse(items[1], out journalNumber) == false || journalNumber < 1)
            {
                var buffer = Encoding.ASCII.GetBytes("CLIENT_ERROR invalid journal number.\r\n");
                this.Send(buffer);
                //
                Program.ClosedQueue.Add(this.id);
                //
                return;
            }

            //get journal
            try
            {
                var length = Program.Journal.GetLength(journalNumber);

                // VALUE <bytes> 
                var buffer = Encoding.ASCII.GetBytes("VALUE " + length + "\r\n");
                this.stream.Write(buffer, 0, buffer.Length);
                //
                Program.Journal.CopyTo(journalNumber, this.stream);
            }
            catch (IOException)
            {
                //ignore file io exception, only follow network exception
                Program.ClosedQueue.Add(this.id);
                return;
            }
            catch (ObjectDisposedException)
            {
                //ignore
                return;
            }
            catch (Exception exception)
            {
                Program.LogManager.Exception(exception);
                //
                var buffer = Encoding.ASCII.GetBytes("SERVER_ERROR extra_getjournal failure, please check server logs.\r\n");
                this.Send(buffer);
                //
                Program.ClosedQueue.Add(this.id);
                //
                return;
            }

            //new read
            this.Read();

        }
                
        private void ReplicateSync(string[] items)
        {
            //extra_replicate_sync

            try
            {
                Program.Journal.ReplicateDataCompleted();
            }
            catch (Exception exception)
            {
                Program.LogManager.Exception(exception);
                //
                var buffer = Encoding.ASCII.GetBytes("SERVER_ERROR extra_replicate_sync failure, please check server logs.\r\n");
                this.Send(buffer);
                //
                Program.ClosedQueue.Add(this.id);
                //
                return;
            }

            this.Send(DATA_OK);
        }

        public void Dispose()
        {
            this.stream.Close();
            this.stream.Dispose();
            this.socket.Close();
        }
    }
}