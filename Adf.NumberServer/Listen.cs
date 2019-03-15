using System;
using System.Net.Sockets;
using System.Net;
using System.Collections.Generic;
using System.IO;

namespace Adf.NumberServer
{
    class Listen : IDisposable
    {
        const int BUCKETS = 32;
        Dictionary<long, MemcachedSession>[] sessionDictionarys;
        Socket listenSocket;
        long identityBase = 0;

        public Listen()
        {
            var port = Adf.ConfigHelper.GetSettingAsInt("Port", 6224);
            var ip = Adf.ConfigHelper.GetSetting("Ip", "*");
            var backlog = Adf.ConfigHelper.GetSettingAsInt("Backlog", 4096);
            //
            var listenIp = ip == "*" ? IPAddress.Any : IPAddress.Parse(ip);
            //
            this.sessionDictionarys = new Dictionary<long, MemcachedSession>[BUCKETS];
            for (int i = 0; i < BUCKETS; i++)
                this.sessionDictionarys[i] = new Dictionary<long, MemcachedSession>(32);
            //
            Program.LogManager.Message.WriteTimeLine("Listen {0}:{1}", listenIp, port);
            //
            this.listenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            this.listenSocket.Bind(new IPEndPoint(listenIp, port));
            this.listenSocket.Listen(backlog);
            //
            this.listenSocket.BeginAccept(this.AcceptCallback, null);
        }

        public int GetSessionCount()
        {
            int count = 0;

            for (int i = 0; i < BUCKETS; i++)
                count += this.sessionDictionarys[i].Count;

            return count;
        }

        private void AcceptCallback(IAsyncResult ar)
        {
            Socket socket = null;
            try
            {
                socket = this.listenSocket.EndAccept(ar);
            }
            catch { }

            //new accept
            try
            {
                this.listenSocket.BeginAccept(this.AcceptCallback, null);
            }
            catch (ObjectDisposedException) { }

            //
            if (socket != null)
            {
                MemcachedSession session = null;
                var id = System.Threading.Interlocked.Increment(ref this.identityBase);
                var bucket = id % BUCKETS;
                lock (this.sessionDictionarys[bucket])
                {
                    session = new MemcachedSession(id, socket);
                    this.sessionDictionarys[bucket].Add(id, session);
                }
                //
                session.Read();
            }
        }

        public void CloseSession(long id)
        {
            var bucket = id % BUCKETS;
            MemcachedSession session = null;
            lock (this.sessionDictionarys[bucket])
            {
                this.sessionDictionarys[bucket].TryGetValue(id, out session);
                this.sessionDictionarys[bucket].Remove(id);
            }
            if (session == null)
            {
                return;
            }
            //
            try
            {
                session.Dispose();
            }
            catch (SocketException)
            {
                //ignore
            }
            catch (IOException)
            {
                //ignore
            }
            catch (Exception exception)
            {
                Program.LogManager.Exception(exception);
            }
        }

        public MemcachedSession GetSession(long id)
        {
            var bucket = id % BUCKETS;
            MemcachedSession session = null;
            lock (this.sessionDictionarys[bucket])
            {
                this.sessionDictionarys[bucket].TryGetValue(id, out session);
            }
            return session;
        }

        public void Dispose()
        {
            this.listenSocket.Close();
            //
            for (int i = 0; i < BUCKETS; i++)
            {
                lock (this.sessionDictionarys[i])
                {
                    foreach (var item in this.sessionDictionarys[i])
                    {
                        try
                        {
                            item.Value.Dispose();
                        }
                        catch
                        {
                            //ignore
                        }
                    }
                    //
                    this.sessionDictionarys[i].Clear();
                }
            }
        }
    }
}