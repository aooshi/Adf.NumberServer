using System;

namespace Adf.NumberServer
{
    class Program : Adf.Service.IService
    {
        public static string DATA_PATH;
        //
        public const Int32 HASH_POOL_SIZE = 255;
        public static Int64 UPTIME = 0;
        //
        public static QueueTask<long> ClosedQueue;
        public static NumberManager[] NumberManagers;
        //
        //public static LogWriter CommandLogWriter;
        public static LogManager LogManager;
        public static Adf.Service.ServiceContext ServiceContext;
        public static Listen Listen;
        //
        public static DataManager DataManager;
        public static JournalManager Journal;
        //
        public static string VERSION;

        private System.Threading.Thread loadThread;
        private System.Threading.Thread slaveThread;

        private ReplicationClient replicationClient;

        private bool running = false;

        static void Main(string[] args)
        {
            Adf.Service.ServiceHelper.Entry(args);
        }

        public void Start(Adf.Service.ServiceContext serviceContext)
        {
            Program.UPTIME = Adf.UnixTimestampHelper.ToInt64Timestamp();
            //
            var version = this.GetType().Assembly.GetName().Version;
            Program.VERSION = string.Concat(version.Major, ".", version.Minor, ".", version.Build);
            //
            Program.ServiceContext = serviceContext;
            Program.LogManager = serviceContext.LogManager;
            //Program.CommandLogWriter = serviceContext.LogManager.GetWriter("Command");
            //Program.CommandLogWriter.Enable = Adf.ConfigHelper.GetSettingAsBoolean("CommandLog", false);
            //
            Program.DATA_PATH = Adf.ConfigHelper.GetSetting("DataPath");
            if (Program.DATA_PATH == "")
            {
                throw new ConfigException("no configuration DataPath item");
            }
            else if (System.IO.Directory.Exists(Program.DATA_PATH) == false)
            {
                System.IO.Directory.CreateDirectory(Program.DATA_PATH);
            }
            //
            serviceContext.LogManager.Message.WriteTimeLine("LogPath:" + serviceContext.LogManager.Path);
            serviceContext.LogManager.Message.WriteTimeLine("LogFlushInterval:" + serviceContext.LogManager.FlushInterval);
            serviceContext.LogManager.Message.WriteTimeLine("DataPath:" + Program.DATA_PATH);

            //
            if (serviceContext.HAEnable == true)
            {
                serviceContext.ToMaster += new EventHandler(this.ToMaster);
                serviceContext.ToSlave += new EventHandler(this.ToSlave);
                serviceContext.ToRestore += new EventHandler(this.ToRestore);
                serviceContext.ToWitness += new EventHandler(this.ToWitness);
            }
            else
            {
                this.Start();
            }
        }

        private void ToMaster(object sender, EventArgs e)
        {
            this.Start();
        }

        private void ToSlave(object sender, EventArgs e)
        {
            this.StartSlave();
        }

        private void ToRestore(object sender, EventArgs e)
        {
            this.Stop();
        }

        private void ToWitness(object sender, EventArgs e)
        {
            //
        }

        private void LoadAction()
        {
            try
            {
                this.LoadData(Program.ServiceContext);
                this.LoadJournal(Program.ServiceContext);

                var rows = 0;
                for (int i = 0; i < Program.NumberManagers.Length; i++)
                {
                    rows += Program.NumberManagers[i].Count;
                }

                Program.ServiceContext.LogManager.Message.WriteTimeLine("All " + rows + " keys");

                //listen
                Program.Listen = new Listen();
            }
            catch (System.Threading.ThreadAbortException)
            {
                //ignore
            }
        }

        private void LoadJournal(Service.ServiceContext serviceContext)
        {
            serviceContext.LogManager.Message.WriteTimeLine("Load journal begin");

            var manager = Program.Journal;
            var rows = 0;
            manager.LoadJournal((byte flag, DataItem item) =>
            {
                var key = System.Text.Encoding.ASCII.GetString(item.key_buffer, 0, item.key_length);
                var hashCode = HashHelper.GetHashCode(key);
                var cacheManger = NumberManager.GetManager(key, hashCode);

                if (flag == BlockFlag.DATA)
                {
                    cacheManger.Set(key, hashCode, item);
                }
                else if (flag == BlockFlag.FREE)
                {
                    cacheManger.Remove(key, hashCode);
                }

                rows++;
            });

            serviceContext.LogManager.Message.WriteTimeLine("Load journal " + rows + " all rows");
            serviceContext.LogManager.Message.WriteTimeLine("Load journal end");

            //
            if (manager.Enable)
            {
                manager.Open();
            }
        }

        private void LoadData(Service.ServiceContext serviceContext)
        {
            serviceContext.LogManager.Message.WriteTimeLine("Load data begin");

            var manager = Program.DataManager;
            var rows = 0;
            manager.LoadData((byte flag, DataItem item) =>
            {
                if (flag == BlockFlag.DATA)
                {
                    var key = System.Text.Encoding.ASCII.GetString(item.key_buffer, 0, item.key_length);
                    var hashCode = HashHelper.GetHashCode(key);
                    var cacheManger = NumberManager.GetManager(key, hashCode);
                    //
                    cacheManger.Set(key, hashCode, item);
                    rows++;
                }
            });

            serviceContext.LogManager.Message.WriteTimeLine("Load data " + rows + " rows");
            serviceContext.LogManager.Message.WriteTimeLine("Load data end");
        }

        private void InitializeJournal(Service.ServiceContext serviceContext)
        {
            string dataPath = Program.DATA_PATH;
            LogManager logManager = serviceContext.LogManager;
            //
            JournalManager manager = new JournalManager(logManager, dataPath);
            var jl = Adf.ConfigHelper.GetSetting("Journal", "disk");
            //
            if ("memory".Equals(jl, StringComparison.OrdinalIgnoreCase))
            {
                serviceContext.LogManager.Message.WriteTimeLine("Journal: disabled");
                manager.Enable = false;
            }
            else if ("disk".Equals(jl, StringComparison.OrdinalIgnoreCase))
            {
                serviceContext.LogManager.Message.WriteTimeLine("Journal: enabled");
                manager.Enable = true;
            }
            else
            {
                throw new ConfigException("Journal configuration invalid");
            }
            //
            Program.Journal = manager;
        }

        private void InitializeData(Service.ServiceContext serviceContext)
        {
            Program.DataManager = new DataManager(serviceContext.LogManager, Program.DATA_PATH);
        }

        private void InitializeSlave()
        {
            var serviceContext = Program.ServiceContext;
            var port = Adf.ConfigHelper.GetSettingAsInt("Port", 201);
            var logManager = serviceContext.LogManager;

            try
            {

                while (Program.ServiceContext.ServiceState == Service.ServiceState.Slave)
                {
                    var master = serviceContext.GetMaster();
                    if (master == null)
                        break;

                    try
                    {
                        this.replicationClient.Dispose();
                    }
                    catch { }


                    System.Threading.Thread.Sleep(5000);

                    //
                    try
                    {
                        this.replicationClient = new ReplicationClient(serviceContext.LogManager
                            , Program.DATA_PATH
                            , master
                            , port);

                        this.replicationClient.IOError += new EventHandler<ReplicationExceptionEventArgs>(this.ReplicationClientIOError);
                        this.replicationClient.Request();

                        var success = this.replicationClient.GetMain();
                        if (success == true)
                        {
                            success = this.replicationClient.GetJournal();
                        }

                        if (success == true)
                        {
                            success = this.replicationClient.Sync();
                        }

                        if (success == true)
                        {
                            break;
                        }
                    }
                    catch (Exception exception)
                    {
                        logManager.Message.WriteTimeLine("connection master failure, " + exception.Message + ".");
                    }
                }
            }
            catch (System.Threading.ThreadAbortException)
            {
                //ignore
            }
        }

        private void ReplicationClientIOError(object sender, ReplicationExceptionEventArgs e)
        {
            this.slaveThread = new System.Threading.Thread(this.InitializeSlave);
            this.slaveThread.IsBackground = true;
            this.slaveThread.Start();
        }

        public void Stop(Adf.Service.ServiceContext serviceContext)
        {
            this.Stop();
        }

        private void Start()
        {
            lock (this)
            {
                if (this.running == true)
                {
                    this.Stop();
                }

                Program.ClosedQueue = new QueueTask<long>(this.SessionClosed);

                Program.NumberManagers = new NumberManager[Program.HASH_POOL_SIZE];
                for (int i = 0; i < Program.HASH_POOL_SIZE; i++)
                {
                    Program.NumberManagers[i] = new NumberManager();
                }

                //
                this.InitializeData(Program.ServiceContext);
                this.InitializeJournal(Program.ServiceContext);

                //load data
                this.loadThread = new System.Threading.Thread(this.LoadAction);
                this.loadThread.IsBackground = true;
                this.loadThread.Start();

                //
                this.running = true;
            }
        }

        private void StartSlave()
        {
            lock (this)
            {
                if (this.running == true)
                {
                    this.Stop();
                }

                this.InitializeData(Program.ServiceContext);
                this.InitializeJournal(Program.ServiceContext);

                //
                this.slaveThread = new System.Threading.Thread(this.InitializeSlave);
                this.slaveThread.IsBackground = true;
                this.slaveThread.Start();

                //
                this.running = true;
            }
        }

        private void Stop()
        {
            lock (this)
            {
                try
                {
                    if (this.loadThread != null)
                    {
                        this.loadThread.Abort();
                    }
                }
                catch (System.Threading.ThreadStateException)
                {
                    //
                }

                try
                {
                    if (this.slaveThread != null)
                    {
                        this.slaveThread.Abort();
                    }
                }
                catch (System.Threading.ThreadStateException)
                {
                    //
                }

                if (this.replicationClient != null)
                {
                    this.replicationClient.Dispose();
                }

                if (Program.Listen != null)
                {
                    Program.Listen.Dispose();
                }

                if (Program.Journal != null)
                {
                    Program.Journal.Dispose();
                }

                if (Program.ClosedQueue != null)
                {
                    Program.ClosedQueue.WaitCompleted();
                }

                if (Program.ClosedQueue != null)
                {
                    Program.ClosedQueue.Dispose();
                }

                if (Program.DataManager != null)
                {
                    Program.DataManager.Dispose();
                }

                if (Program.NumberManagers != null)
                {
                    for (int i = 0; i < Program.HASH_POOL_SIZE; i++)
                    {
                        Program.NumberManagers[i] = null;
                    }
                }

                this.running = false;
            }
        }

        private void SessionClosed(long id)
        {
            Program.Listen.CloseSession(id);
        }
    }
}