using System;
using System.Collections.Generic;
using System.Text;

namespace NumberServerTest
{
    class Program
    {
        static void Main(string[] args)
        {
            //var result = "";

            var pool = new Adf.MemcachePool("NumberServer");

            var cacheItems = new List<string>(10000);
            var stopwatche = new System.Diagnostics.Stopwatch();
            var speed = 0d;
            stopwatche.Reset();
            stopwatche.Start();

            for (int i = 0; i < cacheItems.Capacity; i++)
            {
                var key = "k" + i;
                var value = "value" + key;
                cacheItems.Add(key);

                pool.Set(key, value, 10000);
            }

            speed = (double)cacheItems.Capacity / (double)(stopwatche.ElapsedMilliseconds / 1000);
            Console.WriteLine("init {0} | {1:F4}/s completed", cacheItems.Capacity, speed);

            var counter = new Adf.Counter(cacheItems.Capacity);

            stopwatche.Reset();
            stopwatche.Start();

            for (int i = 0; i < cacheItems.Capacity; i++)
            {
                System.Threading.ThreadPool.QueueUserWorkItem(CheckItem, new object[] { i, counter,pool });
            }

            stopwatche.Reset();
            stopwatche.Start();
            if (stopwatche.ElapsedMilliseconds * 1000 < 1)
                speed = 0;
            else
                speed = (double)cacheItems.Capacity / (double)(stopwatche.ElapsedMilliseconds * 1000);
            Console.WriteLine("thread queue {0:F4} completed", speed);

            var handle = new System.Threading.EventWaitHandle(false, System.Threading.EventResetMode.ManualReset);

            var thread1 = new System.Threading.Thread(obj => {

                while (true)
                {
                    System.Threading.Thread.Sleep(1000);
                    if (counter.Value == 0)
                    {
                        handle.Set();
                    }

                    var elapsed = (double)(stopwatche.ElapsedMilliseconds / 1000);
                    var count = counter.Init - counter.Value;
                    speed = (double)(count) / elapsed;

                    Console.WriteLine("total: {0}, run: {1}, speed: {2:F4}/s, elapsed:{3}", cacheItems.Capacity, count, speed, elapsed);
                }
            });
            thread1.IsBackground = true;
            thread1.Start();

            handle.WaitOne();

            Console.WriteLine("NumberServer Completed");
            Console.Read();
        }

        static void CheckItem(object state)
        {
            var arr = (object[])state;
            var i = (int)arr[0];
            var counter = (Adf.Counter)arr[1];
            var pool = (Adf.MemcachePool)arr[2];

            //
            var key = "k" + i;
            var value = "value" + key;

            //
            var v = pool.Get(key);
            if (v == null)
            {
                System.Diagnostics.Debug.WriteLineIf(value == v, "index: " + i + ", value: " + value + ", cache: " + v);
            }

            //
            var deleted = pool.Delete(key);
            System.Diagnostics.Debug.WriteLineIf(deleted == false, "delete failure: " + key);

            //
            counter.Decrement();

        }
    }
}
