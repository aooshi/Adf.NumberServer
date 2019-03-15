using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;

namespace NumberServerPerformanceTest
{
    class Program
    {
        static void Main(string[] args)
        {
            string result = "";
            //try
            //{
            //    result = Adf.NumberServerClient.Instance.Delete("/test/performance");
            //    Console.WriteLine("delete /test/performance:" + result);
            //}
            //catch(Exception exception)
            //{
            //    Console.WriteLine("delete /test/performance:" + exception.Message);
            //}

            //result = Adf.NumberServerClient.Instance.New("/test/performance", ulong.MaxValue, Adf.NumberServerAlgorithmType.Increment, 1, 1, false);
            //Console.WriteLine("add /test/1 result: " + result);

            //Int64 id = 0;

            //const int count = 100000;

            //while (true)
            //{
            //    Console.WriteLine("GetInt64 " + count + " begin");

            //    var stopwatch = Stopwatch.StartNew();
            //    for (int i = 0; i < count; i++)
            //    {
            //        id = Adf.NumberServerClient.Instance.GetInt64("/test/performance");
            //    }
            //    stopwatch.Stop();


            //    Console.WriteLine("GetInt64 " + count + " completed,Elapsed");
            //    Console.WriteLine("--Elapsed {0}ms", stopwatch.ElapsedMilliseconds);
            //    Console.WriteLine("--{0}/s", count / stopwatch.Elapsed.TotalSeconds);
            //}
        }
    }
}
