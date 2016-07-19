using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.ServiceBus.Messaging;

namespace stress_eventhub
{
    public class Program
    {
        public static int TotalMessages = 0;

        static string[] g_AllMessages = { "hello_world", "json_rules", "compact_binary" };

        public static void Main(string[] args)
        {
            Stopwatch watch = new Stopwatch();
            string message = g_AllMessages[0];
            int batchSize = 1;
            int asyncCount = 30;
            int loopCount = 10;
            int numberPublishers = 1;

            Console.WriteLine("SENDER: Stress Test");

            var builder = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .AddJsonFile("app.json", optional: true)
                .AddUserSecrets("aspnet-stress-eventhub-20160705064558")
                .AddCommandLine(args);

            var config = builder.Build();
            
            string proto = config["proto"] ?? "Amqp";
            string connectionString = config["eventhub-ns"];

            if(proto != null)
            {
                connectionString += $";TransportType={proto}";
            }

            if (config["batch"] != null)
            {
                batchSize = Int32.Parse(config["batch"]);
            }
            if (config["async"] != null)
            {
                asyncCount = Int32.Parse(config["async"]);
            }
            if (config["loop"] != null)
            {
                loopCount = Int32.Parse(config["loop"]);
            }

            if (config["pub"] != null)
            {
                numberPublishers = Int32.Parse(config["pub"]);
            }
            
            string eventhubPath = config["eventhub-path"];

            IPublisher pub = CreateEventHub(connectionString, eventhubPath);

            Console.WriteLine($"Using protocol: {proto}");

            Console.Write("Initializing... ");

            pub.InitAsync("Init").Wait();

            Console.WriteLine("DONE");

            watch.Start();

            Console.WriteLine("Message body:");
            Console.WriteLine(message);
            Console.WriteLine();
            Console.WriteLine($"Sending messages ({message})");
            Console.WriteLine($"  - loopCount = {loopCount}");
            Console.WriteLine($"  - asyncCount = {asyncCount}");
            Console.WriteLine($"  - batchSize = {batchSize}");
            Console.WriteLine();

            SendLoopAsync(pub, message, loopCount, asyncCount, batchSize).Wait();

            watch.Stop();

            Console.WriteLine();
            Console.WriteLine("  Total time = {0} ms", watch.ElapsedMilliseconds);
            Console.WriteLine("  Total messages= {0} msgs", Program.TotalMessages);
            Console.WriteLine();
            Console.WriteLine("  Througput= {0} msg/sec", 1000 * Program.TotalMessages / watch.ElapsedMilliseconds);

            Console.ReadLine();
        }

        static IPublisher CreateEventHub(string connectionString, string eventhubPath)
        {
            return (IPublisher)new EventHubPublisher(connectionString, eventhubPath, newConnection: false);
        }

        static async Task SendLoopAsync(IPublisher pub, string message, int loopCount, int asyncCount, int batchSize)
        {
            Console.Write("Sending message... ");
            
            int total = 0;
            int limit = 10;

            for (int i=0; i<loopCount; i++)
            {
                await SendAsync(pub, message, asyncCount, batchSize);

                if( i == 0 )
                {
                    Console.WriteLine("STARTED");
                }

                total += batchSize;

                while(total > limit)
                {
                    Console.Write(".");
                    total -= limit;
                }
            }

            Console.WriteLine();
            Console.WriteLine("DONE");
        }

        static Task SendAsync(IPublisher pub, string message, int asyncCount, int batchSize)
        {
            byte[] messageBody = Encoding.UTF8.GetBytes(message);

            Task[] taskList = new Task[asyncCount];

            for (int i = 0; i < asyncCount; i++)
            {
                Program.TotalMessages += batchSize;
                taskList[i] = pub.SendAsync(message, batchSize);
            }

            return Task.WhenAll(taskList);
        }
    }
}
