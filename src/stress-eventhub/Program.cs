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
            string pubtype = "eventhub";
            int batchSize = 1;
            int asyncCount = 30;
            int loopCount = 10;

            Console.WriteLine("SENDER: Stress Test");
            Console.WriteLine();

            var builder = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .AddJsonFile("app.json", optional: true)
                .AddUserSecrets("aspnet-stress-eventhub-20160705064558")
                .AddCommandLine(args);

            var config = builder.Build();
            
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
            if (config["type"] != null)
            {
                pubtype = config["type"];
            }

            Console.Write($"Creating Publisher [{pubtype}]... ");

            IPublisher pub = null;

            if (pubtype == "azurequeue")
            {
                string azureQueueNamespace = config["azurequeue-ns"];
                string azureQueuePath = config["azurequeue-path"];

                pub = CreateAzureQueue(azureQueueNamespace, azureQueuePath);
            }

            if (pubtype == "eventhub")
            {
                string proto = config["proto"] ?? "Amqp";
                string eventHubNamespace = config["eventhub-ns"];

                if (proto != null)
                {
                    eventHubNamespace += $";TransportType={proto}";
                    Console.WriteLine($"Using protocol: {proto}");
                }
                string eventhubPath = config["eventhub-path"];

                pub = CreateEventHub(eventHubNamespace, eventhubPath);
            }

            if (pub == null)
                throw new Exception("Invalid pub");

            Console.WriteLine("DONE");
            Console.WriteLine();

            Console.Write("Initializing... ");

            pub.InitAsync("Init").Wait();

            Console.WriteLine("DONE");
            Console.WriteLine();

            Console.Write("Opening messages... ");

            var loader = new MessageLoader();
            loader.Load(".");

            Console.Write($"FOUND {loader.MessageCount} messages");

            message = loader.GetRandom();

            Console.WriteLine("DONE");
            Console.WriteLine();

            watch.Start();

            Console.WriteLine("Message body:");
            Console.WriteLine("+++++++++++++++++++++++++++++++");
            Console.WriteLine(message);
            Console.WriteLine("+++++++++++++++++++++++++++++++");
            Console.WriteLine();
            Console.WriteLine($"Sending message settings:");
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
            return new EventHubPublisher(connectionString, eventhubPath, newConnection: false);
        }

        static IPublisher CreateAzureQueue(string connectionString, string queuePath)
        {
            return new AzureQueuePublisher(connectionString, queuePath);
        }

        static async Task SendLoopAsync(IPublisher pub, string message, int loopCount, int asyncCount, int batchSize)
        {
            Console.WriteLine("Sending message... ");
            
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
