﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

namespace reader
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Reader");

            var builder = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .AddJsonFile("app.json", optional: true)
                .AddUserSecrets("aspnet-stress-eventhub-20160705064558")
                .AddCommandLine(args);

            var config = builder.Build();

            string pubtype = config["type"] ?? "eventhub";

            if (pubtype == "azurequeue")
            {
                string azureQueueNS = config["azurequeue-ns"];
                string azureQueueName = config["azurequeue-path"];

                Console.WriteLine("Reading Azure Queues... ");

                var queueReader = new QueueReader(azureQueueNS, azureQueueName);
                queueReader.Init();
                queueReader.ReadAsync();
            }
            else
            {
                string eventHubNS = config["eventhub-ns"];
                string eventHubName = config["eventhub-path"];

                var reader = new Reader(eventHubNS, eventHubName);

                Console.Write("Initializing... ");
                reader.Init();
                Console.WriteLine("DONE");

                Console.Write("Setup Readers... ");
                reader.Read<EventProcess>();
                Console.WriteLine("DONE");

                Console.WriteLine("Waiting...");
            }

            Console.ReadLine();
        }
    }
}
