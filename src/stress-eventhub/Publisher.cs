using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace stress_eventhub
{
    public interface IPublisher
    {
        Task InitAsync(string message);
        Task SendAsync(string message, int loopCount, int batchSize);
    }

    public class Publisher : IPublisher
    {
        EventHubClient _client;
        
        public Publisher(string connStr, string path) : this(connStr, path, true)
        {
        }

        public Publisher(string connStr, string path, bool newConnection)
        {
            if(newConnection)
            {
                var factory = MessagingFactory.CreateFromConnectionString(connStr);
                _client = factory.CreateEventHubClient(path);
            }
            else
            {
                _client = EventHubClient.CreateFromConnectionString(connStr, path);
            }

            Console.WriteLine("Creating Publisher...");
        }

        public Task InitAsync(string message)
        {
            return _client.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
        }

        public Task SendAsync(string message, int asyncCount, int batchSize)
        {
            byte[] messageBody = Encoding.UTF8.GetBytes(message);

            EventData[] eventList = new EventData[batchSize];

            Task[] taskList = new Task[asyncCount];

            for (int i=0; i< asyncCount; i++)
            {
                Program.TotalMessages += eventList.Length;

                for (int j = 0; j < batchSize; j++)
                {
                    eventList[j] = new EventData(messageBody);
                }

                taskList[i] = (batchSize == 1) ?
                    _client.SendAsync(eventList[0]) : 
                    _client.SendBatchAsync(eventList);
            }

            return Task.WhenAll(taskList);
        } 
    }
}
