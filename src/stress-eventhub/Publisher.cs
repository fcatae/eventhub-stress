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
        Task InitAsync();
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

        public Task InitAsync()
        {
            return _client.SendAsync(new EventData(Encoding.UTF8.GetBytes("Init")));
        }

        public Task SendAsync(string message, int loopCount, int batchSize)
        {
            byte[] messageBody = Encoding.UTF8.GetBytes(message);

            EventData[] eventList = new EventData[batchSize];

            for (int i=0; i<batchSize; i++)
            {
                eventList[i] = new EventData(messageBody);
            }

            Task[] taskList = new Task[loopCount];

            for (int i=0; i< loopCount; i++)
            {
                Program.TotalMessages += eventList.Length;

                taskList[i] = _client.SendBatchAsync(eventList);
            }

            return Task.WhenAll(taskList);
        } 
    }
}
