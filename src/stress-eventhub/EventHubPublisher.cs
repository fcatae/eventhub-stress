using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace stress_eventhub
{
    public class EventHubPublisher : IPublisher
    {
        EventHubClient _client;
        
        public EventHubPublisher(string connStr, string path) : this(connStr, path, true)
        {
        }

        public EventHubPublisher(string connStr, string path, bool newConnection)
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

        public Task SendAsync(string message, int batchSize)
        {
            byte[] messageBody = Encoding.UTF8.GetBytes(message);

            EventData[] eventList = new EventData[batchSize];

            for (int j = 0; j < batchSize; j++)
            {
                eventList[j] = new EventData(messageBody);
            }
            
            // Return a single message
            if (batchSize == 1)
            {                
                return _client.SendAsync(eventList[0]);
            }

            // Return a batch of messages
            return _client.SendBatchAsync(eventList);
        }
    }
}
