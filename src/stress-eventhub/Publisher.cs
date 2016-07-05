using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace stress_eventhub
{
    public class Publisher
    {
        EventHubClient _client;

        public Publisher(string connStr)
        {
            _client = EventHubClient.CreateFromConnectionString(connStr);
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
                taskList[i] = _client.SendBatchAsync(eventList);
            }

            return Task.WhenAll(taskList);
        } 
    }
}
