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

        public Task SendAsync()
        {
            int batchSize = 10;
            byte[] messageBody = Encoding.UTF8.GetBytes("Send async message");

            EventData[] eventList = new EventData[batchSize];

            for (int i=0; i<batchSize; i++)
            {
                eventList[i] = new EventData(messageBody);
            }

            return _client.SendBatchAsync(eventList);
        } 
    }
}
