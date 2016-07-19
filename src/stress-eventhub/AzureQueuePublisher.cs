using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace stress_eventhub
{
    public class AzureQueuePublisher : IPublisher
    {
        private readonly string _connectionString;
        private readonly CloudQueue _queue;
        private readonly string _queueName;

        public AzureQueuePublisher(string connectionString, string queueName)
        {
            this._connectionString = connectionString;
            this._queueName = queueName;

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);

            // Recommendation: Disable nagling
            // See: https://blogs.msdn.microsoft.com/windowsazurestorage/2010/06/25/nagles-algorithm-is-not-friendly-towards-small-requests/
            //
            ServicePoint queueServicePoint = ServicePointManager.FindServicePoint(storageAccount.QueueEndpoint);
            queueServicePoint.UseNagleAlgorithm = false;
            
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();

            CloudQueue queue = queueClient.GetQueueReference(queueName);

            this._queue = queue;
        }

        public Task InitAsync(string message)
        {
            CloudQueueMessage queueMessage = new CloudQueueMessage(message);

            return _queue.AddMessageAsync(queueMessage);
        }

        public async Task SendAsync(string message, int batchSize)
        {
            CloudQueueMessage queueMessage = new CloudQueueMessage(message);

            // There is no concept of batch size
            // Send the message sequentially
            for(int i=0; i<batchSize; i++)
            {
                await _queue.AddMessageAsync(queueMessage);
            }            
        }
    }
}
