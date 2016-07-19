using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace reader
{
    public class QueueReader
    {
        private readonly string _connectionString;
        private readonly CloudQueue _queue;
        private readonly string _queueName;

        public QueueReader(string connectionString, string queueName)
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

        public void Init()
        {
        }

        public async void Read()
        {
            var queueProcessor = new QueueProcess();

            while(true)
            {                
                var retrievedMessage = _queue.GetMessages(10);

                if( retrievedMessage == null )
                {
                    await Task.Delay(5);
                    continue;
                }

                queueProcessor.ProcessAsync(retrievedMessage);

                foreach(var m in retrievedMessage)
                {
                    _queue.DeleteMessageAsync(m);
                }
                
            }

        }
    }
}
