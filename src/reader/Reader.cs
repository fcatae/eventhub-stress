using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace reader
{
    public class Reader
    {
        EventHubClient _client;
        ReaderPartition[] _partitions;

        public Reader(string connectionString, string path)
        {
            this._client = EventHubClient.CreateFromConnectionString(connectionString, path);
        }

        public void Init()
        {
            var partitions = _client.GetRuntimeInformation().PartitionIds;
            int total = partitions.Length;

            // Async: Initialize a reader per partition (ReaderPartition)
            Task<ReaderPartition>[] tasks = new Task<ReaderPartition>[total];
            for (int i=0; i< total; i++)
            {
                tasks[i] = CreateReaderPartitionAsync(partitions[i]);
            }

            // Wait for the initialization
            Task.WaitAll(tasks);

            // Store the results in the partition array
            ReaderPartition[] readers = new ReaderPartition[total];
            for (int i = 0; i < total; i++)
            {
                readers[i] = tasks[i].Result;
            }

            this._partitions = readers;
        }

        public void Read()
        {
            int total = this._partitions.Length;

            foreach(var partition in this._partitions)
            {
                partition.ReadAsync().ContinueWith( task => {

                    var events = task.Result;

                    foreach(var ev in events)
                    {
                        var seq = ev.SequenceNumber;
                        var time = ev.EnqueuedTimeUtc; 
                        var buffer = ev.GetBytes();
                        string msg = new string(Encoding.UTF8.GetChars(buffer));
                        Console.WriteLine(msg + ": "+ seq.ToString() + " : " + time.ToLongTimeString());
                    }

                });
            }
        }

        async Task<string> GetLastEnqueuedOffsetAsync(string partition_id)
        {
            var partitionInfo = await this._client.GetPartitionRuntimeInformationAsync(partition_id);

            return partitionInfo.LastEnqueuedOffset;            
        }

        Task<EventHubReceiver> CreateEventHubReceiverAsync(string partition_id, string offset)
        {
            return this._client.GetDefaultConsumerGroup().CreateReceiverAsync(partition_id, offset, offsetInclusive: false);
        }

        async Task<ReaderPartition> CreateReaderPartitionAsync(string partition_id)
        {
            string offset = await GetLastEnqueuedOffsetAsync(partition_id);
            //var receiver = await CreateEventHubReceiverAsync(partition_id, offset);

            var receiver = await CreateEventHubReceiverAsync(partition_id, "0");

            return new ReaderPartition(receiver);
        }

    }
}
