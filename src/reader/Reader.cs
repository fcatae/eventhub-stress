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

        public void Read<TEventProcess>() 
            where TEventProcess : IEventProcess, new()
        {
            int total = this._partitions.Length;

            foreach (var partition in this._partitions)
            {
                var ev = new TEventProcess();
                var task = partition.ProcessLoop(ev).ConfigureAwait(false);
            }
        }

        async Task<string> GetLastEnqueuedOffsetAsync(string partition_id)
        {
            var partitionInfo = await this._client.GetPartitionRuntimeInformationAsync(partition_id);

            return partitionInfo.LastEnqueuedOffset;            
        }

        Task<EventHubReceiver> CreateEventHubReceiverAsync(string partition_id)
        {
            return this._client.GetDefaultConsumerGroup().CreateReceiverAsync(partition_id);
        }

        Task<EventHubReceiver> CreateEventHubReceiverAsync(string partition_id, string offset)
        {
            return this._client.GetDefaultConsumerGroup().CreateReceiverAsync(partition_id, offset, offsetInclusive: false);
        }

        Task<EventHubReceiver> CreateEventHubReceiverAsync(string partition_id, DateTime startTime)
        {
            return this._client.GetDefaultConsumerGroup().CreateReceiverAsync(partition_id, startTime);
        }

        async Task<ReaderPartition> CreateReaderPartitionAsync(string partition_id)
        {
            string offset = await GetLastEnqueuedOffsetAsync(partition_id);
            
            var receiver = await CreateEventHubReceiverAsync(partition_id, offset);

            // var receiver = await CreateEventHubReceiverAsync(partition_id);

            return new ReaderPartition(receiver);
        }

    }
}
