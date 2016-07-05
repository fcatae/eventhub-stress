using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace stress_eventhub
{
    public class MultiPublisher : IPublisher
    {
        int _instanceCount;
        Publisher[] _publishers;

        public MultiPublisher(int instances, string connectionString, string path)
        {
            _publishers = new Publisher[instances];
            _instanceCount = instances;

            SetPublisher(all => new Publisher(connectionString, path));
        }

        Task Map(Func<Publisher, Task> func)
        {
            Task[] tasklist = new Task[_instanceCount];

            for (int i = 0; i < _instanceCount; i++)
            {
                tasklist[i] = func(_publishers[i]);
            }

            return Task.WhenAll(tasklist);
        }

        void SetPublisher(Func<int, Publisher> createInstance)
        {
            for (int i = 0; i < _instanceCount; i++)
            {
                _publishers[i] = createInstance(i);
            }
        }

        public Task InitAsync()
        {
            return Map(p => p.InitAsync());
        }

        public Task SendAsync(string message, int loopCount, int batchSize)
        {
            return Map(p => p.SendAsync(message, loopCount, batchSize));
        }
    }
}