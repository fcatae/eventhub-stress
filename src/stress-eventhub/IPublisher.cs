using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace stress_eventhub
{
    public interface IPublisher
    {
        Task InitAsync(string message);
        Task SendAsync(string message, int batchSize);
    }
}
