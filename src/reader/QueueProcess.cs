using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure.Storage.Queue;

namespace reader
{
    public class QueueProcess
    {
        DateTimeOffset _lastTime;
        int _lastSecond = -1;
        int _statCount = -1;
        string _statMsg = "INIT";

        public void ProcessAsync(IEnumerable<CloudQueueMessage> messages)
        {
            foreach(var ev in messages)
            {
                var time = ev.InsertionTime.Value;

                int seconds = time.Second;

                if (seconds == _lastSecond)
                {
                    // update statistics
                    _statCount++;
                }
                else
                {
                    Console.WriteLine();
                    Console.WriteLine(_statMsg + " : " + _lastTime.ToLocalTime() + " = " + _statCount);

                    // set variables
                    _lastSecond = seconds;
                    _lastTime = time;

                    _statCount = 0;
                    _statMsg = ev.AsString;
                }
            }
        }

        public void ProcessAsync(CloudQueueMessage message)
        {
            var ev = message;

            var time = ev.InsertionTime.Value;

            int seconds = time.Second;

            if (seconds == _lastSecond)
            {
                // update statistics
                _statCount++;
            }
            else
            {
                Console.WriteLine(_statMsg + " : " + _lastTime.ToLocalTime() + " = " + _statCount);

                // set variables
                _lastSecond = seconds;
                _lastTime = time;

                _statCount = 0;
                _statMsg = ev.AsString;
            }

        }
    }
}
