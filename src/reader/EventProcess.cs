using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace reader
{
    public interface IEventProcess
    {
        void ProcessAsync(IEnumerable<EventData> events);
    }

    public class EventProcess : IEventProcess
    {
        DateTime _lastTime;
        int _lastSecond = -1;
        int _statCount = -1;
        string _statMsg = "INIT";

        public void ProcessAsync(IEnumerable<EventData> events)
        {
            foreach (var ev in events)
            {
                var time = ev.EnqueuedTimeUtc;

                int seconds = time.Second;

                if( seconds == _lastSecond )
                {
                    // update statistics
                    _statCount++;
                }
                else
                {
                    Console.WriteLine(_statMsg + " : " + _lastTime.ToLongTimeString() + " = " + _statCount);

                    // set variables
                    _lastSecond = seconds;
                    _lastTime = time;

                    var buffer = ev.GetBytes();

                    _statCount = 0;
                    _statMsg = new string(Encoding.UTF8.GetChars(buffer));
                }
            }
        }
    }
}
