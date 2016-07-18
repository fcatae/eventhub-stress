using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace reader
{
    public class ReaderPartition
    {
        EventHubReceiver _receiver;
        int _maxEvents = 100;
        TimeSpan _waitTime = TimeSpan.FromDays(1); //TimeSpan.FromMilliseconds(1000);

        public ReaderPartition(EventHubReceiver receiver)
        {
            this._receiver = receiver;
        }

        public Task<IEnumerable<EventData>> ReadAsync()
        {
            var data = this._receiver.Receive();

            return this._receiver.ReceiveAsync( this._maxEvents , this._waitTime );
        }

        public void Close()
        {
            if (this._receiver != null)
            {
                _receiver.Close();
                _receiver = null;
            }
        }
    }
}
