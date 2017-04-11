using Amazon.SQS;
using Amazon.SQS.Model;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SqsTest
{
    public class SqsWrapper
    {

        private AmazonSQSClient _sqs;
        private object lockObj = new object();
        private int _inflightMessages = 0;
        private int _readMessages = 0;
        private int _processedMessages = 0;
        private int _deletedMessages = 0;
        private string _queueUrl;


        public SqsWrapper()
        {
            _sqs = new AmazonSQSClient();
        }

        public void InitQueue(string queueName)
        {}

        private void Status(Message message = default(Message))
        {
            Console.Write($"\rRead: {_readMessages} | Processed: {_processedMessages} | Deleted: {_deletedMessages} | InFlight: {_inflightMessages} | Last processed: {message?.MessageId}");
        }

        public async Task Subscribe(string queueName, Action<Message> onReceiveMessage, int maxInFlightMessages, CancellationToken cancellationToken = default(CancellationToken))
        {
            _queueUrl = await GetQueueUrlAsync(queueName);
            lock (lockObj) { _inflightMessages = 0; }
            var receiveRequest = new ReceiveMessageRequest { QueueUrl = _queueUrl, MaxNumberOfMessages = 10, WaitTimeSeconds = 20 };
            while (!cancellationToken.IsCancellationRequested)
            {
                var response = await _sqs.ReceiveMessageAsync(receiveRequest);
                lock (lockObj) { _readMessages += response?.Messages.Count() ?? 0; }
                Status();
                foreach (var msg in response?.Messages)
                {
                    while (_inflightMessages >= maxInFlightMessages)
                    {
                        Status();
                    }
                    lock (lockObj) { _inflightMessages++; }
                    Task.Run(() =>
                     {
                         Status(msg);
                         onReceiveMessage(msg);
                         lock (lockObj) {
                             _processedMessages++;
                         }
                         Status();
                     });
                }
            }
        }

        public void DeleteMessage(string receiptHandle)
        {
            if (string.IsNullOrEmpty(_queueUrl)) throw new NullReferenceException("Queue not initialized...");
            var result = _sqs.DeleteMessage(_queueUrl, receiptHandle);
            lock (lockObj)
            {
                _deletedMessages++;
                _inflightMessages--;
            }
            if (_inflightMessages < 0) _inflightMessages = 0;
            Status();
        }

        private async Task<string> GetQueueUrlAsync(string queueName)
        {
            var queueInfoResponse = await _sqs.GetQueueUrlAsync(queueName);
            return queueInfoResponse?.QueueUrl;
        }

    }
}
