using Amazon.SQS;
using Amazon.SQS.Model;
using Newtonsoft.Json;
using System;
using System.Linq;
using System.Threading;

namespace SqsTest
{
    class Program
    {

        private const string QUEUE_NAME = "test-batch";
        private const int NUM_MESSAGES_TO_SEND = 100;
        private const int MAX_THREADS = 20;

        private static SqsWrapper _sqsWrapper = new SqsWrapper();


        static void Main(string[] args)
        {

            var cancellationToken = new CancellationTokenSource();
            var subscriberTask = _sqsWrapper.Subscribe(QUEUE_NAME, m => ProcessMessage(m), MAX_THREADS, cancellationToken.Token);

            ConsoleKeyInfo consoleKeyInfo = default(ConsoleKeyInfo);
            Console.WriteLine("Waiting for messages... Press 'S' to send a block, 'R' to Reset/Purge queue, ESC to finish...");
            while (consoleKeyInfo.Key != ConsoleKey.Escape)
            {
                if (consoleKeyInfo.KeyChar.ToString().ToUpper() == "S")
                    SendPackages();

                if (consoleKeyInfo.KeyChar.ToString().ToUpper() == "R")
                    ResetQueue();

                consoleKeyInfo = Console.ReadKey();
            }

            Console.WriteLine("Cancelling subscriber...");
            cancellationToken.Cancel();
            
            Thread.Sleep(2500);
            cancellationToken.Dispose();
        }

        static void ResetQueue()
        {
            _sqsWrapper.PurgeAndResetCounters();
        }

        static void SendPackages()
        {
            var sqs = new AmazonSQSClient();
            var queueUrl = sqs.GetQueueUrl(QUEUE_NAME).QueueUrl;

            Enumerable
                .Range(0, NUM_MESSAGES_TO_SEND)
                .Select(i => new PerformanceCalculationSpan { PackageNumber = i + 1, LastDate = DateTime.Today, FromAccount = (i * 10) + 1, ToAccount = (i + 1) * 10 })
                .ToList()
                .ForEach(p =>
                {
                    var jsonBody = JsonConvert.SerializeObject(p);
                    var sendMessageRequest = new SendMessageRequest
                    {
                        QueueUrl = queueUrl,
                        MessageBody = jsonBody
                    };
                    sqs.SendMessage(sendMessageRequest);
//                    Console.WriteLine($"Message {p.PackageNumber} sent '{jsonBody}'...");
                });
        }

        static void ProcessMessage(Message message)
        {
            var sleep = new Random().Next(3) + 2;
            var data = JsonConvert.DeserializeObject<PerformanceCalculationSpan>(message.Body);
//            Console.WriteLine($"Processing message nº {data.PackageNumber} for {sleep} seconds...");
            Thread.Sleep(sleep*1000);
            _sqsWrapper.DeleteMessage(message.ReceiptHandle);
        }
    }
}
