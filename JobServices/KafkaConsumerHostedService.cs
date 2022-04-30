using System.Text;
using Kafka.Public;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Hosting;
using Kafka.Public.Loggers;
using KafkaNet;
using KafkaNet.Model;
using System.Text.Json;
using KafkaApp.Model;

namespace KafkaApp.JobServices
{
    public class KafkaConsumerHostedService : IHostedService
    {
        private readonly ILogger<KafkaConsumerHostedService> _logger;
        private readonly ClusterClient _cluster;

        public KafkaConsumerHostedService(ILogger<KafkaConsumerHostedService> logger)
        {
            _logger = logger;
            _cluster = new ClusterClient(new Configuration
            {
                Seeds = "localhost:9092"
            }, new ConsoleLogger());
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _cluster.ConsumeFromLatest("testLogs");
            _cluster.MessageReceived += record =>
            {
                _logger.LogInformation($"Received: {Encoding.UTF8.GetString(record.Value as byte[])}");
            };

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _cluster?.Dispose();
            return Task.CompletedTask;
        }
    }

    public class KafkaConsumerV2HostedService : IHostedService
    {
        private readonly ILogger<KafkaConsumerV2HostedService> _logger;
        private Uri uri = new Uri("http://localhost:9092");
        private string topic = "testLogs";
        private Consumer _consumer;
        public KafkaConsumerV2HostedService(ILogger<KafkaConsumerV2HostedService> logger)
        {
            _logger = logger;
            var options = new KafkaOptions(uri);
            var brokerRouter = new BrokerRouter(options);
            _consumer = new Consumer(new ConsumerOptions(topic, brokerRouter)/* ,new KafkaNet.Protocol.OffsetPosition(0, 115) */);
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            foreach (var msg in _consumer.Consume())
            {
                var jsonData = Encoding.UTF8.GetString(msg.Value);
                var customer = JsonSerializer.Deserialize<CustomerModel>(jsonData);
                _logger.LogInformation($"Received offset: {msg.Meta.Offset} - msg: {customer.name}");
            }

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _consumer?.Dispose();
            return Task.CompletedTask;
        }
    }
}