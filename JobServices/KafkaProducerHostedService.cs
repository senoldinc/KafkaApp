using Kafka.Public;
using Kafka.Public.Loggers;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using KafkaNet;
using KafkaNet.Model;
using System.Text.Json;
using KafkaApp.Model;

namespace KafkaApp.JobServices
{
    public class KafkaProducerHostedService : IHostedService
    {
        private readonly ILogger<KafkaProducerHostedService> _logger;
        private readonly ClusterClient _cluster;

        public KafkaProducerHostedService(ILogger<KafkaProducerHostedService> logger)
        {
            _logger = logger;

            _cluster = new ClusterClient(new Configuration
            {
                Seeds = "localhost:9092"
            }, new ConsoleLogger());
        }
        public Task StartAsync(CancellationToken cancellationToken)
        {
            for (int i = 0; i < 100; i++)
            {
                var value = $"Hello kafka {i}";
                _logger.LogInformation(value);
                _cluster.Produce("testLogs", value);
            }

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _cluster?.Dispose();
            return Task.CompletedTask;
        }
    }

    public class KafkaProducerV2HostedService : IHostedService
    {
        private readonly ILogger<KafkaProducerV2HostedService> _logger;
        private Uri uri = new Uri("http://localhost:9092");
        private string topic = "testLogs";
        private Producer _producer;
        public KafkaProducerV2HostedService(ILogger<KafkaProducerV2HostedService> logger)
        {
            _logger = logger;
            var options = new KafkaOptions(uri);
            var router = new BrokerRouter(options);
            _producer = new Producer(router);
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            for (int i = 0; i < 10; i++)
            {
                var value = $"My name is {i.ToString("000")}";
                var customer = new CustomerModel 
                {
                    id = Guid.NewGuid(),
                    name = value
                };
                var payload = JsonSerializer.Serialize(customer);
                _logger.LogInformation(payload);

                KafkaNet.Protocol.Message msg = new KafkaNet.Protocol.Message(payload);
                _producer.SendMessageAsync(topic, new List<KafkaNet.Protocol.Message> { msg }).Wait();
            }

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _producer?.Dispose();
            return Task.CompletedTask;
        }
    }
}