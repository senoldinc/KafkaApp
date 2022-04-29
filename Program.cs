
using KafkaApp.JobServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace KafkaApp
{
    class Program
    {
        static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        private static IHostBuilder CreateHostBuilder(string[] args) =>
                Host.CreateDefaultBuilder(args)
                    .ConfigureServices((context, collection) =>
                    {
                        collection.AddHostedService<KafkaProducerV2HostedService>();
                        collection.AddHostedService<KafkaConsumerV2HostedService>();
                    });
    }
}
