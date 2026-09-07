using Confluent.Kafka;
using Microsoft.Extensions.Options;
using ServiceCacheRecovery.Api.Configuration;

namespace ServiceCacheRecovery.Api.Persistence;

public sealed class DatabaseProjectorService(
    IOptions<KafkaOptions> kafkaOptions,
    ProductProjectionRepository repository,
    ILogger<DatabaseProjectorService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = kafkaOptions.Value;
        using var consumer = new ConsumerBuilder<string, byte[]>(
            new ConsumerConfig 
            { 
                BootstrapServers = options.BootstrapServers, 
                GroupId = options.ProjectorGroupId, 
                AutoOffsetReset = AutoOffsetReset.Earliest, 
            })
            .Build();

        consumer.Subscribe(options.ProductsTopic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var record = consumer.Consume(stoppingToken);
                await repository.ProjectAsync(record, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            logger.LogInformation("Database projector is stopping.");
        }
        finally
        {
            consumer.Close();
        }
    }
}
