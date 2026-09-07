using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Options;
using ServiceCacheRecovery.Api.Configuration;
using ServiceCacheRecovery.Api.Persistence;

namespace ServiceCacheRecovery.Api.Infrastructure;

public sealed class InfrastructureInitializer(IOptions<KafkaOptions> kafkaOptions, ProductProjectionRepository repository) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await repository.InitializeAsync(cancellationToken);

        using var admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = kafkaOptions.Value.BootstrapServers
        }).Build();

        try
        {
            await admin.CreateTopicsAsync([
                new TopicSpecification
                {
                    Name = kafkaOptions.Value.ProductsTopic,
                    NumPartitions = kafkaOptions.Value.PartitionCount,
                    ReplicationFactor = 1,
                    Configs = new Dictionary<string, string>
                    {
                        ["cleanup.policy"] = "compact",
                        ["delete.retention.ms"] = "86400000"
                    }
                }
            ]);
        }
        catch (CreateTopicsException exception) when (exception.Results.All(result => result.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // Another replica already created the topic.
        }

        await ValidateTopicAsync(admin, kafkaOptions.Value);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private static async Task ValidateTopicAsync(IAdminClient admin, KafkaOptions options)
    {
        var metadata = admin.GetMetadata(options.ProductsTopic, TimeSpan.FromSeconds(10));
        var topic = metadata.Topics.Single(candidate => candidate.Topic == options.ProductsTopic);
        if (topic.Error.Code != ErrorCode.NoError)
        {
            throw new KafkaException(topic.Error);
        }

        if (topic.Partitions.Count != options.PartitionCount)
        {
            throw new InvalidOperationException(
                $"Topic {options.ProductsTopic} has {topic.Partitions.Count} partitions, " +
                $"but the service expects {options.PartitionCount}. Partition changes break offset versions.");
        }

        var resource = new ConfigResource
        {
            Name = options.ProductsTopic,
            Type = ResourceType.Topic
        };
        var results = await admin.DescribeConfigsAsync([resource]);
        var cleanupPolicy = results.Single().Entries["cleanup.policy"].Value;
        if (!cleanupPolicy.Split(',').Contains("compact", StringComparer.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Topic {options.ProductsTopic} must use the compact cleanup policy.");
        }
    }
}
