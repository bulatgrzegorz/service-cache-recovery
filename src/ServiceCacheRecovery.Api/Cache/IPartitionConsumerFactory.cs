using Confluent.Kafka;
using Microsoft.Extensions.Options;
using ServiceCacheRecovery.Api.Configuration;

namespace ServiceCacheRecovery.Api.Cache;

public interface IPartitionConsumerFactory
{
    IConsumer<string, byte[]> Create();
}

public sealed class PartitionConsumerFactory(IOptions<KafkaOptions> kafkaOptions) : IPartitionConsumerFactory
{
    public IConsumer<string, byte[]> Create() =>
        new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = kafkaOptions.Value.BootstrapServers,
            GroupId = $"product-cache-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        }).Build();
}