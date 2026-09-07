using Confluent.Kafka;

namespace ServiceCacheRecovery.Api.Kafka;

public static class KafkaTopology
{
    public static IReadOnlyList<TopicPartition> GetPartitions(
        string bootstrapServers,
        string topic)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = bootstrapServers
        }).Build();
        
        var metadata = admin.GetMetadata(topic, TimeSpan.FromSeconds(10));
        var topicMetadata = metadata.Topics.SingleOrDefault(candidate => candidate.Topic == topic)
            ?? throw new InvalidOperationException($"Kafka returned no metadata for topic {topic}.");

        if (topicMetadata.Error.Code != ErrorCode.NoError)
        {
            throw new KafkaException(topicMetadata.Error);
        }

        return topicMetadata.Partitions
            .OrderBy(partition => partition.PartitionId)
            .Select(partition => new TopicPartition(topic, new Partition(partition.PartitionId)))
            .ToArray();
    }

    public static IReadOnlyDictionary<int, WatermarkOffsets> GetWatermarks(
        IConsumer<string, byte[]> consumer,
        IEnumerable<TopicPartition> partitions) =>
        partitions.ToDictionary(
            partition => partition.Partition.Value,
            partition => consumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(10)));
}
