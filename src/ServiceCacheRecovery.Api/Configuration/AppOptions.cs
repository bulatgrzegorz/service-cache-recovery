namespace ServiceCacheRecovery.Api.Configuration;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; init; } = "localhost:9092";

    public string ProductsTopic { get; init; } = "products";

    public string ProjectorGroupId { get; init; } = "product-database-projector";

    public int PartitionCount { get; init; } = 3;
}

public sealed class DatabaseOptions
{
    public const string SectionName = "Database";

    public string ConnectionString { get; init; } =
        "Host=localhost;Port=5432;Database=products;Username=postgres;Password=postgres";
}

public sealed class CacheRecoveryOptions
{
    public const string SectionName = "CacheRecovery";

    public RecoveryStrategy Strategy { get; init; } = RecoveryStrategy.KafkaReplay;

    public int MarkerPollMilliseconds { get; init; } = 50;

    public int MarkerTimeoutSeconds { get; init; } = 30;
}

public enum RecoveryStrategy
{
    KafkaReplay,
    DatabaseSnapshot
}
