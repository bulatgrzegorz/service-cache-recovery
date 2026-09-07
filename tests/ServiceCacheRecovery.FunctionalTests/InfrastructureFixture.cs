using Testcontainers.Kafka;
using Testcontainers.PostgreSql;
using TUnit.Core.Interfaces;

namespace ServiceCacheRecovery.FunctionalTests;

public sealed class InfrastructureFixture : IAsyncInitializer, IAsyncDisposable
{
    private readonly KafkaContainer _kafka = new KafkaBuilder("apache/kafka-native:3.9.1").Build();
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:17-alpine").Build();

    public string KafkaBootstrapServers => _kafka.GetBootstrapAddress();
    public string PostgresConnectionString => _postgres.GetConnectionString();

    public Task InitializeAsync() => Task.WhenAll(_kafka.StartAsync(), _postgres.StartAsync());

    public async ValueTask DisposeAsync()
    {
        await _kafka.DisposeAsync();
        await _postgres.DisposeAsync();
    }
}
