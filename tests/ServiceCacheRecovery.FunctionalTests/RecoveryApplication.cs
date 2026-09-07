using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Npgsql;
using ServiceCacheRecovery.Api.Cache;
using ServiceCacheRecovery.Api.Configuration;
using ServiceCacheRecovery.Api.Kafka;
using ServiceCacheRecovery.Api.Products;

namespace ServiceCacheRecovery.FunctionalTests;

public sealed class RecoveryApplication : WebApplicationFactory<Program>
{
    private readonly string _bootstrapServers;
    private readonly string _connectionString;
    private readonly string _topic;
    private readonly string _projectorGroup;
    private readonly RecoveryStrategy _strategy;

    public Func<CancellationToken, Task>? AfterMarkersPersisted { get; set; }
    public Func<CancellationToken, Task>? AfterRecoveryPlanCreated { get; set; }

    private RecoveryApplication(
        string bootstrapServers,
        string connectionString,
        string topic,
        string projectorGroup,
        RecoveryStrategy strategy)
    {
        _bootstrapServers = bootstrapServers;
        _connectionString = connectionString;
        _topic = topic;
        _projectorGroup = projectorGroup;
        _strategy = strategy;
    }

    public static async Task<RecoveryApplication> CreateAsync(
        InfrastructureFixture infrastructure,
        RecoveryStrategy strategy)
    {
        var id = Guid.NewGuid().ToString("N");
        var topic = $"products-{id}";
        var schema = $"test_{id}";

        await using (var connection = new NpgsqlConnection(infrastructure.PostgresConnectionString))
        {
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand($"CREATE SCHEMA {schema};", connection);
            await command.ExecuteNonQueryAsync();
        }

        using (var admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = infrastructure.KafkaBootstrapServers
        }).Build())
        {
            await admin.CreateTopicsAsync([
                new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = 3,
                    ReplicationFactor = 1,
                    Configs = new Dictionary<string, string>
                    {
                        ["cleanup.policy"] = "compact",
                        ["delete.retention.ms"] = "86400000"
                    }
                }
            ]);
        }

        var connectionString = new NpgsqlConnectionStringBuilder(infrastructure.PostgresConnectionString)
        {
            SearchPath = schema
        }.ConnectionString;

        return new RecoveryApplication(
            infrastructure.KafkaBootstrapServers,
            connectionString,
            topic,
            $"projector-{id}",
            strategy);
    }

    public async Task UpsertAsync(Product product)
    {
        using var producer = CreateProducer();
        await producer.ProduceAsync(_topic, new Message<string, byte[]>
        {
            Key = ProductRecord.ProductKey(product.Id),
            Value = ProductRecord.Serialize(product)
        });
        producer.Flush(TimeSpan.FromSeconds(10));
    }

    public async Task UpsertAsync(Product product, int partition)
    {
        using var producer = CreateProducer();
        await producer.ProduceAsync(
            new TopicPartition(_topic, new Partition(partition)),
            new Message<string, byte[]>
            {
                Key = ProductRecord.ProductKey(product.Id),
                Value = ProductRecord.Serialize(product)
            });
        producer.Flush(TimeSpan.FromSeconds(10));
    }

    public async Task DeleteAsync(Guid productId)
    {
        using var producer = CreateProducer();
        await producer.ProduceAsync(_topic, new Message<string, byte[]>
        {
            Key = ProductRecord.ProductKey(productId),
            Value = null!
        });
        producer.Flush(TimeSpan.FromSeconds(10));
    }

    public RecoveryApplication CreateReplica(RecoveryStrategy strategy) =>
        new(
            _bootstrapServers,
            _connectionString,
            _topic,
            _projectorGroup,
            strategy);

    public async Task WaitForProjectionAsync(Product product, CancellationToken cancellationToken)
    {
        while (true)
        {
            await using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);
            await using var command = new NpgsqlCommand(
                "SELECT name, price, is_deleted FROM products WHERE product_id = $1;",
                connection);
            command.Parameters.AddWithValue(product.Id);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken) &&
                !reader.GetBoolean(2) &&
                reader.GetString(0) == product.Name &&
                reader.GetDecimal(1) == product.Price)
            {
                return;
            }

            await Task.Delay(25, cancellationToken);
        }
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseSetting("Kafka:BootstrapServers", _bootstrapServers);
        builder.UseSetting("Kafka:ProductsTopic", _topic);
        builder.UseSetting("Kafka:ProjectorGroupId", _projectorGroup);
        builder.UseSetting("Kafka:PartitionCount", "3");
        builder.UseSetting("Database:ConnectionString", _connectionString);
        builder.UseSetting("CacheRecovery:Strategy", _strategy.ToString());
        builder.UseSetting("CacheRecovery:MarkerTimeoutSeconds", "30");

        if (AfterMarkersPersisted is not null || AfterRecoveryPlanCreated is not null)
        {
            builder.ConfigureTestServices(services =>
            {
                services.RemoveAll<IRecoveryObserver>();
                services.AddSingleton<IRecoveryObserver>(new CallbackRecoveryObserver(
                    AfterMarkersPersisted,
                    AfterRecoveryPlanCreated));
            });
        }
    }

    private IProducer<string, byte[]> CreateProducer() =>
        new ProducerBuilder<string, byte[]>(new ProducerConfig
        {
            BootstrapServers = _bootstrapServers,
            Acks = Acks.All
        }).Build();

    private sealed class CallbackRecoveryObserver(
        Func<CancellationToken, Task>? afterMarkersPersisted,
        Func<CancellationToken, Task>? afterRecoveryPlanCreated) : IRecoveryObserver
    {
        public Task AfterMarkersPersistedAsync(CancellationToken cancellationToken) =>
            afterMarkersPersisted?.Invoke(cancellationToken) ?? Task.CompletedTask;

        public Task AfterRecoveryPlanCreatedAsync(CancellationToken cancellationToken) =>
            afterRecoveryPlanCreated?.Invoke(cancellationToken) ?? Task.CompletedTask;
    }
}
