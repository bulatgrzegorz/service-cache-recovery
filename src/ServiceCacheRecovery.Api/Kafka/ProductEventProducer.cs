using Confluent.Kafka;
using Microsoft.Extensions.Options;
using ServiceCacheRecovery.Api.Configuration;
using ServiceCacheRecovery.Api.Products;

namespace ServiceCacheRecovery.Api.Kafka;

public sealed class ProductEventProducer : IDisposable
{
    private readonly KafkaOptions _options;
    private readonly IProducer<string, byte[]> _producer;

    public ProductEventProducer(IOptions<KafkaOptions> options)
    {
        _options = options.Value;
        _producer = new ProducerBuilder<string, byte[]>(new ProducerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true
        }).Build();
    }

    public Task<DeliveryResult<string, byte[]>> UpsertAsync(Product product, CancellationToken cancellationToken) =>
        _producer.ProduceAsync(
            _options.ProductsTopic,
            new Message<string, byte[]>
            {
                Key = ProductRecord.ProductKey(product.Id),
                Value = ProductRecord.Serialize(product)
            },
            cancellationToken);

    public Task<DeliveryResult<string, byte[]>> DeleteAsync(Guid productId, CancellationToken cancellationToken) =>
        _producer.ProduceAsync(
            _options.ProductsTopic,
            new Message<string, byte[]>
            {
                Key = ProductRecord.ProductKey(productId),
                Value = null!
            },
            cancellationToken);

    public Task<DeliveryResult<string, byte[]>> PublishMarkerAsync(
        RecoveryMarker marker,
        CancellationToken cancellationToken) =>
        _producer.ProduceAsync(
            new TopicPartition(_options.ProductsTopic, new Partition(marker.Partition)),
            new Message<string, byte[]>
            {
                Key = ProductRecord.MarkerKey(marker.RecoveryId, marker.Partition),
                Value = ProductRecord.Serialize(marker)
            },
            cancellationToken);

    public Task<DeliveryResult<string, byte[]>> DeleteMarkerAsync(
        RecoveryMarker marker,
        CancellationToken cancellationToken) =>
        _producer.ProduceAsync(
            new TopicPartition(_options.ProductsTopic, new Partition(marker.Partition)),
            new Message<string, byte[]>
            {
                Key = ProductRecord.MarkerKey(marker.RecoveryId, marker.Partition),
                Value = null!
            },
            cancellationToken);

    public void Dispose() => _producer.Dispose();
}
