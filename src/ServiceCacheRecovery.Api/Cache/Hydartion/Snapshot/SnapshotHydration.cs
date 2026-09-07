using Microsoft.Extensions.Options;
using ServiceCacheRecovery.Api.Configuration;
using ServiceCacheRecovery.Api.Kafka;
using ServiceCacheRecovery.Api.Persistence;

namespace ServiceCacheRecovery.Api.Cache.Hydartion.Snapshot;

public class SnapshotHydration(
    ProductEventProducer producer, 
    ProductProjectionRepository repository,
    ProductCache cache,
    ProductEventProducer recoveryProducer,
    RecoveryWorker recoveryWorker,
    RehydrationStatus rehydrationStatus,
    IRecoveryObserver recoveryObserver,
    IPartitionConsumerFactory consumerFactory,
    IOptions<CacheRecoveryOptions> recoveryOptions, 
    IOptions<KafkaOptions> kafkaOptions) : ICacheHydration
{
    public async Task RehydrateAsync(CancellationToken cancellationToken)
    {
        var partitions = KafkaTopology.GetPartitions(kafkaOptions.Value.BootstrapServers, kafkaOptions.Value.ProductsTopic);
        
        rehydrationStatus.Setup(partitions);
        
        var recoveryId = Guid.NewGuid();
        var markers = partitions
            .Select(partition => new RecoveryMarker(recoveryId, partition.Partition.Value))
            .ToArray();
        
        var markerOffsets = new Dictionary<int, long>();
        foreach (var marker in markers)
        {
            var delivery = await producer.PublishMarkerAsync(marker, cancellationToken);
            markerOffsets[marker.Partition] = delivery.Offset.Value;
        }

        await repository.WaitForMarkersAsync(
            recoveryId,
            markerOffsets,
            TimeSpan.FromMilliseconds(recoveryOptions.Value.MarkerPollMilliseconds),
            TimeSpan.FromSeconds(recoveryOptions.Value.MarkerTimeoutSeconds),
            cancellationToken);

        await recoveryObserver.AfterMarkersPersistedAsync(cancellationToken);

        var snapshot = await repository.LoadSnapshotAsync(cancellationToken);
        cache.LoadSnapshot(snapshot);
        
        foreach (var marker in markers) await recoveryProducer.DeleteMarkerAsync(marker, cancellationToken);

        var consumer = consumerFactory.Create();
        var watermarks = KafkaTopology.GetWatermarks(consumer, partitions);
        var plans = partitions
            .Select(partition => new PartitionRecoveryPlan(
                partition,
                markerOffsets[partition.Partition.Value] + 1,
                watermarks[partition.Partition.Value].High.Value))
            .ToArray();

        await recoveryObserver.AfterRecoveryPlanCreatedAsync(cancellationToken);
        
        await recoveryWorker.RunWorkerAsync(plans, cancellationToken);
    }
}