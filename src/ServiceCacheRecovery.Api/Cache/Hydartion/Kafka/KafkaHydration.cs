using Microsoft.Extensions.Options;
using ServiceCacheRecovery.Api.Configuration;
using ServiceCacheRecovery.Api.Kafka;

namespace ServiceCacheRecovery.Api.Cache.Hydartion.Kafka;

public class KafkaHydration(
    IPartitionConsumerFactory consumerFactory, 
    RecoveryWorker recoveryWorker, 
    RehydrationStatus rehydrationStatus,
    IRecoveryObserver recoveryObserver,
    IOptions<KafkaOptions> kafkaOptions) : ICacheHydration
{
    public async Task RehydrateAsync(CancellationToken cancellationToken)
    {
        var partitions = KafkaTopology.GetPartitions(kafkaOptions.Value.BootstrapServers, kafkaOptions.Value.ProductsTopic);
        
        rehydrationStatus.Setup(partitions);
        
        var consumer = consumerFactory.Create();
        var watermarks = KafkaTopology.GetWatermarks(consumer, partitions);
        
        var plans = partitions
            .Select(partition => new PartitionRecoveryPlan(
                partition,
                watermarks[partition.Partition.Value].Low.Value,
                watermarks[partition.Partition.Value].High.Value))
            .ToArray();
        
        await recoveryObserver.AfterRecoveryPlanCreatedAsync(cancellationToken);
        
        await recoveryWorker.RunWorkerAsync(plans, cancellationToken);
    }
}