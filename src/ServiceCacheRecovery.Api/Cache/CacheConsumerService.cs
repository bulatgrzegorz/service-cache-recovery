using Microsoft.Extensions.Options;
using ServiceCacheRecovery.Api.Cache.Hydartion;
using ServiceCacheRecovery.Api.Cache.Hydartion.Kafka;
using ServiceCacheRecovery.Api.Cache.Hydartion.Snapshot;
using ServiceCacheRecovery.Api.Configuration;

namespace ServiceCacheRecovery.Api.Cache;

public sealed class CacheConsumerService(
    IOptions<CacheRecoveryOptions> recoveryOptions,
    RehydrationStatus status,
    ILogger<CacheConsumerService> logger,
    KafkaHydration kafkaHydration,
    SnapshotHydration snapshotHydration) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            var recovery = recoveryOptions.Value;
            if (recovery.Strategy == RecoveryStrategy.DatabaseSnapshot)
            {
                await snapshotHydration.RehydrateAsync(stoppingToken);
            }
            else
            {
                await kafkaHydration.RehydrateAsync(stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            logger.LogInformation("Cache consumer is stopping.");
        }
        catch
        {
            status.MarkFailed();
            throw;
        }
    }
}