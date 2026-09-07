namespace ServiceCacheRecovery.Api.Cache.Hydartion;

public class RecoveryWorker(PartitionRecoveryWorker partitionRecoveryWorker, RehydrationStatus recoveryStatus)
{
    private Task? _workerTask;
    
    public async Task RunWorkerAsync(IReadOnlyList<PartitionRecoveryPlan> plans, CancellationToken stoppingToken)
    {
        using var workersCancellation = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        
        var workers = plans
            .Select(plan => partitionRecoveryWorker.RunWorkerAsync(plan, workersCancellation))
            .ToArray();
        _workerTask = Task.WhenAll(workers);

        await recoveryStatus.WhenReady(stoppingToken);
    }
}