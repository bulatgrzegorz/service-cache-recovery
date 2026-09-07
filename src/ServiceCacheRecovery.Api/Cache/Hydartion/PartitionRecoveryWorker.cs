using Confluent.Kafka;

namespace ServiceCacheRecovery.Api.Cache.Hydartion;

public sealed record PartitionRecoveryPlan(TopicPartition Partition, long Start, long Target);

public sealed class PartitionRecoveryWorker(
    IPartitionConsumerFactory consumerFactory,
    ProductCache cache,
    RehydrationStatus status)
{
    public async Task RunWorkerAsync(PartitionRecoveryPlan plan, CancellationTokenSource workersCancellation)
    {
        try
        {
            var stoppingToken = workersCancellation.Token;
            await Task.Run(() => Run(plan, stoppingToken), stoppingToken);
        }
        catch (OperationCanceledException) when (workersCancellation.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            status.MarkFailed(plan.Partition.Partition.Value);
            await workersCancellation.CancelAsync();
            throw;
        }
    }
    
    private void Run(PartitionRecoveryPlan plan, CancellationToken stoppingToken)
    {
        using var consumer = consumerFactory.Create();
        consumer.Assign(new TopicPartitionOffset(plan.Partition, new Offset(plan.Start)));

        var nextOffset = plan.Start;
        while (nextOffset < plan.Target)
        {
            nextOffset = ProcessRecord(consumer.Consume(stoppingToken));
        }

        status.MarkReady(plan.Partition.Partition.Value);

        while (!stoppingToken.IsCancellationRequested)
        {
            ProcessRecord(consumer.Consume(stoppingToken));
        }
    }

    private long ProcessRecord(ConsumeResult<string, byte[]> record)
    {
        cache.Apply(record);
        return record.Offset.Value + 1;
    }
}