using System.Collections.Concurrent;
using System.Collections.Frozen;
using Confluent.Kafka;

namespace ServiceCacheRecovery.Api.Cache.Hydartion;

public class RehydrationStatus
{
    private FrozenSet<int> _partitions = [];
    private bool _failed;
    private readonly ConcurrentDictionary<int, byte> _readyPartitions = [];
    private readonly ConcurrentDictionary<int, byte> _failedPartitions = [];

    public void Setup(IReadOnlyList<TopicPartition> partitions)
    {
        _failed = false;
        _partitions = partitions.Select(x => x.Partition.Value).ToFrozenSet();
    }

    public bool IsReady => !_failed && _partitions.Count > 0 && _partitions.Count == _readyPartitions.Count;
    
    public void MarkReady(int partition)
    {
        _readyPartitions.TryAdd(partition, 0);
        _failedPartitions.TryRemove(partition, out _);
    }
    
    public void MarkFailed() => _failed = true;

    public void MarkFailed(int partition)
    {
        _failedPartitions.TryAdd(partition, 0);
        _readyPartitions.TryRemove(partition, out _);
    }
    
    public async Task WhenReady(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(100, stoppingToken);
            
            if(_readyPartitions.Count == _partitions.Count) return;
        }
    }
}