namespace ServiceCacheRecovery.Api.Cache;

public interface IRecoveryObserver
{
    Task AfterMarkersPersistedAsync(CancellationToken cancellationToken);

    Task AfterRecoveryPlanCreatedAsync(CancellationToken cancellationToken);
}

public sealed class NoOpRecoveryObserver : IRecoveryObserver
{
    public Task AfterMarkersPersistedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task AfterRecoveryPlanCreatedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}