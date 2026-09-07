using ServiceCacheRecovery.Api.Configuration;

namespace ServiceCacheRecovery.Api.Cache.Hydartion;

public interface ICacheHydration
{
    Task RehydrateAsync(CancellationToken cancellationToken);
}