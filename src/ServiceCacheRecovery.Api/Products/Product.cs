namespace ServiceCacheRecovery.Api.Products;

public sealed record Product(Guid Id, string Name, decimal Price);

public sealed record UpsertProductRequest(string Name, decimal Price);

public sealed record ProjectedProduct(
    Guid Id,
    string? Name,
    decimal? Price,
    bool IsDeleted,
    int SourcePartition,
    long SourceOffset);
