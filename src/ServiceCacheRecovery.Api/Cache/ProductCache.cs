using System.Collections.Concurrent;
using Confluent.Kafka;
using ServiceCacheRecovery.Api.Kafka;
using ServiceCacheRecovery.Api.Products;

namespace ServiceCacheRecovery.Api.Cache;

public enum CacheApplyResult
{
    Ignored,
    Applied,
    TombstoneApplied,
    SkippedAsOlder
}

public sealed record CachedProduct(
    Product? Product,
    bool IsDeleted,
    int SourcePartition,
    long SourceOffset);

public sealed class ProductCache
{
    private ConcurrentDictionary<Guid, CachedProduct> _products = new();

    public Product? Get(Guid id) =>
        _products.TryGetValue(id, out var cached) && !cached.IsDeleted
            ? cached.Product
            : null;

    public IReadOnlyList<Product> GetAll() => _products.Values
        .Where(product => !product.IsDeleted)
        .Select(product => product.Product!)
        .OrderBy(product => product.Id)
        .ToArray();

    public void LoadSnapshot(IEnumerable<ProjectedProduct> products)
    {
        var snapshot = new ConcurrentDictionary<Guid, CachedProduct>(
            products.Select(product => new KeyValuePair<Guid, CachedProduct>(
                product.Id,
                new CachedProduct(
                    product.IsDeleted
                        ? null
                        : new Product(product.Id, product.Name!, product.Price!.Value),
                    product.IsDeleted,
                    product.SourcePartition,
                    product.SourceOffset))));

        Volatile.Write(ref _products, snapshot);
    }

    public CacheApplyResult Apply(ConsumeResult<string, byte[]> record)
    {
        if (ProductRecord.IsMarker(record.Message.Key) ||
            !ProductRecord.TryGetProductId(record.Message.Key, out var productId))
        {
            return CacheApplyResult.Ignored;
        }

        var products = Volatile.Read(ref _products);
        if (products.TryGetValue(productId, out var current))
        {
            if (current.SourcePartition != record.Partition.Value)
            {
                throw new InvalidOperationException(
                    $"Product {productId} moved from partition {current.SourcePartition} " +
                    $"to {record.Partition.Value}. Offset versions are no longer comparable.");
            }

            if (current.SourceOffset >= record.Offset.Value)
            {
                return CacheApplyResult.SkippedAsOlder;
            }
        }

        if (record.Message.Value is null)
        {
            products[productId] = new CachedProduct(
                null,
                IsDeleted: true,
                record.Partition.Value,
                record.Offset.Value);
            return CacheApplyResult.TombstoneApplied;
        }

        var product = ProductRecord.DeserializeProduct(record.Message.Value);
        if (product.Id != productId)
        {
            throw new InvalidDataException("Product key and payload IDs do not match.");
        }

        products[productId] = new CachedProduct(
            product,
            false,
            record.Partition.Value,
            record.Offset.Value);
        return CacheApplyResult.Applied;
    }
}