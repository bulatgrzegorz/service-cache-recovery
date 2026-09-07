using System.Text.Json;
using ServiceCacheRecovery.Api.Products;

namespace ServiceCacheRecovery.Api.Kafka;

public sealed record RecoveryMarker(Guid RecoveryId, int Partition);

public static class ProductRecord
{
    private const string ProductPrefix = "product:";
    private const string MarkerPrefix = "marker:";

    public static string ProductKey(Guid id) => $"{ProductPrefix}{id:N}";

    public static string MarkerKey(Guid recoveryId, int partition) =>
        $"{MarkerPrefix}{recoveryId:N}:{partition}";

    public static bool TryGetProductId(string key, out Guid productId)
    {
        if (key.StartsWith(ProductPrefix, StringComparison.Ordinal) &&
            Guid.TryParseExact(key.AsSpan(ProductPrefix.Length), "N", out productId))
        {
            return true;
        }

        productId = Guid.Empty;
        return false;
    }

    public static bool IsMarker(string key) => key.StartsWith(MarkerPrefix, StringComparison.Ordinal);

    public static byte[] Serialize(Product product) => JsonSerializer.SerializeToUtf8Bytes(product);

    public static byte[] Serialize(RecoveryMarker marker) => JsonSerializer.SerializeToUtf8Bytes(marker);

    public static Product DeserializeProduct(byte[] value) =>
        JsonSerializer.Deserialize<Product>(value) ?? throw new InvalidDataException("Product record was empty.");

    public static RecoveryMarker DeserializeMarker(byte[] value) =>
        JsonSerializer.Deserialize<RecoveryMarker>(value) ?? throw new InvalidDataException("Marker record was empty.");
}
