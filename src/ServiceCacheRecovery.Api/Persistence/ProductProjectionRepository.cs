using Confluent.Kafka;
using Npgsql;
using ServiceCacheRecovery.Api.Kafka;
using ServiceCacheRecovery.Api.Products;

namespace ServiceCacheRecovery.Api.Persistence;

public sealed class ProductProjectionRepository(NpgsqlDataSource dataSource)
{
    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT pg_advisory_xact_lock(721734921);

            CREATE TABLE IF NOT EXISTS products (
                product_id uuid PRIMARY KEY,
                name text NULL,
                price numeric NULL,
                is_deleted boolean NOT NULL,
                source_partition integer NOT NULL,
                source_offset bigint NOT NULL
            );

            CREATE TABLE IF NOT EXISTS recovery_markers (
                recovery_id uuid NOT NULL,
                partition integer NOT NULL,
                marker_offset bigint NOT NULL,
                processed_at timestamp with time zone NOT NULL,
                PRIMARY KEY (recovery_id, partition)
            );
            """;

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    public async Task ProjectAsync(ConsumeResult<string, byte[]> record, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        if (ProductRecord.IsMarker(record.Message.Key))
        {
            if (record.Message.Value is not null)
            {
                var marker = ProductRecord.DeserializeMarker(record.Message.Value);
                if (marker.Partition != record.Partition.Value)
                {
                    throw new InvalidDataException("Recovery marker was written to the wrong partition.");
                }

                await StoreMarkerAsync(connection, transaction, marker, record.Offset.Value, cancellationToken);
            }
        }
        else if (ProductRecord.TryGetProductId(record.Message.Key, out var productId))
        {
            Product? product = null;
            if (record.Message.Value is not null)
            {
                product = ProductRecord.DeserializeProduct(record.Message.Value);
                if (product.Id != productId)
                {
                    throw new InvalidDataException("Product key and payload IDs do not match.");
                }
            }

            await StoreProductAsync(
                connection,
                transaction,
                productId,
                product,
                record.Partition.Value,
                record.Offset.Value,
                cancellationToken);
        }
        
        await transaction.CommitAsync(cancellationToken);
    }

    public async Task WaitForMarkersAsync(
        Guid recoveryId,
        IReadOnlyDictionary<int, long> expectedOffsets,
        TimeSpan pollInterval,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        using var timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutSource.CancelAfter(timeout);

        while (!timeoutSource.Token.IsCancellationRequested)
        {
            var persisted = await GetMarkersAsync(recoveryId, timeoutSource.Token);
            if (expectedOffsets.All(expected =>
                    persisted.TryGetValue(expected.Key, out var actual) && actual == expected.Value))
            {
                return;
            }

            await Task.Delay(pollInterval, timeoutSource.Token);
        }
    }

    public async Task<IReadOnlyList<ProjectedProduct>> LoadSnapshotAsync(CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(
            System.Data.IsolationLevel.RepeatableRead,
            cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            SELECT product_id, name, price, is_deleted, source_partition, source_offset
            FROM products;
            """,
            connection,
            transaction);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        var products = new List<ProjectedProduct>();
        while (await reader.ReadAsync(cancellationToken))
        {
            products.Add(new ProjectedProduct(
                reader.GetGuid(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetDecimal(2),
                reader.GetBoolean(3),
                reader.GetInt32(4),
                reader.GetInt64(5)));
        }

        await reader.CloseAsync();
        await transaction.CommitAsync(cancellationToken);
        return products;
    }

    private async Task<IReadOnlyDictionary<int, long>> GetMarkersAsync(
        Guid recoveryId,
        CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(
            """
            SELECT partition, marker_offset
            FROM recovery_markers
            WHERE recovery_id = $1;
            """);
        command.Parameters.AddWithValue(recoveryId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        var markers = new Dictionary<int, long>();
        while (await reader.ReadAsync(cancellationToken))
        {
            markers[reader.GetInt32(0)] = reader.GetInt64(1);
        }

        return markers;
    }

    private static async Task StoreProductAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        Guid productId,
        Product? product,
        int partition,
        long offset,
        CancellationToken cancellationToken)
    {
        const string sql = """
            INSERT INTO products (
                product_id, name, price, is_deleted, source_partition, source_offset)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (product_id) DO UPDATE SET
                name = EXCLUDED.name,
                price = EXCLUDED.price,
                is_deleted = EXCLUDED.is_deleted,
                source_partition = EXCLUDED.source_partition,
                source_offset = EXCLUDED.source_offset
            WHERE products.source_partition = EXCLUDED.source_partition
              AND products.source_offset < EXCLUDED.source_offset;
            """;

        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue(productId);
        command.Parameters.AddWithValue(product?.Name is { } name ? name : DBNull.Value);
        command.Parameters.AddWithValue(product?.Price is { } price ? price : DBNull.Value);
        command.Parameters.AddWithValue(product is null);
        command.Parameters.AddWithValue(partition);
        command.Parameters.AddWithValue(offset);
        var affectedRows = await command.ExecuteNonQueryAsync(cancellationToken);
        if (affectedRows == 0)
        {
            await using var partitionCommand = new NpgsqlCommand(
                "SELECT source_partition FROM products WHERE product_id = $1;",
                connection,
                transaction);
            partitionCommand.Parameters.AddWithValue(productId);
            var existingPartition = (int?)await partitionCommand.ExecuteScalarAsync(cancellationToken);
            if (existingPartition is not null && existingPartition != partition)
            {
                throw new InvalidOperationException(
                    $"Product {productId} moved from partition {existingPartition} to {partition}.");
            }
        }
    }

    private static async Task StoreMarkerAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RecoveryMarker marker,
        long offset,
        CancellationToken cancellationToken)
    {
        const string sql = """
            INSERT INTO recovery_markers (recovery_id, partition, marker_offset, processed_at)
            VALUES ($1, $2, $3, now())
            ON CONFLICT (recovery_id, partition) DO UPDATE SET
                marker_offset = EXCLUDED.marker_offset,
                processed_at = EXCLUDED.processed_at;
            """;

        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue(marker.RecoveryId);
        command.Parameters.AddWithValue(marker.Partition);
        command.Parameters.AddWithValue(offset);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
