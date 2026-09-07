using Microsoft.Extensions.Options;
using Npgsql;
using ServiceCacheRecovery.Api.Cache;
using ServiceCacheRecovery.Api.Cache.Hydartion;
using ServiceCacheRecovery.Api.Cache.Hydartion.Kafka;
using ServiceCacheRecovery.Api.Cache.Hydartion.Snapshot;
using ServiceCacheRecovery.Api.Configuration;
using ServiceCacheRecovery.Api.Infrastructure;
using ServiceCacheRecovery.Api.Kafka;
using ServiceCacheRecovery.Api.Persistence;
using ServiceCacheRecovery.Api.Products;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.BootstrapServers))
    .Validate(options => !string.IsNullOrWhiteSpace(options.ProductsTopic))
    .Validate(options => options.PartitionCount > 0)
    .ValidateOnStart();
builder.Services.AddOptions<DatabaseOptions>()
    .Bind(builder.Configuration.GetSection(DatabaseOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.ConnectionString))
    .ValidateOnStart();
builder.Services.AddOptions<CacheRecoveryOptions>()
    .Bind(builder.Configuration.GetSection(CacheRecoveryOptions.SectionName))
    .Validate(options => options.MarkerPollMilliseconds > 0)
    .Validate(options => options.MarkerTimeoutSeconds > 0)
    .ValidateOnStart();

builder.Services.AddSingleton(serviceProvider =>
    NpgsqlDataSource.Create(
        serviceProvider.GetRequiredService<IOptions<DatabaseOptions>>().Value.ConnectionString));
builder.Services.AddSingleton<ProductProjectionRepository>();
builder.Services.AddSingleton<ProductEventProducer>();
builder.Services.AddSingleton<ProductCache>();
builder.Services.AddSingleton<IRecoveryObserver, NoOpRecoveryObserver>();
builder.Services.AddSingleton<IPartitionConsumerFactory, PartitionConsumerFactory>();
builder.Services.AddSingleton<PartitionRecoveryWorker>();
builder.Services.AddSingleton<KafkaHydration>();
builder.Services.AddSingleton<SnapshotHydration>();
builder.Services.AddSingleton<RecoveryWorker>();
builder.Services.AddSingleton<RehydrationStatus>();

// Registration order matters: infrastructure must exist before either consumer starts.
builder.Services.AddHostedService<InfrastructureInitializer>();
builder.Services.AddHostedService<DatabaseProjectorService>();
builder.Services.AddHostedService<CacheConsumerService>();

var app = builder.Build();

app.MapPut("/products/{id:guid}", async (
    Guid id,
    UpsertProductRequest request,
    ProductEventProducer producer,
    CancellationToken cancellationToken) =>
{
    if (string.IsNullOrWhiteSpace(request.Name) || request.Price < 0)
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["product"] = ["Name is required and price cannot be negative."]
        });
    }

    var delivery = await producer.UpsertAsync(new Product(id, request.Name, request.Price), cancellationToken);
    return Results.Accepted($"/products/{id}", new
    {
        partition = delivery.Partition.Value,
        offset = delivery.Offset.Value
    });
});

app.MapDelete("/products/{id:guid}", async (
    Guid id,
    ProductEventProducer producer,
    CancellationToken cancellationToken) =>
{
    var delivery = await producer.DeleteAsync(id, cancellationToken);
    return Results.Accepted($"/products/{id}", new
    {
        partition = delivery.Partition.Value,
        offset = delivery.Offset.Value
    });
});

app.MapGet("/products/{id:guid}", (Guid id, ProductCache cache, RehydrationStatus recovery) =>
{
    if (!recovery.IsReady)
    {
        return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
    }

    return cache.Get(id) is { } product ? Results.Ok(product) : Results.NotFound();
});

app.MapGet("/products", (ProductCache cache, RehydrationStatus recovery) =>
{
    if (!recovery.IsReady)
    {
        return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
    }

    return Results.Ok(cache.GetAll());
});

app.MapGet("/health/ready", (RehydrationStatus recovery) =>
    recovery.IsReady
        ? Results.Ok(new { status = "ready" })
        : Results.StatusCode(StatusCodes.Status503ServiceUnavailable));

app.Run();

public partial class Program;
