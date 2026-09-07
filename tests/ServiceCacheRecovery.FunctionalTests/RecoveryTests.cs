using System.Net;
using System.Net.Http.Json;
using ServiceCacheRecovery.Api.Configuration;
using ServiceCacheRecovery.Api.Products;

namespace ServiceCacheRecovery.FunctionalTests;

[NotInParallel]
public sealed class RecoveryTests
{
    [ClassDataSource<InfrastructureFixture>(Shared = SharedType.PerTestSession)]
    public required InfrastructureFixture Infrastructure { get; init; }

    [Test]
    public async Task KafkaReplay_RebuildsLatestStateAndAppliesTombstones()
    {
        await using var app = await RecoveryApplication.CreateAsync(
            Infrastructure,
            RecoveryStrategy.KafkaReplay);
        var existing = new Product(Guid.NewGuid(), "Mechanical keyboard", 149.00m);
        var deleted = new Product(Guid.NewGuid(), "Old monitor", 90.00m);

        await app.UpsertAsync(existing with { Price = 129.00m });
        await app.UpsertAsync(existing);
        await app.UpsertAsync(deleted);
        await app.DeleteAsync(deleted.Id);

        using var client = app.CreateClient();
        await WaitUntilReadyAsync(client);

        var recovered = await client.GetFromJsonAsync<Product>($"/products/{existing.Id}");
        var deletedResponse = await client.GetAsync($"/products/{deleted.Id}");

        await Assert.That(recovered).IsEqualTo(existing);
        await Assert.That(deletedResponse.StatusCode).IsEqualTo(HttpStatusCode.NotFound);
    }

    [Test]
    public async Task KafkaReplay_DoesNotLoseRecordsPublishedAfterRecoveryTargetWasCaptured()
    {
        await using var app = await RecoveryApplication.CreateAsync(
            Infrastructure,
            RecoveryStrategy.KafkaReplay);
        var original = new Product(Guid.NewGuid(), "Webcam", 120.00m);
        var changed = original with { Price = 99.00m };
        await app.UpsertAsync(original);
        app.AfterRecoveryPlanCreated = _ => app.UpsertAsync(changed);

        using var client = app.CreateClient();
        await WaitUntilReadyAsync(client);
        await EventuallyAsync(async () =>
            await client.GetFromJsonAsync<Product>($"/products/{original.Id}") == changed);

        var recovered = await client.GetFromJsonAsync<Product>($"/products/{original.Id}");
        await Assert.That(recovered).IsEqualTo(changed);
    }

    [Test]
    public async Task KafkaReplay_EmptyTopicBecomesReady()
    {
        await using var app = await RecoveryApplication.CreateAsync(
            Infrastructure,
            RecoveryStrategy.KafkaReplay);

        using var client = app.CreateClient();
        await WaitUntilReadyAsync(client);
    }

    [Test]
    public async Task KafkaReplay_EveryPartitionHydratesAndContinuesLive()
    {
        await using var app = await RecoveryApplication.CreateAsync(
            Infrastructure,
            RecoveryStrategy.KafkaReplay);
        var products = Enumerable.Range(0, 3)
            .Select(partition => new Product(Guid.NewGuid(), $"Product {partition}", 10 + partition))
            .ToArray();

        for (var partition = 0; partition < products.Length; partition++)
        {
            await app.UpsertAsync(products[partition], partition);
        }

        using var client = app.CreateClient();
        await WaitUntilReadyAsync(client);

        var changed = products
            .Select(product => product with { Price = product.Price + 100 })
            .ToArray();
        for (var partition = 0; partition < changed.Length; partition++)
        {
            await app.UpsertAsync(changed[partition], partition);
        }

        await EventuallyAsync(async () =>
        {
            for (var partition = 0; partition < changed.Length; partition++)
            {
                var product = await client.GetFromJsonAsync<Product>($"/products/{changed[partition].Id}");
                if (product != changed[partition])
                {
                    return false;
                }
            }

            return true;
        });
    }

    [Test]
    public async Task DatabaseSnapshot_LoadsProjectionThenContinuesLive()
    {
        await using var app = await RecoveryApplication.CreateAsync(
            Infrastructure,
            RecoveryStrategy.DatabaseSnapshot);
        var product = new Product(Guid.NewGuid(), "Standing desk", 699.00m);
        await app.UpsertAsync(product);

        using var client = app.CreateClient();
        await WaitUntilReadyAsync(client);

        var recovered = await client.GetFromJsonAsync<Product>($"/products/{product.Id}");

        await Assert.That(recovered).IsEqualTo(product);

        var changed = product with { Price = 649.00m };
        var updateResponse = await client.PutAsJsonAsync(
            $"/products/{product.Id}",
            new UpsertProductRequest(changed.Name, changed.Price));
        await Assert.That(updateResponse.StatusCode).IsEqualTo(HttpStatusCode.Accepted);

        await EventuallyAsync(async () =>
            await client.GetFromJsonAsync<Product>($"/products/{product.Id}") == changed);
    }

    [Test]
    public async Task DatabaseSnapshot_DoesNotRegressWhenProjectionIsNewerThanMarker()
    {
        await using var app = await RecoveryApplication.CreateAsync(
            Infrastructure,
            RecoveryStrategy.DatabaseSnapshot);
        var original = new Product(Guid.NewGuid(), "USB-C dock", 189.00m);
        var changed = original with { Price = 169.00m };
        await app.UpsertAsync(original);

        app.AfterMarkersPersisted = async cancellationToken =>
        {
            await app.UpsertAsync(changed);
            await app.WaitForProjectionAsync(changed, cancellationToken);
        };

        using var client = app.CreateClient();
        await WaitUntilReadyAsync(client);

        var recovered = await client.GetFromJsonAsync<Product>($"/products/{original.Id}");

        await Assert.That(recovered).IsEqualTo(changed);
    }

    [Test]
    public async Task EveryReplicaBuildsTheCompleteCache()
    {
        await using var first = await RecoveryApplication.CreateAsync(
            Infrastructure,
            RecoveryStrategy.DatabaseSnapshot);
        var product = new Product(Guid.NewGuid(), "Noise-cancelling headphones", 299.00m);
        await first.UpsertAsync(product);

        await using var second = first.CreateReplica(RecoveryStrategy.DatabaseSnapshot);
        var firstClientTask = Task.Run(first.CreateClient);
        var secondClientTask = Task.Run(second.CreateClient);
        using var firstClient = await firstClientTask;
        using var secondClient = await secondClientTask;
        await Task.WhenAll(WaitUntilReadyAsync(firstClient), WaitUntilReadyAsync(secondClient));

        var fromFirst = await firstClient.GetFromJsonAsync<Product>($"/products/{product.Id}");
        var fromSecond = await secondClient.GetFromJsonAsync<Product>($"/products/{product.Id}");

        await Assert.That(fromFirst).IsEqualTo(product);
        await Assert.That(fromSecond).IsEqualTo(product);
    }

    private static async Task WaitUntilReadyAsync(HttpClient client) =>
        await EventuallyAsync(async () =>
            (await client.GetAsync("/health/ready")).StatusCode == HttpStatusCode.OK);

    private static async Task EventuallyAsync(Func<Task<bool>> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        try
        {
            while (true)
            {
                if (await condition())
                {
                    return;
                }

                await Task.Delay(50, timeout.Token);
            }
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested)
        {
            throw new TimeoutException("Condition was not satisfied within 30 seconds.");
        }
    }
}
