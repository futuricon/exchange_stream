using Abstractions;
using FluentAssertions;
using Infrastructure.Normalizers;
using Infrastructure.Processing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace ExchangeStream.Tests.Pipeline;

public class TickPipelineTests
{
    private static IReadOnlyList<ITickNormalizer> Normalizers() => new ITickNormalizer[]
    {
        new JsonTickNormalizer(NullLogger<JsonTickNormalizer>.Instance),
        new CsvTickNormalizer()
    };

    private static HashSetDeduplicator Dedup() =>
        new(NullLogger<HashSetDeduplicator>.Instance, TimeSpan.FromMinutes(10));

    private static TickPipeline BuildPipeline(
        IExchangeClient[] clients,
        ITickRepository repo,
        IDeduplicator dedup) =>
        new(clients, Normalizers(), dedup, repo,
            NullLogger<TickPipeline>.Instance,
            flushInterval: TimeSpan.FromMilliseconds(100),
            statsInterval: TimeSpan.FromSeconds(30));

    [Fact]
    public async Task Pipeline_FromTwoExchanges_PersistsAllUniqueTicks()
    {
        var jsonTicks = new[]
        {
            new RawTick("A", """{"ticker":"BTC","price":100,"volume":1,"ts":1716000000000}""", "json"),
            new RawTick("A", """{"ticker":"BTC","price":101,"volume":1,"ts":1716000000001}""", "json"),
            new RawTick("A", """{"ticker":"BTC","price":102,"volume":1,"ts":1716000000002}""", "json"),
        };
        var csvTicks = new[]
        {
            new RawTick("B", "ETH,10,2,1716000000000", "csv"),
            new RawTick("B", "ETH,11,2,1716000000001", "csv"),
            new RawTick("B", "ETH,12,2,1716000000002", "csv"),
        };

        var clientA = new FakeExchangeClient("A", jsonTicks);
        var clientB = new FakeExchangeClient("B", csvTicks);
        var repo = new InMemoryTickRepository();
        using var dedup = Dedup();

        var pipeline = BuildPipeline(new IExchangeClient[] { clientA, clientB }, repo, dedup);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var run = pipeline.RunAsync(cts.Token);

        await WaitUntil(() => repo.Saved.Count >= 6, TimeSpan.FromSeconds(5));
        cts.Cancel();
        try { await run; } catch (OperationCanceledException) { }

        repo.Saved.Should().HaveCount(6);
        repo.Saved.Select(t => t.Source).Distinct().Should().BeEquivalentTo(new[] { "A", "B" });
    }

    [Fact]
    public async Task Pipeline_Deduplicates_TicksWithSameSourceTickerTimestamp()
    {
        var ticks = new[]
        {
            new RawTick("A", """{"ticker":"BTC","price":100,"volume":1,"ts":1716000000000}""", "json"),
            new RawTick("A", """{"ticker":"BTC","price":999,"volume":2,"ts":1716000000000}""", "json"), // dup
            new RawTick("A", """{"ticker":"BTC","price":101,"volume":1,"ts":1716000000001}""", "json"),
        };

        var client = new FakeExchangeClient("A", ticks);
        var repo = new InMemoryTickRepository();
        using var dedup = Dedup();

        var pipeline = BuildPipeline(new IExchangeClient[] { client }, repo, dedup);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var run = pipeline.RunAsync(cts.Token);

        await WaitUntil(() => repo.Saved.Count >= 2, TimeSpan.FromSeconds(5));
        await Task.Delay(300);
        cts.Cancel();
        try { await run; } catch (OperationCanceledException) { }

        repo.Saved.Should().HaveCount(2);
        pipeline.DuplicatesCount.Should().Be(1);
    }

    [Fact]
    public async Task Pipeline_DropsInvalidTicks_AndContinues()
    {
        var ticks = new[]
        {
            new RawTick("A", "not json", "json"),
            new RawTick("A", """{"ticker":"BTC","price":100,"volume":1,"ts":1716000000000}""", "json"),
            new RawTick("A", """{"ticker":"","price":1,"volume":1,"ts":1}""", "json"),
        };

        var client = new FakeExchangeClient("A", ticks);
        var repo = new InMemoryTickRepository();
        using var dedup = Dedup();

        var pipeline = BuildPipeline(new IExchangeClient[] { client }, repo, dedup);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var run = pipeline.RunAsync(cts.Token);

        await WaitUntil(() => repo.Saved.Count >= 1, TimeSpan.FromSeconds(5));
        await Task.Delay(300);
        cts.Cancel();
        try { await run; } catch (OperationCanceledException) { }

        repo.Saved.Should().HaveCount(1);
        pipeline.InvalidCount.Should().Be(2);
    }

    private static async Task WaitUntil(Func<bool> condition, TimeSpan timeout)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            if (condition()) return;
            await Task.Delay(20);
        }
    }
}
