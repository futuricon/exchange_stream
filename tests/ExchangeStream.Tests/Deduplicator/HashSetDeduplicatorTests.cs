using FluentAssertions;
using Infrastructure.Processing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace ExchangeStream.Tests.Deduplicator;

public class HashSetDeduplicatorTests
{
    private static HashSetDeduplicator Create(TimeSpan? window = null) =>
        new(NullLogger<HashSetDeduplicator>.Instance, window ?? TimeSpan.FromMinutes(10));

    [Fact]
    public void TryAdd_FirstTime_ReturnsTrue()
    {
        using var dedup = Create();
        dedup.TryAdd("key1").Should().BeTrue();
    }

    [Fact]
    public void TryAdd_SameKeyTwice_ReturnsFalseSecondTime()
    {
        using var dedup = Create();
        dedup.TryAdd("key1");
        dedup.TryAdd("key1").Should().BeFalse();
    }

    [Fact]
    public void TryAdd_DifferentKeys_AllReturnTrue()
    {
        using var dedup = Create();
        dedup.TryAdd("a").Should().BeTrue();
        dedup.TryAdd("b").Should().BeTrue();
        dedup.TryAdd("c").Should().BeTrue();
    }

    [Fact]
    public void TryAdd_HighConcurrency_AddsKeyExactlyOnce()
    {
        using var dedup = Create();
        var successes = 0;
        Parallel.For(0, 1000, _ =>
        {
            if (dedup.TryAdd("shared-key"))
                Interlocked.Increment(ref successes);
        });
        successes.Should().Be(1);
    }

    [Fact]
    public async Task PeriodicClear_AllowsReaddingKeyAfterWindow()
    {
        using var dedup = Create(TimeSpan.FromMilliseconds(100));
        dedup.TryAdd("k").Should().BeTrue();
        dedup.TryAdd("k").Should().BeFalse();

        await Task.Delay(200);
        dedup.TryAdd("k").Should().BeTrue("after the window expired the cache is cleared");
    }
}
