using System.Runtime.CompilerServices;
using Abstractions;

namespace ExchangeStream.Tests.Pipeline;

internal sealed class FakeExchangeClient : IExchangeClient
{
    private readonly IReadOnlyList<RawTick> _ticks;
    private readonly TimeSpan _delayBetween;

    public FakeExchangeClient(
        string name,
        IReadOnlyList<RawTick> ticks,
        TimeSpan? delayBetween = null)
    {
        ExchangeName = name;
        _ticks = ticks;
        _delayBetween = delayBetween ?? TimeSpan.Zero;
    }

    public string ExchangeName { get; }

    public async IAsyncEnumerable<RawTick> StreamTicksAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        foreach (var t in _ticks)
        {
            if (ct.IsCancellationRequested) yield break;
            if (_delayBetween > TimeSpan.Zero)
                await Task.Delay(_delayBetween, ct).ConfigureAwait(false);
            yield return t;
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
