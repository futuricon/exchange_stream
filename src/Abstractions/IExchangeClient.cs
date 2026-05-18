namespace Abstractions;

public interface IExchangeClient : IAsyncDisposable
{
    string ExchangeName { get; }

    IAsyncEnumerable<RawTick> StreamTicksAsync(CancellationToken ct);
}
