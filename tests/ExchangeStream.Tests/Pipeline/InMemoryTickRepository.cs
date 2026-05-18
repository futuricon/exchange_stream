using System.Collections.Concurrent;
using Abstractions;
using Domain;

namespace ExchangeStream.Tests.Pipeline;

internal sealed class InMemoryTickRepository : ITickRepository
{
    private readonly ConcurrentBag<Tick> _ticks = new();

    public IReadOnlyCollection<Tick> Saved => _ticks;

    public Task SaveBatchAsync(IReadOnlyList<Tick> ticks, CancellationToken ct)
    {
        foreach (var t in ticks) _ticks.Add(t);
        return Task.CompletedTask;
    }
}
