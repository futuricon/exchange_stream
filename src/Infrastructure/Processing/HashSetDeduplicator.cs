using System.Collections.Concurrent;
using Abstractions;
using Microsoft.Extensions.Logging;

namespace Infrastructure.Processing;

public sealed class HashSetDeduplicator : IDeduplicator, IDisposable
{
    private readonly ConcurrentDictionary<string, byte> _seen = new();
    private readonly ILogger<HashSetDeduplicator> _logger;
    private readonly Timer _cleanupTimer;

    public HashSetDeduplicator(ILogger<HashSetDeduplicator> logger)
        : this(logger, TimeSpan.FromMinutes(1))
    {
    }

    public HashSetDeduplicator(ILogger<HashSetDeduplicator> logger, TimeSpan windowSize)
    {
        _logger = logger;
        _cleanupTimer = new Timer(_ => Clear(), null, windowSize, windowSize);
    }

    public bool TryAdd(string key) => _seen.TryAdd(key, 0);

    private void Clear()
    {
        var count = _seen.Count;
        _seen.Clear();
        _logger.LogDebug("Deduplicator cache cleared ({Count} entries)", count);
    }

    public void Dispose() => _cleanupTimer.Dispose();
}
