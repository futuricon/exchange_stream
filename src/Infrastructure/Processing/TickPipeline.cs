using System.Threading.Channels;
using Abstractions;
using Domain;
using Microsoft.Extensions.Logging;

namespace Infrastructure.Processing;

public sealed class TickPipeline
{
    private const int BatchSize = 100;
    private const int ChannelCapacity = 1000;
    private const int NormalizerWorkers = 2;

    private readonly IReadOnlyList<IExchangeClient> _clients;
    private readonly IReadOnlyList<ITickNormalizer> _normalizers;
    private readonly IDeduplicator _deduplicator;
    private readonly ITickRepository _repository;
    private readonly ILogger<TickPipeline> _logger;

    private readonly TimeSpan _flushInterval;
    private readonly TimeSpan _statsInterval;

    private readonly Channel<RawTick> _rawChannel;
    private readonly Channel<Tick> _normalizedChannel;

    private long _processed;
    private long _duplicates;
    private long _invalid;

    public TickPipeline(
        IReadOnlyList<IExchangeClient> clients,
        IReadOnlyList<ITickNormalizer> normalizers,
        IDeduplicator deduplicator,
        ITickRepository repository,
        ILogger<TickPipeline> logger,
        TimeSpan? flushInterval = null,
        TimeSpan? statsInterval = null)
    {
        _clients = clients;
        _normalizers = normalizers;
        _deduplicator = deduplicator;
        _repository = repository;
        _logger = logger;
        _flushInterval = flushInterval ?? TimeSpan.FromSeconds(1);
        _statsInterval = statsInterval ?? TimeSpan.FromSeconds(10);

        _rawChannel = Channel.CreateBounded<RawTick>(new BoundedChannelOptions(ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait
        });
        _normalizedChannel = Channel.CreateBounded<Tick>(new BoundedChannelOptions(ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true
        });
    }

    public long ProcessedCount => Interlocked.Read(ref _processed);
    public long DuplicatesCount => Interlocked.Read(ref _duplicates);
    public long InvalidCount => Interlocked.Read(ref _invalid);

    public async Task RunAsync(CancellationToken ct)
    {
        var producers = Task.WhenAll(_clients.Select(c => RunClientAsync(c, ct)));
        var normalizers = Task.WhenAll(Enumerable.Range(0, NormalizerWorkers)
            .Select(_ => RunNormalizerAsync(ct)));
        var writer = RunBatchWriterAsync(ct);
        var stats = RunStatsAsync(ct);

        // When all producers finished — close the raw channel so normalizers can drain and exit.
        _ = producers.ContinueWith(_ => _rawChannel.Writer.TryComplete(),
            TaskContinuationOptions.ExecuteSynchronously);
        _ = normalizers.ContinueWith(_ => _normalizedChannel.Writer.TryComplete(),
            TaskContinuationOptions.ExecuteSynchronously);

        await Task.WhenAll(producers, normalizers, writer, stats).ConfigureAwait(false);
    }

    private async Task RunClientAsync(IExchangeClient client, CancellationToken ct)
    {
        try
        {
            await foreach (var raw in client.StreamTicksAsync(ct).ConfigureAwait(false))
            {
                await _rawChannel.Writer.WriteAsync(raw, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Client {Exchange} terminated", client.ExchangeName);
        }
    }

    private async Task RunNormalizerAsync(CancellationToken ct)
    {
        try
        {
            await foreach (var raw in _rawChannel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                var normalizer = _normalizers.FirstOrDefault(n => n.CanHandle(raw.Format));
                if (normalizer is null)
                {
                    _logger.LogWarning("No normalizer for format '{Format}'", raw.Format);
                    Interlocked.Increment(ref _invalid);
                    continue;
                }

                var tick = normalizer.Normalize(raw);
                if (tick is null)
                {
                    Interlocked.Increment(ref _invalid);
                    continue;
                }

                if (!_deduplicator.TryAdd(tick.DeduplicationKey))
                {
                    Interlocked.Increment(ref _duplicates);
                    continue;
                }

                await _normalizedChannel.Writer.WriteAsync(tick, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
    }

    private async Task RunBatchWriterAsync(CancellationToken ct)
    {
        var batch = new List<Tick>(BatchSize);
        var reader = _normalizedChannel.Reader;

        while (!ct.IsCancellationRequested)
        {
            using var flushCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            flushCts.CancelAfter(_flushInterval);

            try
            {
                var tick = await reader.ReadAsync(flushCts.Token).ConfigureAwait(false);
                batch.Add(tick);
                if (batch.Count >= BatchSize)
                    await FlushAsync(batch, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                // Flush timer fired — write partial batch
                if (batch.Count > 0)
                    await FlushAsync(batch, ct).ConfigureAwait(false);
            }
            catch (ChannelClosedException) { break; }
            catch (OperationCanceledException) { break; }
        }

        if (batch.Count > 0)
        {
            try { await FlushAsync(batch, CancellationToken.None).ConfigureAwait(false); }
            catch (Exception ex) { _logger.LogError(ex, "Final flush failed"); }
        }
    }

    private async Task FlushAsync(List<Tick> batch, CancellationToken ct)
    {
        try
        {
            await _repository.SaveBatchAsync(batch, ct).ConfigureAwait(false);
            Interlocked.Add(ref _processed, batch.Count);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Failed to save batch of {Count}", batch.Count);
        }
        finally
        {
            batch.Clear();
        }
    }

    private async Task RunStatsAsync(CancellationToken ct)
    {
        try
        {
            using var timer = new PeriodicTimer(_statsInterval);
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                _logger.LogInformation(
                    "Stats: processed={Processed} duplicates={Duplicates} invalid={Invalid} rawQ={RawQ} normQ={NormQ}",
                    ProcessedCount, DuplicatesCount, InvalidCount,
                    _rawChannel.Reader.Count, _normalizedChannel.Reader.Count);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
    }
}
