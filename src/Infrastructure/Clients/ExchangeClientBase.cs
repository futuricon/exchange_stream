using System.Buffers;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text;
using Abstractions;
using Microsoft.Extensions.Logging;

namespace Infrastructure.Clients;

public abstract class ExchangeClientBase : IExchangeClient
{
    private const int ReceiveBufferSize = 4096;

    private readonly ILogger _logger;
    private readonly ExchangeClientOptions _options;
    private readonly Random _random = new();

    private ClientWebSocket? _ws;

    protected ExchangeClientBase(ILogger logger, ExchangeClientOptions options)
    {
        _logger = logger;
        _options = options;
    }

    public abstract string ExchangeName { get; }

    public async IAsyncEnumerable<RawTick> StreamTicksAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        int attempt = 0;

        while (!ct.IsCancellationRequested)
        {
            _ws = new ClientWebSocket();
            bool connected = false;

            try
            {
                _logger.LogInformation("[{Exchange}] Connecting to {Uri}", ExchangeName, _options.Uri);
                await _ws.ConnectAsync(_options.Uri, ct).ConfigureAwait(false);
                _logger.LogInformation("[{Exchange}] Connected", ExchangeName);
                connected = true;
                attempt = 0;
            }
            catch (OperationCanceledException) { yield break; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[{Exchange}] Connect failed", ExchangeName);
            }

            if (connected)
            {
                await foreach (var tick in ReadAllAsync(ct).ConfigureAwait(false))
                    yield return tick;
            }

            await SafeCloseAsync().ConfigureAwait(false);
            _ws.Dispose();

            if (ct.IsCancellationRequested) yield break;

            var delay = ComputeBackoff(attempt++);
            _logger.LogWarning("[{Exchange}] Reconnecting in {DelayMs}ms", ExchangeName, (int)delay.TotalMilliseconds);

            try { await Task.Delay(delay, ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { yield break; }
        }
    }

    private async IAsyncEnumerable<RawTick> ReadAllAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(ReceiveBufferSize);
        using var ms = new MemoryStream();

        try
        {
            while (_ws!.State == WebSocketState.Open && !ct.IsCancellationRequested)
            {
                WebSocketReceiveResult result;
                try
                {
                    result = await _ws.ReceiveAsync(new ArraySegment<byte>(buffer), ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) { yield break; }
                catch (WebSocketException ex)
                {
                    _logger.LogWarning(ex, "[{Exchange}] Receive error", ExchangeName);
                    yield break;
                }

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    _logger.LogWarning("[{Exchange}] Server closed connection", ExchangeName);
                    yield break;
                }

                ms.Write(buffer, 0, result.Count);

                if (!result.EndOfMessage) continue;
                if (ms.Length == 0) continue;

                var message = Encoding.UTF8.GetString(ms.GetBuffer(), 0, (int)ms.Length);
                ms.SetLength(0);

                RawTick? tick = null;
                try { tick = ParseMessage(message); }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[{Exchange}] Parse failed: {Msg}", ExchangeName, message);
                }

                if (tick is not null)
                    yield return tick;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    protected abstract RawTick? ParseMessage(string message);

    private TimeSpan ComputeBackoff(int attempt)
    {
        var baseMs = _options.InitialReconnectDelay.TotalMilliseconds * Math.Pow(2, Math.Min(attempt, 10));
        var capped = Math.Min(baseMs, _options.MaxReconnectDelay.TotalMilliseconds);
        // ±20% jitter to avoid synchronized reconnects
        var jitter = capped * (0.8 + _random.NextDouble() * 0.4);
        return TimeSpan.FromMilliseconds(jitter);
    }

    private async Task SafeCloseAsync()
    {
        if (_ws is null) return;
        if (_ws.State != WebSocketState.Open && _ws.State != WebSocketState.CloseReceived)
            return;

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        try
        {
            await _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "closing", cts.Token).ConfigureAwait(false);
        }
        catch { /* best effort */ }
    }

    public async ValueTask DisposeAsync()
    {
        await SafeCloseAsync().ConfigureAwait(false);
        _ws?.Dispose();
        _ws = null;
    }
}
