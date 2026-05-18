using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;

namespace ExchangeStream.Tests.Clients;

internal sealed class FragmentingWebSocketServer : IAsyncDisposable
{
    private readonly HttpListener _listener;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _runLoop;

    public Uri Uri { get; }

    private FragmentingWebSocketServer(HttpListener listener, int port, IReadOnlyList<string> messages, IReadOnlyList<int> fragmentSizes)
    {
        _listener = listener;
        Uri = new Uri($"ws://localhost:{port}/");
        _runLoop = AcceptLoopAsync(messages, fragmentSizes, _cts.Token);
    }

    public static FragmentingWebSocketServer Start(IReadOnlyList<string> messages, IReadOnlyList<int> fragmentSizes)
    {
        var port = GetFreePort();
        var listener = new HttpListener();
        listener.Prefixes.Add($"http://localhost:{port}/");
        listener.Start();
        return new FragmentingWebSocketServer(listener, port, messages, fragmentSizes);
    }

    private async Task AcceptLoopAsync(IReadOnlyList<string> messages, IReadOnlyList<int> fragmentSizes, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                HttpListenerContext ctx;
                try { ctx = await _listener.GetContextAsync().ConfigureAwait(false); }
                catch (HttpListenerException) { return; }
                catch (ObjectDisposedException) { return; }

                if (!ctx.Request.IsWebSocketRequest)
                {
                    ctx.Response.StatusCode = 400;
                    ctx.Response.Close();
                    continue;
                }

                var wsCtx = await ctx.AcceptWebSocketAsync(null).ConfigureAwait(false);
                _ = Task.Run(() => HandleAsync(wsCtx.WebSocket, messages, fragmentSizes, ct), ct);
            }
        }
        catch (OperationCanceledException) { }
    }

    private static async Task HandleAsync(
        WebSocket ws,
        IReadOnlyList<string> messages,
        IReadOnlyList<int> fragmentSizes,
        CancellationToken ct)
    {
        try
        {
            foreach (var msg in messages)
            {
                var bytes = Encoding.UTF8.GetBytes(msg);
                int offset = 0;
                int chunkIdx = 0;

                while (offset < bytes.Length)
                {
                    var remaining = bytes.Length - offset;
                    var size = chunkIdx < fragmentSizes.Count
                        ? Math.Min(fragmentSizes[chunkIdx], remaining)
                        : remaining;

                    var endOfMessage = (offset + size) >= bytes.Length;
                    await ws.SendAsync(
                        new ArraySegment<byte>(bytes, offset, size),
                        WebSocketMessageType.Text,
                        endOfMessage,
                        ct).ConfigureAwait(false);

                    offset += size;
                    chunkIdx++;
                }
            }

            await Task.Delay(200, ct).ConfigureAwait(false);
            if (ws.State == WebSocketState.Open)
            {
                await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "done", ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException) { }
    }

    private static int GetFreePort()
    {
        using var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        var port = ((IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try { _listener.Stop(); _listener.Close(); } catch { }
        try { await _runLoop.ConfigureAwait(false); } catch { }
        _cts.Dispose();
    }
}
