using Abstractions;
using FluentAssertions;
using Infrastructure.Clients;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace ExchangeStream.Tests.Clients;

public class ExchangeClientBaseFragmentTests
{
    private sealed class EchoClient : ExchangeClientBase
    {
        public EchoClient(ILogger logger, ExchangeClientOptions options) : base(logger, options) { }
        public override string ExchangeName => "Echo";
        protected override RawTick? ParseMessage(string message)
            => new RawTick(ExchangeName, message, "raw");
    }

    [Fact]
    public async Task ReceivesCompleteMessage_FromMultipleFragments()
    {
        const string payload = """{"ticker":"BTCUSDT","price":65000.5,"volume":1.25,"ts":1716000000000}""";

        // Force fragmentation: server splits into 3 chunks
        var fragments = new[] { 10, 20, 200 };
        await using var server = FragmentingWebSocketServer.Start(new[] { payload }, fragments);

        var client = new EchoClient(
            NullLogger.Instance,
            new ExchangeClientOptions
            {
                Uri = server.Uri,
                InitialReconnectDelay = TimeSpan.FromMilliseconds(50),
                MaxReconnectDelay = TimeSpan.FromMilliseconds(200)
            });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var received = new List<RawTick>();

        try
        {
            await foreach (var t in client.StreamTicksAsync(cts.Token))
            {
                received.Add(t);
                if (received.Count >= 1) break;
            }
        }
        catch (OperationCanceledException) { }

        await client.DisposeAsync();

        received.Should().HaveCount(1);
        received[0].RawData.Should().Be(payload, "fragments must be reassembled into the original message");
    }

    [Fact]
    public async Task ReceivesMultipleMessages_EachFromOwnFragments()
    {
        var msg1 = """{"ticker":"BTC","price":1,"volume":1,"ts":1}""";
        var msg2 = """{"ticker":"ETH","price":2,"volume":2,"ts":2}""";

        await using var server = FragmentingWebSocketServer.Start(
            new[] { msg1, msg2 },
            new[] { 8, 8, 200 });

        var client = new EchoClient(
            NullLogger.Instance,
            new ExchangeClientOptions
            {
                Uri = server.Uri,
                InitialReconnectDelay = TimeSpan.FromMilliseconds(50)
            });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var received = new List<RawTick>();

        try
        {
            await foreach (var t in client.StreamTicksAsync(cts.Token))
            {
                received.Add(t);
                if (received.Count >= 2) break;
            }
        }
        catch (OperationCanceledException) { }

        await client.DisposeAsync();

        received.Should().HaveCount(2);
        received[0].RawData.Should().Be(msg1);
        received[1].RawData.Should().Be(msg2);
    }
}
