using Abstractions;
using Microsoft.Extensions.Logging;

namespace Infrastructure.Clients;

public sealed class ExchangeAClient : ExchangeClientBase
{
    public ExchangeAClient(ILogger<ExchangeAClient> logger, ExchangeClientOptions options)
        : base(logger, options)
    {
    }

    public override string ExchangeName => "ExchangeA";

    protected override RawTick? ParseMessage(string message)
    {
        if (string.IsNullOrWhiteSpace(message)) return null;
        return new RawTick(ExchangeName, message, "json");
    }
}
