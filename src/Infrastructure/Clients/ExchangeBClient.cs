using Abstractions;
using Microsoft.Extensions.Logging;

namespace Infrastructure.Clients;

public sealed class ExchangeBClient : ExchangeClientBase
{
    public ExchangeBClient(ILogger<ExchangeBClient> logger, ExchangeClientOptions options)
        : base(logger, options)
    {
    }

    public override string ExchangeName => "ExchangeB";

    protected override RawTick? ParseMessage(string message)
    {
        if (string.IsNullOrWhiteSpace(message)) return null;
        return new RawTick(ExchangeName, message, "csv");
    }
}
