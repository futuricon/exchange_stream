namespace Infrastructure.Clients;

public sealed record ExchangeClientOptions
{
    public required Uri Uri { get; init; }
    public TimeSpan InitialReconnectDelay { get; init; } = TimeSpan.FromSeconds(1);
    public TimeSpan MaxReconnectDelay { get; init; } = TimeSpan.FromSeconds(60);
}
