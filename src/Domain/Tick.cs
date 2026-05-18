namespace Domain;

public sealed record Tick
{
    public required string Ticker { get; init; }
    public required decimal Price { get; init; }
    public required decimal Volume { get; init; }
    public required DateTimeOffset Timestamp { get; init; }
    public required string Source { get; init; }

    public string DeduplicationKey =>
        $"{Source}:{Ticker}:{Timestamp.ToUnixTimeMilliseconds()}";
}
