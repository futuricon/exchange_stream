using System.Globalization;
using Abstractions;
using Domain;

namespace Infrastructure.Normalizers;

public sealed class CsvTickNormalizer : ITickNormalizer
{
    public bool CanHandle(string format) =>
        string.Equals(format, "csv", StringComparison.OrdinalIgnoreCase);

    public Tick? Normalize(RawTick raw)
    {
        var parts = raw.RawData.Split(',', StringSplitOptions.TrimEntries);
        if (parts.Length < 4) return null;

        var ticker = parts[0];
        if (string.IsNullOrWhiteSpace(ticker)) return null;

        if (!decimal.TryParse(parts[1], NumberStyles.Float, CultureInfo.InvariantCulture, out var price) || price <= 0)
            return null;
        if (!decimal.TryParse(parts[2], NumberStyles.Float, CultureInfo.InvariantCulture, out var volume) || volume < 0)
            return null;
        if (!long.TryParse(parts[3], NumberStyles.Integer, CultureInfo.InvariantCulture, out var tsMs) || tsMs <= 0)
            return null;

        return new Tick
        {
            Ticker = ticker.ToUpperInvariant(),
            Price = price,
            Volume = volume,
            Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(tsMs),
            Source = raw.ExchangeName
        };
    }
}
