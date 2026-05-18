using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Abstractions;
using Domain;
using Microsoft.Extensions.Logging;

namespace Infrastructure.Normalizers;

public sealed class JsonTickNormalizer : ITickNormalizer
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNameCaseInsensitive = true,
        NumberHandling = JsonNumberHandling.AllowReadingFromString
    };

    private readonly ILogger<JsonTickNormalizer> _logger;

    public JsonTickNormalizer(ILogger<JsonTickNormalizer> logger) => _logger = logger;

    public bool CanHandle(string format) =>
        string.Equals(format, "json", StringComparison.OrdinalIgnoreCase);

    public Tick? Normalize(RawTick raw)
    {
        try
        {
            var dto = JsonSerializer.Deserialize<JsonTickDto>(raw.RawData, Options);
            if (dto is null) return null;
            if (string.IsNullOrWhiteSpace(dto.Ticker)) return null;
            if (dto.Price <= 0 || dto.Volume < 0) return null;
            if (dto.Ts <= 0) return null;

            return new Tick
            {
                Ticker = dto.Ticker.ToUpperInvariant(),
                Price = dto.Price,
                Volume = dto.Volume,
                Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(dto.Ts),
                Source = raw.ExchangeName
            };
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Failed to parse JSON tick from {Source}", raw.ExchangeName);
            return null;
        }
    }

    private sealed record JsonTickDto(string? Ticker, decimal Price, decimal Volume, long Ts);
}
