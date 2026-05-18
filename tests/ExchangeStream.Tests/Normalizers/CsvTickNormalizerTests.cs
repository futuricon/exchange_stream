using Abstractions;
using FluentAssertions;
using Infrastructure.Normalizers;
using Xunit;

namespace ExchangeStream.Tests.Normalizers;

public class CsvTickNormalizerTests
{
    private readonly CsvTickNormalizer _sut = new();

    [Fact]
    public void CanHandle_OnlyCsv() =>
        _sut.CanHandle("csv").Should().BeTrue();

    [Fact]
    public void Normalize_ValidCsv_ReturnsTick()
    {
        var raw = new RawTick("ExchangeB", "ETHUSDT,3200.50,1.25,1716000000000", "csv");
        var tick = _sut.Normalize(raw);

        tick.Should().NotBeNull();
        tick!.Ticker.Should().Be("ETHUSDT");
        tick.Price.Should().Be(3200.50m);
        tick.Volume.Should().Be(1.25m);
        tick.Source.Should().Be("ExchangeB");
    }

    [Theory]
    [InlineData("")]
    [InlineData("BTC,1,2")] // too few fields
    [InlineData(",1,1,1716000000000")] // empty ticker
    [InlineData("BTC,abc,1,1716000000000")] // bad price
    [InlineData("BTC,-1,1,1716000000000")] // negative price
    [InlineData("BTC,1,-1,1716000000000")] // negative volume
    [InlineData("BTC,1,1,abc")] // bad timestamp
    public void Normalize_InvalidInput_ReturnsNull(string rawData)
    {
        var raw = new RawTick("ExchangeB", rawData, "csv");
        _sut.Normalize(raw).Should().BeNull();
    }

    [Fact]
    public void Normalize_RespectsInvariantCulture()
    {
        // Use dot as decimal separator regardless of system locale
        var raw = new RawTick("ExchangeB", "BTC,65000.50,1.25,1716000000000", "csv");
        _sut.Normalize(raw)!.Price.Should().Be(65000.50m);
    }
}
