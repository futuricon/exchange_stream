using Abstractions;
using FluentAssertions;
using Infrastructure.Normalizers;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace ExchangeStream.Tests.Normalizers;

public class JsonTickNormalizerTests
{
    private readonly JsonTickNormalizer _sut = new(NullLogger<JsonTickNormalizer>.Instance);

    [Theory]
    [InlineData("json", true)]
    [InlineData("JSON", true)]
    [InlineData("csv", false)]
    [InlineData("", false)]
    public void CanHandle_RespectsCaseInsensitiveJsonFormat(string format, bool expected)
        => _sut.CanHandle(format).Should().Be(expected);

    [Fact]
    public void Normalize_ValidJson_ReturnsTick()
    {
        var raw = new RawTick("ExchangeA",
            """{"ticker":"btcusdt","price":65000.5,"volume":1.25,"ts":1716000000000}""",
            "json");

        var tick = _sut.Normalize(raw);

        tick.Should().NotBeNull();
        tick!.Ticker.Should().Be("BTCUSDT");
        tick.Price.Should().Be(65000.5m);
        tick.Volume.Should().Be(1.25m);
        tick.Source.Should().Be("ExchangeA");
        tick.Timestamp.Should().Be(DateTimeOffset.FromUnixTimeMilliseconds(1716000000000));
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("")]
    [InlineData("""{"ticker":"","price":1,"volume":1,"ts":1}""")]
    [InlineData("""{"ticker":"BTC","price":0,"volume":1,"ts":1}""")]
    [InlineData("""{"ticker":"BTC","price":-1,"volume":1,"ts":1}""")]
    [InlineData("""{"ticker":"BTC","price":1,"volume":-0.1,"ts":1}""")]
    [InlineData("""{"ticker":"BTC","price":1,"volume":1,"ts":0}""")]
    public void Normalize_InvalidInput_ReturnsNull(string rawData)
    {
        var raw = new RawTick("ExchangeA", rawData, "json");
        _sut.Normalize(raw).Should().BeNull();
    }

    [Fact]
    public void Normalize_NumberAsString_IsAccepted()
    {
        var raw = new RawTick("ExchangeA",
            """{"ticker":"BTC","price":"65000.5","volume":"1.25","ts":1716000000000}""",
            "json");

        _sut.Normalize(raw).Should().NotBeNull();
    }
}
