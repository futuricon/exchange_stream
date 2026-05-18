using Abstractions;
using Dapper;
using Domain;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Infrastructure.Persistence;

public sealed class PostgresTickRepository : ITickRepository
{
    private const string SchemaSql = """
        CREATE TABLE IF NOT EXISTS raw_ticks (
            id          BIGSERIAL PRIMARY KEY,
            ticker      VARCHAR(20)     NOT NULL,
            price       NUMERIC(20, 8)  NOT NULL,
            volume      NUMERIC(20, 8)  NOT NULL,
            timestamp   TIMESTAMPTZ     NOT NULL,
            source      VARCHAR(50)     NOT NULL,
            created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
        );
        CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_ticks_source_ticker_ts
            ON raw_ticks (source, ticker, timestamp);
        CREATE INDEX IF NOT EXISTS ix_raw_ticks_ticker_ts
            ON raw_ticks (ticker, timestamp DESC);
        """;

    private const string InsertSql = """
        INSERT INTO raw_ticks (ticker, price, volume, timestamp, source)
        SELECT * FROM unnest(
            @Tickers::varchar[],
            @Prices::numeric[],
            @Volumes::numeric[],
            @Timestamps::timestamptz[],
            @Sources::varchar[])
        ON CONFLICT (source, ticker, timestamp) DO NOTHING
        """;

    private readonly string _connectionString;
    private readonly ILogger<PostgresTickRepository> _logger;

    public PostgresTickRepository(string connectionString, ILogger<PostgresTickRepository> logger)
    {
        _connectionString = connectionString;
        _logger = logger;
    }

    public async Task InitializeAsync(CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);
        await conn.ExecuteAsync(new CommandDefinition(SchemaSql, cancellationToken: ct))
            .ConfigureAwait(false);
        _logger.LogInformation("Postgres schema initialized");
    }

    public async Task SaveBatchAsync(IReadOnlyList<Tick> ticks, CancellationToken ct)
    {
        if (ticks.Count == 0) return;

        var tickers = new string[ticks.Count];
        var prices = new decimal[ticks.Count];
        var volumes = new decimal[ticks.Count];
        var timestamps = new DateTime[ticks.Count];
        var sources = new string[ticks.Count];

        for (int i = 0; i < ticks.Count; i++)
        {
            var t = ticks[i];
            tickers[i] = t.Ticker;
            prices[i] = t.Price;
            volumes[i] = t.Volume;
            timestamps[i] = t.Timestamp.UtcDateTime;
            sources[i] = t.Source;
        }

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);

        var affected = await conn.ExecuteAsync(new CommandDefinition(InsertSql,
            new { Tickers = tickers, Prices = prices, Volumes = volumes, Timestamps = timestamps, Sources = sources },
            cancellationToken: ct)).ConfigureAwait(false);

        _logger.LogDebug("Inserted {Affected}/{Total} ticks", affected, ticks.Count);
    }
}
