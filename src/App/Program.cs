using Abstractions;
using App;
using Infrastructure.Clients;
using Infrastructure.Normalizers;
using Infrastructure.Persistence;
using Infrastructure.Processing;
using Serilog;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSerilog(config => config
    .ReadFrom.Configuration(builder.Configuration)
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] [{SourceContext}] {Message:lj}{NewLine}{Exception}"));

var config = builder.Configuration;
var connectionString = config.GetConnectionString("Postgres")
    ?? throw new InvalidOperationException("Missing ConnectionStrings:Postgres");

builder.Services.AddSingleton<ITickNormalizer, JsonTickNormalizer>();
builder.Services.AddSingleton<ITickNormalizer, CsvTickNormalizer>();

builder.Services.AddSingleton<HashSetDeduplicator>();
builder.Services.AddSingleton<IDeduplicator>(sp => sp.GetRequiredService<HashSetDeduplicator>());

builder.Services.AddSingleton(sp => new PostgresTickRepository(
    connectionString,
    sp.GetRequiredService<ILogger<PostgresTickRepository>>()));
builder.Services.AddSingleton<ITickRepository>(sp => sp.GetRequiredService<PostgresTickRepository>());

builder.Services.AddSingleton<IReadOnlyList<IExchangeClient>>(sp =>
{
    var clients = new List<IExchangeClient>();

    var uriA = config["Exchanges:A:Uri"];
    if (!string.IsNullOrWhiteSpace(uriA))
    {
        clients.Add(new ExchangeAClient(
            sp.GetRequiredService<ILogger<ExchangeAClient>>(),
            new ExchangeClientOptions { Uri = new Uri(uriA) }));
    }

    var uriB = config["Exchanges:B:Uri"];
    if (!string.IsNullOrWhiteSpace(uriB))
    {
        clients.Add(new ExchangeBClient(
            sp.GetRequiredService<ILogger<ExchangeBClient>>(),
            new ExchangeClientOptions { Uri = new Uri(uriB) }));
    }

    if (clients.Count == 0)
        throw new InvalidOperationException("No exchange clients configured");

    return clients;
});

builder.Services.AddSingleton(sp => new TickPipeline(
    sp.GetRequiredService<IReadOnlyList<IExchangeClient>>(),
    sp.GetServices<ITickNormalizer>().ToList(),
    sp.GetRequiredService<IDeduplicator>(),
    sp.GetRequiredService<ITickRepository>(),
    sp.GetRequiredService<ILogger<TickPipeline>>()));

builder.Services.AddHostedService<PipelineHostedService>();

var host = builder.Build();
await host.RunAsync().ConfigureAwait(false);
