using Infrastructure.Persistence;
using Infrastructure.Processing;

namespace App;

public sealed class PipelineHostedService : BackgroundService
{
    private readonly TickPipeline _pipeline;
    private readonly PostgresTickRepository _repository;
    private readonly ILogger<PipelineHostedService> _logger;

    public PipelineHostedService(
        TickPipeline pipeline,
        PostgresTickRepository repository,
        ILogger<PipelineHostedService> logger)
    {
        _pipeline = pipeline;
        _repository = repository;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _repository.InitializeAsync(stoppingToken).ConfigureAwait(false);

        _logger.LogInformation("Pipeline starting");
        try
        {
            await _pipeline.RunAsync(stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Pipeline stopped");
        }
    }
}
