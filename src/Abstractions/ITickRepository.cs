using Domain;

namespace Abstractions;

public interface ITickRepository
{
    Task SaveBatchAsync(IReadOnlyList<Tick> ticks, CancellationToken ct);
}
