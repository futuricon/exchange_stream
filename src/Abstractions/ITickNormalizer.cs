using Domain;

namespace Abstractions;

public interface ITickNormalizer
{
    bool CanHandle(string format);
    Tick? Normalize(RawTick raw);
}
