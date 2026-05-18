namespace Abstractions;

public interface IDeduplicator
{
    bool TryAdd(string key);
}
