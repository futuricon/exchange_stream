# ExchangeStream

Имитация системы сбора, нормализации, дедупликации и пакетной записи биржевых тиков от 2–3 источников через WebSocket. Целевая нагрузка — 50–100 тиков/сек суммарно.

## Что делает

1. Параллельно подключается к нескольким WebSocket endpoint'ам.
2. Получает котировки в разных форматах (JSON, CSV).
3. Нормализует к единой модели [`Tick`](src/Domain/Tick.cs).
4. Дедуплицирует по ключу `Source:Ticker:Timestamp_ms` через скользящее окно (1 мин).
5. Собирает батчи и записывает в Postgres через Dapper с массивами и `unnest`.
6. При обрыве соединения — экспоненциальный backoff с decorrelated jitter (AWS-style).

## Структура

```
ExchangeStream.slnx
├── src/
│   ├── Domain/          ← Tick (record)
│   ├── Abstractions/    ← IExchangeClient, ITickNormalizer, IDeduplicator, ITickRepository, RawTick
│   ├── Infrastructure/  ← WS clients, normalizers, deduplicator, Postgres repo, TickPipeline
│   └── App/             ← Composition root, DI, BackgroundService, Serilog
├── tests/ExchangeStream.Tests/
└── docs/
    ├── INTERVIEW_PREP.md   ← Глубокая подготовка (CLR internals, SOLID, perf)
    ├── TRICKY_QA.md        ← 65+ вопросов с ответами
    └── MOCK_INTERVIEW.md   ← 3 mock-сценария интервью
```

## Запуск

Требования: **.NET 10 SDK**, **Docker**.

```powershell
docker-compose up -d
dotnet build ExchangeStream.slnx
dotnet test ExchangeStream.slnx
dotnet run --project src/App
```

Приложение попытается подключиться к WebSocket endpoint'ам из [`appsettings.json`](src/App/appsettings.json) (по умолчанию `ws://localhost:8001/stream`, `ws://localhost:8002/stream`). Если серверов нет — будут видны лог reconnect с растущим backoff.

Каждые 10 секунд выводится статистика:

```
Stats: processed=1500 duplicates=12 invalid=3 unknownFormat=0 persistenceErrors=0 rawQ=4 normQ=0
```

## Проверка данных в БД

```sql
SELECT source, COUNT(*) FROM raw_ticks GROUP BY source;
SELECT * FROM raw_ticks ORDER BY timestamp DESC LIMIT 10;
```

## Архитектурные решения

| Решение                        | Альтернатива                 | Почему                                                   |
| ------------------------------ | ---------------------------- | -------------------------------------------------------- |
| `Channel<T>` (bounded)         | `BlockingCollection`, events | async-friendly, backpressure, no allocations на hot path |
| `IAsyncEnumerable` из клиентов | events/delegates             | composable, cancellation, backpressure                   |
| Template Method base class     | composition + delegate       | reconnect-логика общая, parsing — специфичный            |
| Dapper + `unnest`              | EF Core                      | hot path: ×3–5 быстрее, нет change tracking overhead     |
| Двойное скользящее окно        | single window + reset        | нет дубликатов на стыке окон                             |
| Bounded channel `Wait`         | `DropOldest`                 | требование «не терять» → backpressure до WS              |
| Multi-project solution         | один проект                  | DIP enforced на уровне ассембли (compile-time)           |
| Decorrelated jitter            | constant interval            | AWS-recommended, избегает thundering herd                |

## Тесты

34 теста (unit + integration), включая:

- `HashSetDeduplicatorTests` — sliding window, thread-safety (1000 параллельных TryAdd).
- `JsonTickNormalizerTests` / `CsvTickNormalizerTests` — валидные и невалидные входы, edge cases (BOM, разные culture).
- `TickPipelineTests` — end-to-end с fake-клиентами и in-memory repo.
- `ExchangeClientBaseFragmentTests` — **критическая защита от регрессии**: реальный WebSocket-сервер шлёт сообщение тремя фрагментами, клиент должен собрать в одно.
