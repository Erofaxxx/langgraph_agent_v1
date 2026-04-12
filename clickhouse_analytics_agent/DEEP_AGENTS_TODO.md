# Deep Agents TODO (LangGraph + LangChain)

- [x] Добавить фокус-контекст по таблицам: роутер+скиллы выбирают релевантные таблицы в системный промпт.
- [x] Сохранить кэширование промпта для Anthropic через OpenRouter (`cache_control` + provider pinning).
- [x] Добавить память ошибок по сессии (lessons learned), чтобы агент не повторял частые промахи.
- [x] Сохранить текущий sandbox Python с гарантированным захватом matplotlib/seaborn графиков.
- [ ] Подключить LangChain Deep Agents runtime как опциональный orchestrator (feature flag).
- [ ] Вынести файловые операции в отдельный sandbox skill/tool с явной политикой доступа к ФС.
- [ ] Добавить planner/todo tool для явного пошагового плана внутри длинных задач.
- [ ] Добавить unit/integration тесты для table-focus роутинга и error-memory.
