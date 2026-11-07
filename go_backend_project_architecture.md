# Архитектура Go-проекта

## Совместный редактор документов

**Назначение документа:** практическая карта репозитория Go backend.
Здесь описано, какие пакеты создать, за что они отвечают, кто от кого
зависит и как проходит запрос. Документ дополняет техническую
спецификацию v3.1 и не заменяет её.

## 1. Принцип организации

Для MVP используем **модульный монолит**. Один Go-процесс, один
deployable binary, одна SQLite база. Внутри код разделён по
бизнес-областям и техническим адаптерам.

Не начинать с микросервисов, Kafka, Redis-кластера и десятков
абстрактных слоёв. Они не добавят корректности CRDT, зато добавят
способы сломать запуск.

Основные правила:

1.  `cmd/server` только собирает приложение и запускает его.
2.  `internal` содержит приватную реализацию backend.
3.  Бизнес-логика не зависит от HTTP, WebSocket или SQLite.
4.  SQL и конкретный SQLite driver находятся в persistence/store
    adapters.
5.  `DocActor` --- единственный последовательный владелец
    runtime-состояния одного документа.
6.  WebSocket reader/writer не выполняют бизнес-логику документа.
7.  Все очереди ограничены по размеру.
8.  Между пакетами передаются типизированные команды/интерфейсы, а не
    универсальные `map[string]any`.
9.  Не создавать интерфейс для каждой структуры. Интерфейс нужен на
    границе, где есть реальная смена реализации или изоляция теста.

## 2. Предлагаемая структура репозитория

``` text
collab-docs/
├── cmd/
│   └── server/
│       └── main.go                 # точка входа: config -> app -> run
│
├── internal/
│   ├── app/
│   │   ├── app.go                  # сборка зависимостей и lifecycle
│   │   └── shutdown.go             # graceful shutdown / drain
│   │
│   ├── config/
│   │   ├── config.go               # структура конфигурации
│   │   ├── env.go                  # чтение env
│   │   └── validate.go             # проверка конфигурации
│   │
│   ├── domain/
│   │   ├── room.go                 # Room, RoomID
│   │   ├── document.go             # Document, DocumentID
│   │   ├── session.go              # Session, SessionID, Role, Scope
│   │   ├── invite.go               # InviteLink, InviteScope
│   │   └── errors.go               # доменные ошибки
│   │
│   ├── room/
│   │   ├── service.go              # создание/получение комнаты
│   │   ├── repository.go           # интерфейс хранения комнат
│   │   └── access.go               # проверка доступа к комнате
│   │
│   ├── document/
│   │   ├── service.go              # создание/получение/attach документа
│   │   ├── repository.go           # метаданные и связь room_documents
│   │   └── access.go               # проверка document scope
│   │
│   ├── invite/
│   │   ├── service.go              # создание/проверка/отзыв ссылок
│   │   ├── token.go                # генерация секрета и hash
│   │   └── repository.go            # интерфейс invite storage
│   │
│   ├── session/
│   │   ├── service.go              # создание, срок жизни, revoke
│   │   ├── token.go                # подпись/проверка cookie token
│   │   ├── repository.go           # серверные записи сессий
│   │   └── identity.go             # серверная identity для соединения
│   │
│   ├── httpapi/
│   │   ├── router.go               # маршруты и middleware
│   │   ├── response.go             # единый формат ошибок/ответов
│   │   ├── rooms.go                # HTTP handlers комнат
│   │   ├── documents.go            # HTTP handlers документов
│   │   ├── sessions.go             # вход по invite и cookie
│   │   └── middleware/
│   │       ├── requestid.go
│   │       ├── recover.go
│   │       ├── logging.go
│   │       └── auth.go
│   │
│   ├── gateway/
│   │   ├── websocket.go            # upgrade, Origin, cookie, limits
│   │   ├── connection.go           # lifecycle одного WS соединения
│   │   ├── reader.go               # чтение и decode кадров
│   │   ├── writer.go               # единственный writer в socket
│   │   └── registry.go             # регистрация connection -> document
│   │
│   ├── protocol/
│   │   ├── frame.go                # типы и структура кадров
│   │   ├── codec.go                # бинарное кодирование/декодирование
│   │   ├── validate.go             # лимиты и проверка полей
│   │   └── testdata/               # golden vectors с frontend
│   │
│   ├── docruntime/
│   │   ├── manager.go              # загрузка/поиск/выгрузка активных документов
│   │   ├── actor.go                 # последовательный event loop документа
│   │   ├── events.go                # типизированные события и команды
│   │   ├── clients.go               # участники и connection handles
│   │   ├── lifecycle.go             # ACTIVE/IDLE/FLUSHING/UNLOADED
│   │   └── limits.go                # лимиты документа и очередей
│   │
│   ├── crdt/
│   │   ├── engine.go                # интерфейс CRDT Engine
│   │   ├── yjsrelay/                # только если выбран relay режим
│   │   └── yjsengine/               # адаптер проверенной Go/sidecar реализации
│   │
│   ├── presence/
│   │   ├── awareness.go             # awareness payload / validation
│   │   ├── manager.go               # ephemeral presence per document
│   │   └── throttle.go              # coalescing/rate limit
│   │
│   ├── persistence/
│   │   ├── writer.go                # bounded queue + group commit coordinator
│   │   ├── batch.go                 # batching and retry policy
│   │   ├── ack.go                   # durable ACK result mapping
│   │   └── recovery.go              # snapshot + updates recovery flow
│   │
│   ├── snapshot/
│   │   ├── worker.go                # snapshot scheduling and validation
│   │   └── hash.go                  # SHA-256 content hash
│   │
│   ├── store/
│   │   └── sqlite/
│   │       ├── db.go                # open DB, pragmas, pool config
│   │       ├── migrations/
│   │       ├── rooms.go
│   │       ├── documents.go
│   │       ├── invites.go
│   │       ├── sessions.go
│   │       ├── updates.go
│   │       └── snapshots.go
│   │
│   ├── observability/
│   │   ├── metrics.go
│   │   ├── logging.go
│   │   └── tracing.go              # optional, only if actually used
│   │
│   └── health/
│       ├── handlers.go             # /healthz and /readyz
│       └── checks.go
│
├── migrations/                     # optional alternative if migrations are external
├── protocol/
│   └── collab.v1.md                # human-readable wire contract
├── web/                            # React + TypeScript frontend
├── tools/
│   └── loadgen/                    # load generator / test clients
├── deploy/
│   ├── Dockerfile
│   └── compose.yml
├── go.mod
├── go.sum
└── README.md
```

**Примечание:** не нужно создавать все файлы пустыми заранее. Структура
--- целевая карта. На каждом этапе добавляются только пакеты,
необходимые вертикальному срезу.

## 3. Ответственность пакетов

### `cmd/server`

Только composition root. Загружает конфиг, создаёт logger, открывает БД,
запускает миграции, создаёт репозитории и сервисы, собирает HTTP server,
запускает его и обрабатывает shutdown.

Здесь не должно быть SQL, маршрутов, правил доступа или логики CRDT.

### `internal/app`

Собирает зависимости и управляет жизненным циклом приложения. Знает, в
каком порядке запускать и останавливать компоненты. При shutdown
включает drain mode и ждёт завершения принятых операций.

### `internal/domain`

Чистые типы и правила предметной области. Не импортирует другие
внутренние пакеты, SQL driver, HTTP или WebSocket библиотеки.

Примеры типов: `RoomID`, `DocumentID`, `SessionID`, `InviteScope`,
`Role`, `DocumentMeta`.

### `room`, `document`, `invite`, `session`

Прикладные сервисы. Они реализуют сценарии и правила доступа, но не
знают о конкретном HTTP framework и SQL driver.

-   `room`: создать комнату, получить метаданные, перечислить документы.
-   `document`: создать документ, получить метаданные, прикрепить
    существующий документ к комнате.
-   `invite`: создать ссылку, проверить секрет, истёкший срок и отзыв.
-   `session`: создать анонимную сессию, назначить серверные имя/цвет,
    подписать cookie claims, проверить и отозвать сессию.

### `httpapi`

Тонкий транспортный слой. Парсит HTTP, валидирует входные DTO, вызывает
application services, сериализует результат. Не содержит SQL и не
управляет DocActor напрямую, кроме вызова публичного runtime API при
необходимости.

### `gateway`

Транспорт WebSocket. Отвечает за upgrade, проверку Origin/cookie, лимиты
кадра, read/write loops, ping/pong и закрытие соединения.

Для одного WebSocket соединения должен существовать ровно один writer.
Reader декодирует кадры и передаёт типизированные события в runtime.
Gateway не применяет CRDT updates и не пишет в БД.

### `protocol`

Независимый wire contract. Кодирует и декодирует бинарные кадры,
проверяет версии, типы, размеры и обязательные поля. Не знает о
комнатах, SQLite и сессиях.

`protocol/collab.v1.md` и golden vectors являются контрактом
frontend/backend. Любое изменение формата требует обновления версии или
явной совместимой эволюции.

### `docruntime`

Главный runtime-слой совместной работы.

`Manager` лениво загружает документ и гарантирует единственного
активного владельца на данном процессе. `Actor` последовательно
обрабатывает события документа: join, leave, update, awareness, sync,
persistence result, idle timeout.

Actor не должен блокироваться на медленной сети или диске. Отправка идёт
через ограниченные outbox, запись --- через persistence pipeline. Если
inbox/outbox заполнены, применяется backpressure или отключение клиента,
но CRDT update не теряется молча.

### `crdt`

Интерфейс над выбранным движком. Например:

``` go
type Engine interface {
    ApplyUpdate(update []byte) error
    EncodeState() ([]byte, error)
    StateVector() ([]byte, error)
    Diff(stateVector []byte) ([]byte, error)
}
```

Это эскиз контракта, а не обещание, что каждая реализация сможет
выполнять эти операции эффективно или вообще поддерживает серверный
state. Если выбран relay-only режим, интерфейс должен быть уже и явно
отражать ограничения. Не делать вид, что relay умеет вычислять state
vector и snapshot.

Выбор реализации блокируется M0 interop gate: настоящие Yjs клиенты
должны подтвердить совместимость, convergence, повторное применение,
state vector/diff и восстановление.

### `presence`

Эфемерное состояние пользователей в документе: имя, цвет, client ID,
cursor/selection и timeout. Не сохраняет presence в SQLite. Может
coalesce-ить частые курсорные события, поскольку промежуточные позиции
не являются durable edits.

### `persistence`

Получает updates от runtime, выполняет bounded batching, group commit и
сопоставляет результаты с `update_id`. Возвращает durable result только
после успешного COMMIT.

Пакет не должен отправлять WebSocket ACK напрямую. Он сообщает результат
runtime/координатору, а тот формирует ACK для нужного соединения.

### `snapshot`

Создаёт согласованные snapshots на известной границе `seq`, считает
hash, проверяет возможность чтения и восстановления. Не удаляет журнал
самостоятельно без отдельной политики компактизации.

### `store/sqlite`

Низкоуровневый адаптер SQLite. Содержит SQL, миграции, настройки WAL и
реализации repository-интерфейсов. Не содержит бизнес-правил и не
импортирует `httpapi` или `docruntime`.

### `observability`, `health`

Метрики, структурированные логи, pprof (закрытый admin surface),
liveness и readiness. Метрики не должны использовать document ID/session
ID как labels.

## 4. Направление зависимостей

``` text
cmd/server
    |
    v
internal/app -----------------------> config
    |
    +----> room / document / invite / session services
    |                 |                       |
    |                 v                       v
    |            domain types          repository interfaces
    |                                         ^
    |                                         |
    +----> httpapi --------------------> store/sqlite
    |
    +----> gateway -> protocol -> docruntime -> crdt
    |                                |  |
    |                                |  +----> presence
    |                                |
    |                                +-------> persistence -> store/sqlite
    |                                |
    |                                +-------> snapshot
    |
    +----> observability / health
```

Практическое правило: `store/sqlite` реализует интерфейсы, объявленные
рядом с потребителем (или в узком application package). Не заставлять
domain импортировать инфраструктуру.

Избегать циклических импортов. Если два пакета постоянно импортируют
друг друга, граница ответственности выбрана неправильно.

## 5. Главные интерфейсы между слоями

Примеры ориентировочных интерфейсов:

``` go
type RoomRepository interface {
    Create(ctx context.Context, room Room) error
    Get(ctx context.Context, id RoomID) (Room, error)
    ListDocuments(ctx context.Context, roomID RoomID) ([]DocumentMeta, error)
}

type DocumentRepository interface {
    Create(ctx context.Context, doc DocumentMeta) error
    Get(ctx context.Context, id DocumentID) (DocumentMeta, error)
    AttachToRoom(ctx context.Context, roomID RoomID, documentID DocumentID) error
    HasRoomLink(ctx context.Context, roomID RoomID, documentID DocumentID) (bool, error)
}

type UpdateStore interface {
    AppendBatch(ctx context.Context, batch []PersistedUpdate) ([]PersistResult, error)
    LoadRecoveryState(ctx context.Context, documentID DocumentID) (RecoveryState, error)
}

type Runtime interface {
    Join(ctx context.Context, documentID DocumentID, client ClientHandle) error
    Leave(ctx context.Context, documentID DocumentID, connectionID ConnectionID)
    Submit(ctx context.Context, documentID DocumentID, update IncomingUpdate) error
}
```

Это контракты для обсуждения. Не копировать их вслепую: сигнатуры
уточняются после фиксации протокола и выбранного CRDT Engine. Особенно
важно, чтобы `AppendBatch` возвращал результат по каждому `update_id`,
включая duplicate replay.

## 6. Потоки выполнения

### 6.1. Создание комнаты и приглашения

1.  HTTP handler получает запрос и валидирует DTO.
2.  `room.Service` создаёт Room через repository.
3.  `invite.Service` генерирует криптографически случайный секрет.
4.  В БД записывается hash секрета, scope, роль и expiry.
5.  Клиенту секрет возвращается один раз в URL.
6.  В БД никогда не сохраняется открытый токен приглашения.

### 6.2. Вход по ссылке

1.  Клиент отправляет секрет приглашения и отображаемое имя.
2.  `invite.Service` проверяет hash, expiry и revoke.
3.  `session.Service` создаёт session ID, нормализует имя, назначает
    цвет и scope.
4.  Запись сессии сохраняется.
5.  Сервер выставляет подписанную HttpOnly cookie.
6.  Последующие HTTP/WS запросы проверяют сессию и scope.

### 6.3. Подключение к документу

1.  WebSocket handler проверяет Origin и cookie.
2.  Access service подтверждает право на document ID.
3.  Gateway выполняет upgrade с подпротоколом `collab.v1`.
4.  Connection создаёт reader и writer.
5.  Runtime Manager загружает документ, если он ещё не активен.
6.  DocActor регистрирует клиента и упорядочивает sync handshake.
7.  Presence Manager добавляет участника.
8.  Reader передаёт входящие update/awareness события в Actor.
9.  Writer отправляет sync, broadcast, ACK и служебные кадры.

### 6.4. Приём и сохранение update

1.  Reader декодирует кадр и проверяет размер/формат.
2.  Runtime проверяет session identity, document scope и rate limits.
3.  Actor дедуплицирует `update_id` в пределах runtime (это оптимизация,
    не замена уникального ограничения БД).
4.  Update ставится в bounded persistence pipeline.
5.  После принятия pipeline Actor применяет update к Engine, если Engine
    это поддерживает, и рассылает update остальным клиентам.
6.  Writer сохраняет batch в SQLite транзакцией.
7.  Результат COMMIT связывается с каждым `update_id`.
8.  Runtime отправляет ACK исходному клиенту.
9.  Клиент удаляет update из IndexedDB только после соответствующего
    ACK.

Если persistence pipeline переполнен, сервер не должен подтверждать
update и не должен бесконтрольно продолжать принимать новые изменения.
Конкретная политика: приостановить чтение, вернуть retryable error или
закрыть соединение с кодом перегрузки.

### 6.5. Reconnect

1.  Клиент восстанавливает неподтверждённые updates из IndexedDB.
2.  Открывает WebSocket и выполняет sync handshake.
3.  Повторно отправляет неподтверждённые updates с исходными
    `update_id`.
4.  Сервер распознаёт уже записанные updates и возвращает прежние seq.
5.  Новые updates записываются один раз.
6.  Клиент удаляет только подтверждённые записи.

## 7. DocActor: модель событий

Actor должен принимать типизированные события примерно такого состава:

``` go
type Event interface{ isDocumentEvent() }

type JoinEvent struct { Client ClientHandle }
type LeaveEvent struct { ConnectionID ConnectionID }
type UpdateEvent struct { Update IncomingUpdate }
type AwarenessEvent struct { Payload []byte }
type PersistedEvent struct { Results []PersistResult }
type SyncRequestEvent struct { ClientID ClientID; StateVector []byte }
type IdleTimeoutEvent struct{}
```

Actor обрабатывает события последовательно. Не запускать отдельную
goroutine на каждое событие. Долгие операции делегируются workers и
возвращаются как completion events.

Нельзя давать внешним пакетам прямой доступ к внутреннему CRDT state.
Внешний API Actor должен быть командным, а не выдавать указатель на
mutable state.

## 8. Конфигурация

Минимальные переменные окружения:

``` text
APP_ENV=development
HTTP_ADDR=:8080
DATABASE_PATH=/data/collab.sqlite
SESSION_SIGNING_KEY=...
SESSION_TTL=24h
INVITE_DEFAULT_TTL=168h
WS_READ_LIMIT_BYTES=2097152
WS_WRITE_TIMEOUT=5s
WS_PING_INTERVAL=30s
DOC_INBOX_CAPACITY=...
CLIENT_OUTBOX_CAPACITY=...
PERSISTENCE_QUEUE_CAPACITY=...
PERSISTENCE_BATCH_SIZE=100
PERSISTENCE_FLUSH_INTERVAL=75ms
DOC_IDLE_TIMEOUT=90s
LOG_LEVEL=info
```

Секреты не коммитить в `.env` и не печатать в логах. В production ключ
подписи передаётся через secret management. Численные значения очередей
и лимитов являются стартовыми, их нужно подтвердить профилированием.

## 9. Что не должно происходить

-   HTTP handler не выполняет SQL напрямую.
-   WebSocket reader не пишет в SQLite.
-   WebSocket writer не вызывается параллельно из нескольких goroutine.
-   DocActor не ждёт завершения сетевой записи.
-   DocActor не держит глобальный mutex на все документы.
-   Presence не попадает в durable update journal.
-   Клиент не назначает себе серверное имя, цвет или роль.
-   ACK не отправляется до COMMIT.
-   Повторный `update_id` не создаёт вторую строку журнала.
-   Один SQLite файл не используется несколькими backend-инстансами
    через сетевую FS.
-   Relay-only CRDT режим не объявляется полноценным server-side Yjs
    engine.
-   Нельзя добавлять package `common`, `utils` или `helpers` как свалку
    несвязанных функций.

## 10. Порядок реализации по пакетам

### Этап 0. Проверка CRDT и протокола

Создать только: - `internal/protocol` - `internal/crdt` -
`protocol/collab.v1.md` - interop harness с реальным Yjs на Node.js

Проверить update exchange, duplicate delivery, reconnect, state vector,
diff, snapshot/recovery. До прохождения gate не строить остальную
систему вокруг непроверенной Go CRDT-библиотеки.

### Этап 1. Вертикальный срез без WebSocket

Создать: - `config`, `domain`, `store/sqlite` - `room`, `document`,
`invite`, `session` - `httpapi`, `app`

Результат: создать комнату, создать приглашение, войти анонимно, создать
документ, сохранить метаданные.

### Этап 2. Один документ, два клиента

Создать: - `gateway` - `docruntime` - `presence`

Результат: два клиента соединяются с одним документом, синхронизируют
изменения и видят курсоры.

### Этап 3. Durable persistence

Создать: - `persistence` - `snapshot` - IndexedDB очередь во frontend

Результат: ACK после COMMIT, reconnect с повторной отправкой,
восстановление после перезапуска.

### Этап 4. Устойчивость

Добавить bounded queues, backpressure, slow-consumer handling, graceful
shutdown, metrics, fuzz/race/load tests.

### Этап 5. История и кластер

Только после стабилизации MVP добавлять commits/diff/rollback,
PostgreSQL, durable broker, lease/fencing и document routing между
инстансами.

## 11. Минимальный Definition of Done

-   `go test ./...` проходит.
-   `go test -race ./...` проходит на ключевых сценариях.
-   Protocol golden tests проходят на Go и TypeScript.
-   Два и более клиента сходятся при конкурентном редактировании.
-   Новый клиент не теряет update, сделанный во время handshake.
-   Повторная отправка одного `update_id` создаёт одну запись.
-   ACK приходит только после COMMIT.
-   После перезапуска подтверждённые данные восстанавливаются.
-   Переполнение очереди не приводит к неограниченному росту памяти.
-   Медленный клиент не блокирует рассылку другим.
-   Сессия с отозванным или истёкшим приглашением не получает доступ.
-   Логи не содержат invite secrets, cookie и signing keys.

## 12. Открытые решения перед кодированием

1.  Выбран ли CRDT Engine и прошёл ли он M0 interop gate?
2.  Включает ли MVP прикрепление одного документа к нескольким комнатам,
    или требуется только перемещение/копирование?
3.  Нужны ли read-only приглашения уже в MVP?
4.  Какой формат rich-text структуры поддерживается: только TipTap
    schema или ограниченный набор nodes/marks?
5.  Какая политика удаления комнаты/документа и связанных приглашений?
6.  Нужна ли серверная сессия в SQLite с возможностью отзыва, или
    допустима короткоживущая stateless cookie? Рекомендация этого
    документа --- серверная запись сессии.
