# tiny-event-bus

**Zero-dependency in-process pub/sub event bus for Python**

[![version](https://img.shields.io/badge/version-0.3.0-blue)](#)
[![python](https://img.shields.io/badge/python-3.9+-green)](#)
[![license](https://img.shields.io/badge/license-MIT-orange)](#)
[![dependencies](https://img.shields.io/badge/dependencies-0-brightgreen)](#)

---

## Why?

Most event systems force a tradeoff:

| Approach | Problem |
|---|---|
| `redis pub/sub` | Needs a Redis server; not in-process |
| `asyncio.Event` | Single consumer only; no topic routing |
| `blinker` | No wildcards, no DLQ, no async-first |
| `pypubsub` | Heavy; complex API; not zero-dep |
| `custom observer` | Reinvented badly every time |

`tiny-event-bus` gives you a production-quality pub/sub hub with zero external
dependencies, targeting AI agent architectures, plugin systems, and
inter-component communication. It ships with a dead-letter queue with replay,
message redelivery with retry limits, wildcard topics, one-time subscriptions,
bulk publishing, and runtime stats.

## Use Cases

### AI Agent Tool Events

```python
bus.subscribe("tool.call", log_tool_call)
bus.subscribe("tool.result", log_tool_result)
bus.subscribe("agent.error", alert_on_error)

bus.publish("tool.call", {"tool": "web_search", "args": {"query": "weather"}})
```

### Inter-Component Communication

```python
# Plugin system
bus.subscribe("plugin.loaded", register_plugin_handlers)
bus.publish("plugin.loaded", {"name": "github-integration", "version": "1.2"})

# UI updates
bus.subscribe("order.updated", refresh_order_panel)
bus.subscribe("order.updated", update_badge_count)
```

### Inter-Agent Messaging (multi-agent systems)

```python
bus.subscribe("agent.1.response", lambda p: bus.publish("agent.2.input", p))
bus.publish("agent.1.response", {"content": "分析完成", "next_action": "summarize"})
```

## Features

| Feature | Supported |
|---|---|
| Topic-based subscriptions | ✅ |
| Wildcard topics (`user.*`, `TopicWildcard`) | ✅ |
| Sync publish (`publish`) | ✅ |
| Async publish (`publish_async`) | ✅ |
| Dead-letter queue (`DeadLetterQueue`) | ✅ |
| DLQ replay (`bus.replay`, `dlq.replay_all`) | ✅ |
| Message redelivery (`Message.redeliver`, `max_retries`) | ✅ |
| Redelivery delay scheduling | ✅ |
| One-time subscriptions (`once=True`, `bus.once()`) | ✅ |
| Bulk publishing (`BulkPublisher`) | ✅ |
| Runtime stats (`bus.stats()`) | ✅ |
| Middleware hooks | ✅ |
| Priority ordering | ✅ |
| Event filtering (stop propagation) | ✅ |
| Full type hints | ✅ |
| Zero external dependencies | ✅ |
| Python 3.9+ | ✅ |

## Quick Start

```bash
pip install tiny-event-bus
```

```python
from tiny_event_bus import EventBus

bus = EventBus()

@bus.subscribe("order.created")
def on_order(order):
    print(f"Order {order['id']} confirmed!")

bus.publish("order.created", {"id": 123, "total": 99.99})
# Output: Order 123 confirmed!
```

## Architecture

```
Publisher / BulkPublisher
   │
   ▼
┌────────────────────────────────┐
│            EventBus            │
│                                │
│  ┌──────────────────────────┐  │
│  │  Topic Index             │  │
│  │  "order.created" → [sub] │  │
│  │  "user.*"      → [sub]   │  │
│  └──────────────────────────┘  │
│                                │
│  Middleware Hooks              │
│  ├─ on_publish(event)         │
│  ├─ on_subscribe(sub)          │
│  └─ on_unsubscribe(sub)        │
│                                │
│  DeadLetterQueue               │
│  ├─ failed handler routing     │
│  ├─ replay / replay_all        │
│  └─ clear                      │
└────────────────────────────────┘
   │
   ▼
 ┌──────────────┐
 │  Handlers    │
 │  (priority   │
 │   ordered)   │
 └──────────────┘
```

## API Reference

### `EventBus(dlq_handler, on_publish, on_subscribe, on_unsubscribe)`

Create a new bus. All arguments are optional.

```python
bus = EventBus(
    dlq_handler=lambda error, event: log(error),
    on_publish=lambda event: trace(event.topic),
    on_subscribe=lambda sub: registry.add(sub),
)
```

### `bus.subscribe(topic, handler, *, once=False, priority=0)`

Register a handler. Returns a `Subscription` object.

```python
sub = bus.subscribe("user.*", on_user_event, once=True, priority=10)
```

- `topic` — exact string, `*` wildcard pattern (single dot-separated segment),
  or a `TopicWildcard` object
- `once` — auto-unsubscribes after first fire
- `priority` — higher values run first

### `bus.once(topic, handler, **kwargs)`

Subscribe for exactly one message then auto-unsubscribe. Equivalent to
`subscribe(..., once=True)` regardless of caller-supplied `once`.

```python
@bus.once("app.started")
def on_start(payload):
    print("Started once")
```

### `bus.unsubscribe(topic, handler_or_subscription)`

Remove a handler. Accepts the handler function or a `Subscription` object.

### `bus.publish(topic, payload, *, stop_on_false=True, delay=0.0)`

Synchronous publish. Optionally blocks for `delay` seconds before dispatch (to
honour `Message.redeliver(delay=...)`). Returns `True`.

### `bus.publish_async(topic, payload, *, stop_on_false=True, delay=0.0)`

Async publish. Properly `await`s async handlers; honours `delay` via
`asyncio.sleep`.

### `Message` — redelivery & retries

```python
from tiny_event_bus import Message

def handler(payload):
    try:
        risky(payload)
    except TransientError:
        msg = payload if isinstance(payload, Message) else Message("topic", payload)
        retry = msg.redeliver(delay=2.0)      # increment attempts, delay 2s
        if retry is not None:
            bus.publish(retry)                # re-dispatch after honoring delay
```

- `Message(topic, payload, max_retries=3)` — configurable retry budget
- `msg.redeliver(delay=0.0)` — returns a copy with one more attempt; `None`
  once `max_retries` is hit
- `msg.attempts`, `msg.remaining_retries`, `msg.delay`

### `DeadLetterQueue`

Failed handler invocations are routed here instead of being dropped.

```python
bus.dlq                    # DeadLetterQueue
len(bus.dlq)               # queue size
for e in bus.dlq.entries:  # DLQEntry(error, event, handler, key)
    print(e.key, e.topic, e.error)

bus.dlq.replay("dlq-1@...")            # replay one (original handler)
bus.dlq.replay_all()                   # replay everything
bus.dlq.clear()                        # empty the queue
```

### `bus.replay(dlq_key, handler=None)`

Replay a single dead-letter message directly on the bus. If no handler is
given it re-runs the original handler. Removes the entry on success.

```python
bus.replay("dlq-1@...")
# or with a retry-safe handler
bus.replay("dlq-1@...", handler=safer_handler)
```

### `TopicWildcard`

Declare wildcard patterns in a validated way.

```python
from tiny_event_bus import TopicWildcard

pattern = TopicWildcard("user", "*")
bus.subscribe(pattern, on_user_event)     # same as bus.subscribe("user.*", ...)
pattern.matches("user.created")           # True
```

### `BulkPublisher`

Efficiently buffer and publish many events.

```python
from tiny_event_bus import BulkPublisher

bulk = BulkPublisher(bus)
bulk.publish("a", 1).publish("a", 2).publish("b", 3)
bulk.flush()          # 3 — delivers all buffered events
bulk.pending          # 0 after flush

# optionally dedupe by topic (last payload wins)
bulk = BulkPublisher(bus, dedup_topics=True)
```

Every event is delivered on the final flush with a single pass, reducing
per-event overhead in hot loops.

### `bus.stats()`

Return a JSON string (or dict with `json_dump=False`) of runtime stats:

```python
import json
stats = json.loads(bus.stats())
# {
#   "version": "0.3.0",
#   "topics": {"count": 3, "handlers_per_topic": {...}},
#   "subscriptions": {"total": 5, "active": 5},
#   "publish": {"published": 12, "delivered": 9},
#   "dlq": {"size": 1, "keys": [...]}
# }
```

### `bus.topics()` / `bus.subscribers(topic)` / `bus.handler_count(topic)`

Introspection utilities.

## License

MIT — free for personal and commercial use.
