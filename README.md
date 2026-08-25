# tiny-event-bus

**Zero-dependency in-process pub/sub event bus for Python**

[![version](https://img.shields.io/badge/version-0.1.0-blue)](#)
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

`tiny-event-bus` gives you a production-quality pub/sub hub with zero external dependencies, targeting AI agent architectures, plugin systems, and inter-component communication.

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
| Wildcard topics (`user.*`) | ✅ |
| Sync publish | ✅ |
| Async publish (`publish_async`) | ✅ |
| Dead-letter queue (DLQ) | ✅ |
| Middleware hooks | ✅ |
| Priority ordering | ✅ |
| Once-only subscriptions | ✅ |
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
Publisher
   │
   ▼
┌─────────────────────────────┐
│         EventBus            │
│                              │
│  ┌────────────────────────┐  │
│  │  Topic Index           │  │
│  │  "order.created" → [sub]│  │
│  │  "user.*"      → [sub]  │  │
│  └────────────────────────┘  │
│                              │
│  Middleware Hooks            │
│  ├─ on_publish(event)       │
│  ├─ on_subscribe(sub)       │
│  └─ on_unsubscribe(sub)     │
│                              │
│  Dead-Letter Queue           │
│  └─ Failed handler log       │
└─────────────────────────────┘
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

- `topic` — exact string or `*` wildcard (single dot-separated segment)
- `once` — auto-unsubscribes after first fire
- `priority` — higher values run first

### `bus.unsubscribe(topic, handler_or_subscription)`

Remove a handler. Accepts the handler function or a `Subscription` object.

### `bus.publish(topic, payload, *, stop_on_false=True)`

Synchronous publish. Returns `True`.

### `bus.publish_async(topic, payload, *, stop_on_false=True)`

Async publish. Properly `await`s async handlers.

### `bus.dlq`

Read-only list of `DLQEntry(error, event, handler)` objects.

### `bus.topics()` / `bus.subscribers(topic)` / `bus.handler_count(topic)`

Introspection utilities.

## License

MIT — free for personal and commercial use.
