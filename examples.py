"""
tiny_event_bus usage examples.

Run this file directly to see all examples in action:
    python examples.py
"""

from tiny_event_bus import EventBus

# ---------------------------------------------------------------------------
# Example 1: Basic pub/sub
# ---------------------------------------------------------------------------


def example_basic():
    print("\n=== Example 1: Basic Pub/Sub ===")
    bus = EventBus()

    @bus.subscribe("order.created")
    def send_confirmation(order):
        print(f"  -> Order {order['id']} confirmed! Total: ${order['total']}")

    @bus.subscribe("order.created")
    def log_order(order):
        print(f"  -> [LOG] New order: {order}")

    bus.publish("order.created", {"id": 123, "total": 99.99})

    print("  Subscribers on 'order.created':", bus.handler_count("order.created"))


# ---------------------------------------------------------------------------
# Example 2: Wildcard topics
# ---------------------------------------------------------------------------


def example_wildcards():
    print("\n=== Example 2: Wildcard Topics ===")
    bus = EventBus()

    @bus.subscribe("user.*")
    def on_user_event(event):
        print(f"  -> [user.*] {event}")

    @bus.subscribe("*.created")
    def on_created(event):
        print(f"  -> [*.created] {event}")

    bus.publish("user.created", {"user_id": 1, "name": "Alice"})
    bus.publish("user.deleted", {"user_id": 1})
    bus.publish("order.created", {"order_id": 99})  # matches *.created, not user.*
    bus.publish("product.updated", {"product_id": 2})  # matches nothing


# ---------------------------------------------------------------------------
# Example 3: Dead Letter Queue
# ---------------------------------------------------------------------------


def example_dlq():
    print("\n=== Example 3: Dead Letter Queue ===")
    dlq_log: list[dict] = []

    def dlq_handler(error, event):
        dlq_log.append({"topic": event.topic, "payload": event.payload, "error": str(error)})
        print(f"  -> [DLQ] Error: {error} | Event: {event}")

    bus = EventBus(dlq_handler=dlq_handler)

    @bus.subscribe("critical.operation")
    def failing_handler(evt):
        raise RuntimeError("something went wrong")

    @bus.subscribe("critical.operation")
    def succeeding_handler(evt):
        print(f"  -> [critical] Handled: {evt}")

    bus.publish("critical.operation", {"data": 42})
    print(f"  DLQ log: {dlq_log}")


# ---------------------------------------------------------------------------
# Example 4: AI Agent Tool Events
# ---------------------------------------------------------------------------


def example_agent_events():
    print("\n=== Example 4: AI Agent Tool Events ===")
    bus = EventBus()
    tool_log: list[dict] = []

    def log_tool_call(payload):
        tool_log.append({"type": "call", **payload})
        print(f"  -> [tool.call] {payload['tool']} → {payload.get('args', {})}")

    def log_tool_result(payload):
        tool_log.append({"type": "result", **payload})
        print(f"  -> [tool.result] {payload['tool']} ← {payload.get('result', {})}")

    def alert_on_error(payload):
        print(f"  -> [agent.error] ALERT: {payload['message']}")

    bus.subscribe("tool.call", log_tool_call)
    bus.subscribe("tool.result", log_tool_result)
    bus.subscribe("agent.error", alert_on_error)

    bus.publish("tool.call", {"tool": "web_search", "args": {"query": "weather"}})
    bus.publish("tool.result", {"tool": "web_search", "result": {"temp": 72}})
    bus.publish("agent.error", {"message": "Rate limit exceeded", "retry_after": 30})

    print(f"  Tool log ({len(tool_log)} entries):", tool_log)


# ---------------------------------------------------------------------------
# Example 5: Once subscriptions
# ---------------------------------------------------------------------------


def example_once():
    print("\n=== Example 5: Once-Only Subscriptions ===")
    bus = EventBus()
    counter = 0

    @bus.subscribe("ping", once=True)
    def one_time_pong(payload):
        nonlocal counter
        counter += 1
        print(f"  -> Pong received (count: {counter})")

    bus.publish("ping", {})
    bus.publish("ping", {})
    bus.publish("ping", {})
    print(f"  Handler fired {counter} time(s) despite 3 publishes (once=True)")


# ---------------------------------------------------------------------------
# Example 6: Priority ordering
# ---------------------------------------------------------------------------


def example_priority():
    print("\n=== Example 6: Priority Ordering ===")
    bus = EventBus()
    call_order: list[str] = []

    @bus.subscribe("process", priority=0)
    def low_priority(_):
        call_order.append("low")
        print("  -> [priority=0] low")

    @bus.subscribe("process", priority=10)
    def high_priority(_):
        call_order.append("high")
        print("  -> [priority=10] high")

    @bus.subscribe("process", priority=5)
    def medium_priority(_):
        call_order.append("medium")
        print("  -> [priority=5] medium")

    bus.publish("process", {})
    print(f"  Call order: {call_order} (should be high → medium → low)")


# ---------------------------------------------------------------------------
# Example 7: Event filtering (stop propagation)
# ---------------------------------------------------------------------------


def example_filtering():
    print("\n=== Example 7: Event Filtering / Stop Propagation ===")
    bus = EventBus()
    calls: list[str] = []

    @bus.subscribe("gate", priority=10)
    def gatekeeper(payload):
        calls.append("gatekeeper")
        print("  -> [priority=10] Gatekeeper says: STOP")
        return False  # Stop propagation

    @bus.subscribe("gate", priority=5)
    def downstream(_):
        calls.append("downstream")
        print("  -> [priority=5] Downstream called (should NOT run)")

    bus.publish("gate", {}, stop_on_false=True)
    print(f"  Calls made: {calls} (downstream should be absent)")


# ---------------------------------------------------------------------------
# Example 8: Middleware hooks
# ---------------------------------------------------------------------------


def example_middleware():
    print("\n=== Example 8: Middleware Hooks ===")
    events_seen: list[str] = []
    subs_log: list[str] = []

    def on_publish(event):
        events_seen.append(event.topic)
        print(f"  -> [on_publish hook] Topic: {event.topic}")

    def on_subscribe(sub):
        subs_log.append(sub.topic)
        print(f"  -> [on_subscribe hook] {sub.topic}")

    bus = EventBus(on_publish=on_publish, on_subscribe=on_subscribe)

    @bus.subscribe("app.started")
    def on_start(_):
        print("  -> App started!")

    bus.publish("app.started", {})
    print(f"  All published topics: {events_seen}")
    print(f"  All subscribed topics: {subs_log}")


# ---------------------------------------------------------------------------
# Example 9: Async handlers
# ---------------------------------------------------------------------------


def example_async():
    import asyncio

    print("\n=== Example 9: Async Handlers ===")
    bus = EventBus()
    results: list[str] = []

    async def async_handler(payload):
        await asyncio.sleep(0.01)  # Simulate async work
        results.append(f"async:{payload['value']}")
        print(f"  -> [async] Handled {payload['value']}")

    def sync_handler(payload):
        results.append(f"sync:{payload['value']}")
        print(f"  -> [sync] Handled {payload['value']}")

    bus.subscribe("data.*", sync_handler)
    bus.subscribe("data.*", async_handler)

    async def run():
        await bus.publish_async("data.ready", {"value": 42})
        print(f"  Results: {results}")

    asyncio.run(run())


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("tiny-event-bus examples")
    print("=" * 50)
    example_basic()
    example_wildcards()
    example_dlq()
    example_agent_events()
    example_once()
    example_priority()
    example_filtering()
    example_middleware()
    example_async()
    print("\n" + "=" * 50)
    print("All examples complete!")
