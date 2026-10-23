"""
Tests for tiny_event_bus.

Run with:
    python -m pytest test_event_bus.py -v
    python test_event_bus.py  # standalone runner
"""

import asyncio
import sys

from tiny_event_bus import EventBus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def test_basic_subscribe_publish():
    """Subscribing and publishing works with exact topic match."""
    bus = EventBus()
    received = []

    def handler(payload):
        received.append(payload)

    bus.subscribe("order.created", handler)
    bus.publish("order.created", {"id": 1})
    assert received == [{"id": 1}], f"Expected payload, got {received}"


def test_multiple_handlers():
    """Multiple handlers on the same topic all fire."""
    bus = EventBus()
    calls = []

    bus.subscribe("topic", lambda p: calls.append("a"))
    bus.subscribe("topic", lambda p: calls.append("b"))
    bus.publish("topic", None)
    assert calls == ["a", "b"], f"Expected [a, b], got {calls}"


def test_unsubscribe_by_handler():
    """Unsubscribing by passing the handler removes it."""
    bus = EventBus()
    calls = []

    def handler(p):
        calls.append(p)

    bus.subscribe("topic", handler)
    bus.publish("topic", 1)
    assert calls == [1]

    bus.unsubscribe("topic", handler)
    bus.publish("topic", 2)
    assert calls == [1], "Handler should have been removed"


def test_unsubscribe_all():
    """Unsubscribing by topic only removes all handlers for that topic."""
    bus = EventBus()
    calls = []

    bus.subscribe("topic", lambda p: calls.append(1))
    bus.subscribe("topic", lambda p: calls.append(2))
    bus.publish("topic", None)
    assert calls == [1, 2]

    bus.unsubscribe("topic")
    bus.publish("topic", None)
    assert calls == [1, 2], "All handlers should be removed"


def test_unsubscribe_subscription_object():
    """Unsubscribe accepts a Subscription object returned from subscribe."""
    bus = EventBus()
    calls = []

    def handler(p):
        calls.append(p)

    sub = bus.subscribe("topic", handler)
    bus.publish("topic", 1)
    bus.unsubscribe("topic", sub)
    bus.publish("topic", 2)
    assert calls == [1]


def test_wildcard_single_level():
    """Pattern 'user.*' matches 'user.created' and 'user.deleted'."""
    bus = EventBus()
    calls = []

    bus.subscribe("user.*", lambda p: calls.append(p))
    bus.publish("user.created", {"action": "created"})
    bus.publish("user.deleted", {"action": "deleted"})
    assert len(calls) == 2
    assert calls[0]["action"] == "created"
    assert calls[1]["action"] == "deleted"


def test_wildcard_does_not_match_multi_level():
    """Pattern 'a.*' does NOT match 'a.b.c' (single-level wildcard only)."""
    bus = EventBus()
    calls = []

    bus.subscribe("a.*", lambda p: calls.append(p))
    bus.publish("a.b.c", {"x": 1})
    assert calls == [], "Single-level wildcard should not match 'a.b.c'"


def test_no_match():
    """Publishing to a topic with no subscribers is a no-op (no error)."""
    bus = EventBus()
    bus.publish("nonexistent", {"data": 1})  # Should not raise


def test_dlq_on_exception():
    """Handler that raises an exception routes to DLQ."""
    dlq_entries = []

    def dlq_handler(error, event):
        dlq_entries.append((error, event))

    bus = EventBus(dlq_handler=dlq_handler)

    def failing_handler(_):
        raise RuntimeError("boom")

    bus.subscribe("critical", failing_handler)
    bus.publish("critical", {"data": 1})

    assert len(dlq_entries) == 1
    error, event = dlq_entries[0]
    assert isinstance(error, RuntimeError)
    assert error.args[0] == "boom"
    assert event.topic == "critical"
    assert event.payload == {"data": 1}


def test_dlq_accumulates():
    """DLQ grows with each failed handler; does not stop other handlers."""
    dlq_size = []

    def dlq_handler(error, event):
        dlq_size.append(event.topic)

    bus = EventBus(dlq_handler=dlq_handler)
    calls = []

    def fail(_):
        raise ValueError("fail")

    def ok(p):
        calls.append(p)

    bus.subscribe("topic", fail)
    bus.subscribe("topic", ok)
    bus.publish("topic", "payload")

    assert dlq_size == ["topic"]
    assert calls == ["payload"]


def test_dlq_property():
    """The dlq property exposes all accumulated entries."""
    bus = EventBus()

    def fail(_):
        raise RuntimeError("x")

    bus.subscribe("topic", fail)
    bus.publish("topic", None)

    assert len(bus.dlq) == 1
    assert isinstance(bus.dlq[0].error, RuntimeError)


def test_clear_dlq():
    """clear_dlq removes all DLQ entries."""
    bus = EventBus()

    def fail(_):
        raise RuntimeError("x")

    bus.subscribe("topic", fail)
    bus.publish("topic", None)
    assert len(bus.dlq) == 1
    bus.clear_dlq()
    assert len(bus.dlq) == 0


def test_once_subscription():
    """Handler with once=True fires exactly once then auto-unsubscribes."""
    bus = EventBus()
    calls = []

    @bus.subscribe("ping", once=True)
    def pong(_):
        calls.append(1)

    bus.publish("ping", None)
    bus.publish("ping", None)
    bus.publish("ping", None)
    assert calls == [1], f"Expected exactly 1 call, got {calls}"


def test_priority_order():
    """Higher priority handlers execute first."""
    bus = EventBus()
    order = []

    bus.subscribe("topic", lambda _: order.append("low"), priority=0)
    bus.subscribe("topic", lambda _: order.append("high"), priority=10)
    bus.subscribe("topic", lambda _: order.append("medium"), priority=5)

    bus.publish("topic", None)
    assert order == ["high", "medium", "low"], f"Expected [high, medium, low], got {order}"


def test_stop_propagation_on_false():
    """Handler returning False stops propagation to lower-priority handlers."""
    bus = EventBus()
    calls = []

    def stop(_):
        calls.append("stop")
        return False

    def cont(_):
        calls.append("cont")

    bus.subscribe("topic", stop, priority=10)
    bus.subscribe("topic", cont, priority=5)
    bus.publish("topic", None, stop_on_false=True)
    assert calls == ["stop"], f"Expected only [stop], got {calls}"


def test_continue_when_no_false():
    """Handlers still run if stop_on_false=False even when a handler returns False."""
    bus = EventBus()
    calls = []

    def stop(_):
        calls.append("stop")
        return False

    def cont(_):
        calls.append("cont")

    bus.subscribe("topic", stop, priority=10)
    bus.subscribe("topic", cont, priority=5)
    bus.publish("topic", None, stop_on_false=False)
    assert calls == ["stop", "cont"], f"Expected [stop, cont], got {calls}"


def test_middleware_on_publish():
    """on_publish hook is called for every publish."""
    events = []

    def hook(event):
        events.append(event.topic)

    bus = EventBus(on_publish=hook)
    bus.publish("a", None)
    bus.publish("b", None)
    assert events == ["a", "b"]


def test_middleware_on_subscribe():
    """on_subscribe hook is called when a handler subscribes."""
    subs = []

    def hook(sub):
        subs.append(sub.topic)

    bus = EventBus(on_subscribe=hook)
    bus.subscribe("topic1", lambda _: None)
    bus.subscribe("topic2", lambda _: None)
    assert subs == ["topic1", "topic2"]


def test_middleware_on_unsubscribe():
    """on_unsubscribe hook is called when a handler unsubscribes."""
    unsubs = []

    def hook(sub):
        unsubs.append(sub.topic)

    bus = EventBus(on_unsubscribe=hook)

    def handler(_):
        pass

    sub = bus.subscribe("topic", handler)
    bus.unsubscribe("topic", sub)
    assert unsubs == ["topic"]


def test_async_handler_with_publish_async():
    """publish_async awaits async handlers correctly."""
    bus = EventBus()
    results = []

    async def async_h(payload):
        await asyncio.sleep(0.001)
        results.append(payload)

    bus.subscribe("topic", async_h)

    async def run():
        await bus.publish_async("topic", "hello")

    asyncio.run(run())
    assert results == ["hello"]


def test_sync_handler_in_publish_async():
    """publish_async calls sync handlers directly without awaiting."""
    bus = EventBus()
    results = []

    def sync_h(payload):
        results.append(payload)

    bus.subscribe("topic", sync_h)

    async def run():
        await bus.publish_async("topic", "sync-value")

    asyncio.run(run())
    assert results == ["sync-value"]


def test_coroutine_closed_on_sync_publish():
    """Sync publish on a coroutine handler closes it without awaiting."""
    bus = EventBus()
    closed = []

    async def async_h(_):
        await asyncio.sleep(10)  # would hang if awaited
        return "done"

    bus.subscribe("topic", async_h)
    bus.publish("topic", None)  # should not hang; should close the coroutine
    closed.append("ok")  # if we get here, test passes
    assert closed == ["ok"]


def test_introspection_topics():
    """topics() returns all registered patterns."""
    bus = EventBus()
    bus.subscribe("a", lambda _: None)
    bus.subscribe("b.*", lambda _: None)
    assert set(bus.topics()) == {"a", "b.*"}


def test_introspection_subscribers():
    """subscribers() returns matching subscriptions."""
    bus = EventBus()
    bus.subscribe("topic", lambda _: None)
    bus.subscribe("topic", lambda _: None, priority=10)
    subs = bus.subscribers("topic")
    assert len(subs) == 2
    assert subs[0].priority == 10  # highest priority first


def test_introspection_handler_count():
    """handler_count returns count for exact topic."""
    bus = EventBus()
    bus.subscribe("topic", lambda _: None)
    bus.subscribe("topic", lambda _: None)
    bus.subscribe("other", lambda _: None)
    assert bus.handler_count("topic") == 2
    assert bus.handler_count("other") == 1
    assert bus.handler_count("nonexistent") == 0


def test_subscribe_returns_subscription():
    """subscribe() returns a Subscription object usable for later unsubscribe."""
    bus = EventBus()
    calls = []

    def h(_):
        calls.append(1)

    sub = bus.subscribe("topic", h)
    bus.publish("topic", None)
    assert calls == [1]

    # unsubscribe using the sub object
    bus.unsubscribe("topic", sub)
    bus.publish("topic", None)
    assert calls == [1], "Should not fire after unsubscribe"


def test_global_bus_singleton():
    """get_global_bus returns the same instance."""
    from tiny_event_bus import get_global_bus

    bus1 = get_global_bus()
    bus2 = get_global_bus()
    assert bus1 is bus2


def test_module_level_publish_subscribe():
    """Module-level publish/subscribe helpers work."""
    from tiny_event_bus import publish, subscribe, unsubscribe, get_global_bus

    bus = get_global_bus()
    bus._subscriptions.clear()  # reset global state for isolation

    results = []

    def h(p):
        results.append(p)

    subscribe("module.topic", h)
    publish("module.topic", "hello")
    assert results == ["hello"]

    unsubscribe("module.topic")
    publish("module.topic", "ignored")
    assert results == ["hello"]


def test_event_repr():
    """Event has a readable repr."""
    from tiny_event_bus import Event

    e = Event("topic", {"key": "value"})
    assert "topic" in repr(e)
    assert "value" in repr(e)


def test_bus_repr():
    """EventBus has a readable repr."""
    bus = EventBus()
    bus.subscribe("a", lambda _: None)
    assert "EventBus" in repr(bus)


# ---------------------------------------------------------------------------
# New v0.3.0 features
# ---------------------------------------------------------------------------

def test_dead_letter_queue_class():
    """bus.dlq is a DeadLetterQueue instance with entries."""
    from tiny_event_bus import DeadLetterQueue
    bus = EventBus()
    assert isinstance(bus.dlq, DeadLetterQueue)

    def fail(_):
        raise RuntimeError("boom")

    bus.subscribe("topic", fail)
    bus.publish("topic", None)
    assert len(bus.dlq) == 1
    entry = bus.dlq[0]
    assert entry.topic == "topic"
    assert entry.key.startswith("dlq-")
    assert isinstance(entry.error, RuntimeError)


def test_dlq_replay_success():
    """Replaying a DLQ message with a fix re-runs and removes the entry."""
    bus = EventBus()
    calls = []

    def crashing(p):
        if p == "bad":
            raise ValueError("bad payload")
        calls.append(p)

    bus.subscribe("topic", crashing)
    bus.publish("topic", "bad")
    assert len(bus.dlq) == 1
    key = bus.dlq[0].key

    # Replay with a fixing handler
    ok = bus.replay(key, handler=lambda p: calls.append(p))
    assert ok is True
    assert len(bus.dlq) == 0
    assert calls == ["bad"]


def test_dlq_replay_failure_keeps_entry():
    """Replay that fails again keeps the entry and returns False."""
    bus = EventBus()

    def fail(_):
        raise RuntimeError("still bad")

    bus.subscribe("topic", fail)
    bus.publish("topic", None)
    key = bus.dlq[0].key

    ok = bus.replay(key)
    assert ok is False
    assert len(bus.dlq) == 1  # still present


def test_dlq_replay_all():
    """replay_all returns a mapping of key -> success and clears successes."""
    bus = EventBus()

    def fail(_):
        raise RuntimeError("x")

    bus.subscribe("a", fail)
    bus.subscribe("b", fail)
    bus.publish("a", None)
    bus.publish("b", None)
    assert len(bus.dlq) == 2

    results = bus.dlq.replay_all(handler=lambda _: None)
    assert len(results) == 2
    assert all(results.values())
    assert len(bus.dlq) == 0


def test_message_redeliver():
    """Message.redeliver increments attempts and respects max_retries."""
    from tiny_event_bus import Message
    msg = Message("topic", {"k": 1}, max_retries=2)
    assert msg.attempts == 1
    assert msg.remaining_retries == 2

    r1 = msg.redeliver()
    assert r1 is not None
    assert r1.attempts == 2
    assert r1.remaining_retries == 1

    r2 = r1.redeliver()
    assert r2 is not None
    assert r2.attempts == 3
    assert r2.remaining_retries == 0

    r3 = r2.redeliver()
    assert r3 is None  # retry budget exhausted


def test_message_redeliver_delay():
    """Message.redeliver attaches a delay honoured on publish."""
    from tiny_event_bus import Message
    bus = EventBus()
    received = []

    def handler(p):
        received.append(p)

    bus.subscribe("topic", handler)
    msg = Message("topic", 42)
    retry = msg.redeliver(delay=0.001)
    bus.publish(retry)  # topic=Message
    assert received == [42]


def test_message_publish_direct():
    """Publishing a Message object directly dispatches its topic and payload."""
    from tiny_event_bus import Message
    bus = EventBus()
    got = []

    def handler(p):
        got.append(p)

    bus.subscribe("direct.topic", handler)
    bus.publish(Message("direct.topic", "hello"))
    assert got == ["hello"]


def test_topic_wildcard_object():
    """TopicWildcard constructs a valid pattern and matches topics."""
    from tiny_event_bus import TopicWildcard
    tw = TopicWildcard("user", "*")
    assert tw.pattern == "user.*"
    assert tw.matches("user.created")
    assert tw.matches("user.deleted")
    assert not tw.matches("order.created")
    assert not tw.matches("user.profile.updated")

    # from_string
    tw2 = TopicWildcard.from_string("order.*")
    assert tw2.pattern == "order.*"



def test_topic_wildcard_subscribe():
    """Subscribing with a TopicWildcard routes matching events."""
    from tiny_event_bus import TopicWildcard
    bus = EventBus()
    got = []
    bus.subscribe(TopicWildcard("user", "*"), lambda p: got.append(p))
    bus.publish("user.created", {"a": 1})
    bus.publish("user.deleted", {"b": 2})
    bus.publish("other", {"x": 1})
    assert len(got) == 2


def test_event_bus_once_method():
    """bus.once(topic, handler) fires exactly once."""
    bus = EventBus()
    calls = []

    @bus.once("ping")
    def handler(_):
        calls.append(1)

    bus.publish("ping", None)
    bus.publish("ping", None)
    bus.publish("ping", None)
    assert calls == [1]


def test_event_bus_stats():
    """stats() returns JSON with queue depths, handler counts, DLQ size."""
    import json
    bus = EventBus()
    bus.subscribe("a", lambda _: None)
    bus.subscribe("a", lambda _: None)
    bus.subscribe("b.*", lambda _: None)
    bus.publish("a", None)

    stats = json.loads(bus.stats())
    assert stats["version"] == "0.3.0"
    assert stats["topics"]["count"] == 2
    assert stats["topics"]["handlers_per_topic"]["a"] == 2
    assert stats["subscriptions"]["total"] == 3
    assert stats["publish"]["published"] == 1
    assert stats["dlq"]["size"] == 0

    # dict mode
    stats_dict = bus.stats(json_dump=False)
    assert isinstance(stats_dict, dict)


def test_bulk_publisher():
    """BulkPublisher buffers and flushes events efficiently."""
    from tiny_event_bus import BulkPublisher
    bus = EventBus()
    got = []
    bus.subscribe("a", lambda p: got.append(("a", p)))
    bus.subscribe("b", lambda p: got.append(("b", p)))

    bulk = BulkPublisher(bus)
    bulk.publish("a", 1).publish("a", 2).publish("b", 3)
    assert bulk.pending == 3
    assert len(bulk) == 3

    flushed = bulk.flush()
    assert flushed == 3
    assert bulk.pending == 0
    assert got == [("a", 1), ("a", 2), ("b", 3)]


def test_bulk_publisher_dedup():
    """BulkPublisher with dedup_topics keeps the last payload per topic."""
    from tiny_event_bus import BulkPublisher
    bus = EventBus()
    got = []
    bus.subscribe("a", lambda p: got.append(p))

    bulk = BulkPublisher(bus, dedup_topics=True)
    bulk.publish("a", 1).publish("a", 2).publish("a", 3)
    assert bulk.pending == 3
    flushed = bulk.flush()
    assert flushed == 1
    assert got == [3]


def test_bulk_publisher_clear():
    """BulkPublisher.clear discards buffered events."""
    from tiny_event_bus import BulkPublisher
    bus = EventBus()
    got = []
    bus.subscribe("a", lambda p: got.append(p))

    bulk = BulkPublisher(bus)
    bulk.publish("a", 1)
    bulk.clear()
    assert bulk.pending == 0
    assert bulk.flush() == 0
    assert got == []


# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------

def run_tests():
    tests = [
        test_basic_subscribe_publish,
        test_multiple_handlers,
        test_unsubscribe_by_handler,
        test_unsubscribe_all,
        test_unsubscribe_subscription_object,
        test_wildcard_single_level,
        test_wildcard_does_not_match_multi_level,
        test_no_match,
        test_dlq_on_exception,
        test_dlq_accumulates,
        test_dlq_property,
        test_clear_dlq,
        test_once_subscription,
        test_priority_order,
        test_stop_propagation_on_false,
        test_continue_when_no_false,
        test_middleware_on_publish,
        test_middleware_on_subscribe,
        test_middleware_on_unsubscribe,
        test_async_handler_with_publish_async,
        test_sync_handler_in_publish_async,
        test_coroutine_closed_on_sync_publish,
        test_introspection_topics,
        test_introspection_subscribers,
        test_introspection_handler_count,
        test_subscribe_returns_subscription,
        test_global_bus_singleton,
        test_module_level_publish_subscribe,
        test_event_repr,
        test_bus_repr,
        # New v0.3.0 features
        test_dead_letter_queue_class,
        test_dlq_replay_success,
        test_dlq_replay_failure_keeps_entry,
        test_dlq_replay_all,
        test_message_redeliver,
        test_message_redeliver_delay,
        test_message_publish_direct,
        test_topic_wildcard_object,
        test_topic_wildcard_subscribe,
        test_event_bus_once_method,
        test_event_bus_stats,
        test_bulk_publisher,
        test_bulk_publisher_dedup,
        test_bulk_publisher_clear,
    ]

    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            print(f"  PASS  {test.__name__}")
            passed += 1
        except Exception as exc:
            print(f"  FAIL  {test.__name__}: {exc}")
            failed += 1

    print(f"\n{passed}/{passed+failed} tests passed")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    print("tiny_event_bus tests")
    print("=" * 50)
    run_tests()
