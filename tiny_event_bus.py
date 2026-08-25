"""
tiny_event_bus - Zero-dependency in-process pub/sub event bus for Python.

Supports topics with wildcards, dead-letter queue, middleware hooks,
async handlers, priority ordering, and one-time subscriptions.
"""

from __future__ import annotations

import asyncio
import typing
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

EventPayload = typing.Any
TopicName = str
Handler = Callable[[EventPayload], Any]
AsyncHandler = Callable[[EventPayload], Any]
MiddlewareHook = Callable[["Event"], None]
DLQHandler = Callable[[Exception, "Event"], None]


@dataclass
class Event:
    """Represents a published event."""

    topic: TopicName
    payload: EventPayload

    def __repr__(self) -> str:
        return f"Event(topic={self.topic!r}, payload={self.payload!r})"


@dataclass
class Subscription(Generic[T]):
    """A registered event handler."""

    topic: TopicName
    handler: Callable[[EventPayload], T]
    once: bool = False
    priority: int = 0
    active: bool = True
    _order: int = field(default=0, compare=True, repr=False)

    def __lt__(self, other: Subscription) -> bool:
        if self.priority != other.priority:
            return self.priority > other.priority
        return self._order < other._order

    def __call__(self, payload: EventPayload) -> T:
        """Allow Subscription to be used as a decorator shorthand."""
        return self.handler(payload)


@dataclass
class DLQEntry:
    """Entry in the dead-letter queue."""

    error: Exception
    event: Event
    handler: Callable[[EventPayload], Any]


# ---------------------------------------------------------------------------
# EventBus
# ---------------------------------------------------------------------------


class EventBus:
    """
    Central pub/sub hub for in-process event distribution.

    Features:
        - Topic-based subscriptions with single-level wildcard (``*``)
        - Sync and async publish
        - Dead-letter queue for failed handlers
        - Middleware hooks (on_publish, on_subscribe, on_unsubscribe)
        - Priority ordering (higher priority runs first)
        - One-time subscriptions (``once=True``)
        - Event filtering (handler returning ``False`` stops propagation)
        - Full type hints throughout

    Example:
        >>> bus = EventBus()
        >>> @bus.subscribe("order.created")
        ... def on_order(order):
        ...     print(f"Order {order['id']} confirmed!")
        >>> bus.publish("order.created", {"id": 123, "total": 99.99})
        Order 123 confirmed!
    """

    def __init__(
        self,
        dlq_handler: DLQHandler | None = None,
        on_publish: MiddlewareHook | None = None,
        on_subscribe: Callable[[Subscription], None] | None = None,
        on_unsubscribe: Callable[[Subscription], None] | None = None,
    ) -> None:
        self._subscriptions: dict[TopicName, list[Subscription[Any]]] = {}
        self._dlq: list[DLQEntry] = []
        self._dlq_handler: DLQHandler | None = dlq_handler
        self._on_publish: MiddlewareHook | None = on_publish
        self._on_subscribe: Callable[[Subscription], None] | None = on_subscribe
        self._on_unsubscribe: Callable[[Subscription], None] | None = on_unsubscribe
        self._insertion_counter: int = 0

    # -----------------------------------------------------------------------
    # Subscription management
    # -----------------------------------------------------------------------

    def subscribe(
        self,
        topic: TopicName,
        handler: Handler | AsyncHandler | None = None,
        *,
        once: bool = False,
        priority: int = 0,
    ) -> Subscription[Any] | Callable[[Handler | AsyncHandler], Subscription[Any]]:
        """
        Register ``handler`` to be called whenever ``topic`` is published.

        Can be used as a decorator or as a regular call.

        Args:
            topic: Exact topic name or pattern with ``*`` wildcard
                   (matches a single dot-separated segment).
            handler: Synchronous or asynchronous callable that accepts
                     the event payload and may return ``False`` to stop
                     propagation to lower-priority handlers. If omitted,
                     returns a decorator that applies the topic and options.
            once: If True, the handler fires at most once then auto-unsubscribes.
            priority: Higher values run first. Default priority is 0.

        Returns:
            The ``Subscription`` object, or a decorator if ``handler`` is omitted.

        Example (call):
            >>> sub = bus.subscribe("user.*", handler_func)

        Example (decorator):
            >>> @bus.subscribe("order.created")
            ... def on_order(order):
            ...     print(f"Order {order['id']} confirmed!")

        Example (decorator with options):
            >>> @bus.subscribe("ready", once=True)
            ... def once_handler(payload):
            ...     print("Runs only once!")
        """

        def _register(h: Handler | AsyncHandler) -> Subscription[Any]:
            self._insertion_counter += 1
            sub = Subscription(
                topic=topic,
                handler=h,
                once=once,
                priority=priority,
                active=True,
                _order=self._insertion_counter,
            )
            if topic not in self._subscriptions:
                self._subscriptions[topic] = []
            self._subscriptions[topic].append(sub)
            self._subscriptions[topic].sort()
            if self._on_subscribe:
                self._on_subscribe(sub)
            return sub

        if handler is None:
            # Decorator mode: @bus.subscribe("topic", once=True)
            return _register
        # Direct mode: bus.subscribe("topic", handler_func)
        return _register(handler)

    def unsubscribe(
        self,
        topic: TopicName,
        handler: Handler | AsyncHandler | Subscription | None = None,
    ) -> bool:
        """
        Remove a handler (or all handlers on a topic) from the subscription list.

        Pass either:
            - ``topic`` + ``handler`` to remove a specific handler
            - ``topic`` only to remove all handlers on that topic
            - A ``Subscription`` object previously returned by ``subscribe()``

        Returns:
            True if a handler was removed, False otherwise.
        """
        if isinstance(handler, Subscription):
            if handler.topic != topic:
                return False
            subs = self._subscriptions.get(topic, [])
            before = len(subs)
            self._subscriptions[topic] = [s for s in subs if s is not handler]
            removed = before > len(self._subscriptions[topic])
            if removed and self._on_unsubscribe:
                self._on_unsubscribe(handler)
            return removed

        if handler is None:
            if topic in self._subscriptions:
                subs = self._subscriptions.pop(topic, [])
                for s in subs:
                    if self._on_unsubscribe:
                        self._on_unsubscribe(s)
                return True
            return False

        subs = self._subscriptions.get(topic, [])
        before = len(subs)
        self._subscriptions[topic] = [
            s for s in subs if s.handler is not handler
        ]
        removed = before > len(self._subscriptions[topic])
        if removed:
            dead = next((s for s in subs if s.handler is handler), None)
            if dead and self._on_unsubscribe:
                self._on_unsubscribe(dead)
        return removed

    # -----------------------------------------------------------------------
    # Matching
    # -----------------------------------------------------------------------

    @staticmethod
    def _match_topic(topic: TopicName, pattern: TopicName) -> bool:
        """Return True if ``topic`` matches ``pattern`` (single-level wildcard)."""
        if "*" not in pattern:
            return topic == pattern
        t_parts = topic.split(".")
        p_parts = pattern.split(".")
        if len(t_parts) != len(p_parts):
            return False
        return all(tp == pp or pp == "*" for tp, pp in zip(t_parts, p_parts))

    def _collect_subscriptions(self, topic: TopicName) -> list[Subscription[Any]]:
        """Gather all subscriptions (including wildcard patterns) that match ``topic``."""
        collected: dict[int, Subscription[Any]] = {}
        for pattern, subs in self._subscriptions.items():
            if self._match_topic(topic, pattern):
                for sub in subs:
                    if sub.active:
                        collected[id(sub)] = sub
        # Sort by priority descending
        return sorted(collected.values(), key=lambda s: s.priority, reverse=True)

    # -----------------------------------------------------------------------
    # Sync publish
    # -----------------------------------------------------------------------

    def publish(
        self,
        topic: TopicName,
        payload: EventPayload,
        *,
        stop_on_false: bool = True,
    ) -> bool:
        """
        Deliver ``payload`` to all handlers subscribed to ``topic`` synchronously.

        Handlers are called in priority order (highest first). A handler that
        returns ``False`` will stop propagation to remaining handlers when
        ``stop_on_false`` is True (the default).

        If a handler raises an exception, it is caught and routed to the DLQ.
        All remaining handlers are still attempted.

        Args:
            topic: The event topic string.
            payload: Arbitrary data to pass to handlers.
            stop_on_false: If True, stop propagation when a handler returns False.

        Returns:
            True if all handlers succeeded without returning False.
        """
        event = Event(topic=topic, payload=payload)
        if self._on_publish:
            self._on_publish(event)

        subs = self._collect_subscriptions(topic)
        for sub in subs:
            if not sub.active:
                continue
            try:
                result = sub.handler(payload)
                # Support async handlers called from sync publish
                if asyncio.iscoroutine(result):
                    result.close()  # Don't await — fire-and-forget in sync mode
                if stop_on_false and result is False:
                    break
            except Exception as exc:  # noqa: BLE001
                self._route_to_dlq(exc, event, sub)

            if sub.once:
                sub.active = False
                self._remove_subscription(sub)

        return True

    def _remove_subscription(self, sub: Subscription[Any]) -> None:
        """Remove a specific subscription object from the registry."""
        for pattern, subs in self._subscriptions.items():
            if sub in subs:
                subs.remove(sub)
                if not subs:
                    del self._subscriptions[pattern]
                break

    # -----------------------------------------------------------------------
    # Async publish
    # -----------------------------------------------------------------------

    async def publish_async(
        self,
        topic: TopicName,
        payload: EventPayload,
        *,
        stop_on_false: bool = True,
    ) -> bool:
        """
        Asynchronously deliver ``payload`` to all matching handlers.

        Works like ``publish()`` but properly awaits async handlers.
        Sync handlers are called directly.

        Args:
            topic: The event topic string.
            payload: Arbitrary data to pass to handlers.
            stop_on_false: If True, stop propagation when a handler returns False.

        Returns:
            True if all handlers succeeded without returning False.
        """
        event = Event(topic=topic, payload=payload)
        if self._on_publish:
            self._on_publish(event)

        subs = self._collect_subscriptions(topic)
        for sub in subs:
            if not sub.active:
                continue
            try:
                result = sub.handler(payload)
                if asyncio.iscoroutine(result):
                    result = await result
                if stop_on_false and result is False:
                    break
            except Exception as exc:  # noqa: BLE001
                self._route_to_dlq(exc, event, sub)

            if sub.once:
                sub.active = False
                self._remove_subscription(sub)

        return True

    # -----------------------------------------------------------------------
    # Dead-letter queue
    # -----------------------------------------------------------------------

    def _route_to_dlq(
        self,
        error: Exception,
        event: Event,
        sub: Subscription[Any],
    ) -> None:
        """Route a failed handler invocation to the DLQ."""
        entry = DLQEntry(error=error, event=event, handler=sub.handler)
        self._dlq.append(entry)
        if self._dlq_handler:
            self._dlq_handler(error, event)

    @property
    def dlq(self) -> list[DLQEntry]:
        """Read-only view of the dead-letter queue entries."""
        return self._dlq

    def clear_dlq(self) -> None:
        """Remove all entries from the dead-letter queue."""
        self._dlq.clear()

    # -----------------------------------------------------------------------
    # Introspection
    # -----------------------------------------------------------------------

    def topics(self) -> list[TopicName]:
        """Return all registered topic patterns."""
        return list(self._subscriptions.keys())

    def subscribers(self, topic: TopicName) -> list[Subscription[Any]]:
        """Return all subscriptions (including wildcards) that would match ``topic``."""
        return self._collect_subscriptions(topic)

    def handler_count(self, topic: TopicName) -> int:
        """Return the number of handlers registered for an exact topic pattern."""
        return len(self._subscriptions.get(topic, []))

    def __repr__(self) -> str:
        return (
            f"EventBus(topics={list(self._subscriptions.keys())}, "
            f"dlq_size={len(self._dlq)})"
        )


# ---------------------------------------------------------------------------
# Convenience: standalone functions
# ---------------------------------------------------------------------------

# Singleton-style global bus for simple usage
_global_bus: EventBus | None = None


def get_global_bus() -> EventBus:
    """Get or create the module-level global EventBus instance."""
    global _global_bus  # noqa: PLW0603
    if _global_bus is None:
        _global_bus = EventBus()
    return _global_bus


def subscribe(
    topic: TopicName,
    handler: Handler | AsyncHandler,
    *,
    once: bool = False,
    priority: int = 0,
) -> Subscription[Any]:
    """Subscribe on the global bus."""
    return get_global_bus().subscribe(topic, handler, once=once, priority=priority)


def unsubscribe(topic: TopicName, handler: Handler | AsyncHandler | None = None) -> bool:
    """Unsubscribe from the global bus."""
    return get_global_bus().unsubscribe(topic, handler)


def publish(topic: TopicName, payload: EventPayload) -> bool:
    """Publish on the global bus (sync)."""
    return get_global_bus().publish(topic, payload)


async def publish_async(topic: TopicName, payload: EventPayload) -> bool:
    """Publish on the global bus (async)."""
    return await get_global_bus().publish_async(topic, payload)
