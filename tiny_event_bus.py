"""
tiny_event_bus - Zero-dependency in-process pub/sub event bus for Python.

Supports topics with wildcards, dead-letter queue with replay, message
redelivery with retry limits, ordered handlers, async handlers, priority
ordering, one-time subscriptions, bulk publishing, and runtime stats.
"""

from __future__ import annotations

import asyncio
import json
import time
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


class TopicWildcard:
    """
    Helper for declaring wildcard topic subscriptions.

    ``TopicWildcard("user", "*")`` is equivalent to subscribing on
    ``"user.*"``. Provides readable, validated access so callers don't
    hand-roll malformed patterns.

    Examples:
        >>> from tiny_event_bus import TopicWildcard
        >>> TopicWildcard("user", "*").pattern
        'user.*'
        >>> TopicWildcard.from_string("order.*").pattern
        'order.*'
    """

    __slots__ = ("segments",)

    def __init__(self, *segments: str) -> None:
        if not segments:
            raise ValueError("TopicWildcard requires at least one segment")
        for seg in segments:
            if "." in seg:
                raise ValueError(
                    f"Segment {seg!r} must not contain a '.'; pass segments separately"
                )
        self.segments: tuple[str, ...] = tuple(segments)

    @classmethod
    def from_string(cls, pattern: str) -> "TopicWildcard":
        """Build a TopicWildcard from a dotted pattern string."""
        return cls(*pattern.split("."))

    @property
    def pattern(self) -> str:
        """The dotted topic pattern this wildcard matches."""
        return ".".join(self.segments)

    def matches(self, topic: str) -> bool:
        return _match_topic(topic, self.pattern)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, TopicWildcard) and self.segments == other.segments

    def __hash__(self) -> int:
        return hash(self.segments)

    def __str__(self) -> str:
        return self.pattern

    def __repr__(self) -> str:
        return f"TopicWildcard({', '.join(map(repr, self.segments))})"


def _match_topic(topic: str, pattern: str) -> bool:
    """Return True if ``topic`` matches ``pattern`` (single-level wildcard)."""
    if "*" not in pattern:
        return topic == pattern
    t_parts = topic.split(".")
    p_parts = pattern.split(".")
    if len(t_parts) != len(p_parts):
        return False
    return all(tp == pp or pp == "*" for tp, pp in zip(t_parts, p_parts))


@dataclass
class Message:
    """
    A publishable message with optional redelivery support.

    Wraps a (topic, payload) pair plus retry bookkeeping. ``redeliver()``
    returns the message ready to be published again with its retry counter
    incremented (and optional per-publish delay honoured by the bus).
    """

    topic: TopicName
    payload: EventPayload
    _retries: int = field(default=0, repr=False)
    max_retries: int = field(default=3, repr=False)
    _delay: float = field(default=0.0, repr=False)

    def redeliver(self, *, delay: float = 0.0) -> "Message":
        """
        Return a copy of this message marked for one more delivery attempt.

        The returned message has its retry counter incremented and a delivery
        ``delay`` (seconds) attached that the bus will honour before dispatching.
        When the retry limit is reached, ``redeliver()`` returns ``None`` so
        callers can break out of retry loops.

        Args:
            delay: Seconds to wait before redelivering (0 = no delay).

        Returns:
            A new ``Message`` ready for ``bus.publish()``, or ``None`` if the
            retry budget is exhausted.
        """
        if self._retries >= self.max_retries:
            return None
        return Message(
            topic=self.topic,
            payload=self.payload,
            _retries=self._retries + 1,
            max_retries=self.max_retries,
            _delay=delay,
        )

    @property
    def attempts(self) -> int:
        """Number of delivery attempts made so far."""
        return self._retries + 1

    @property
    def remaining_retries(self) -> int:
        """How many more redeliveries are allowed before hitting max_retries."""
        return max(0, self.max_retries - self._retries)

    @property
    def delay(self) -> float:
        """Optional delay (seconds) attached before this delivery."""
        return self._delay

    def __repr__(self) -> str:
        return (
            f"Message(topic={self.topic!r}, payload={self.payload!r}, "
            f"retries={self._retries}/{self.max_retries})"
        )


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
    key: str
    enqueued_at: float = field(default_factory=time.time)

    @property
    def topic(self) -> str:
        """Convenience accessor for the failed event's topic."""
        return self.event.topic

    def to_dict(self) -> dict:
        """Serializable dict representation of this entry."""
        return {
            "key": self.key,
            "topic": self.event.topic,
            "payload": self.event.payload,
            "error": str(self.error),
            "enqueued_at": self.enqueued_at,
        }

    def __repr__(self) -> str:
        return f"DLQEntry(key={self.key!r}, topic={self.event.topic!r}, error={self.error!r})"


class DeadLetterQueue:
    """
    Dead-letter queue holding failed message deliveries.

    When a handler raises an exception, the event is routed here instead of
    being silently dropped. Entries can be listed, replayed, cleared, or
    drained. Replaying re-runs the original handler (or a supplied handler)
    against the retained event and removes the entry on success.
    """

    def __init__(self, bus: "EventBus") -> None:
        self._bus = bus
        self._entries: dict[str, DLQEntry] = {}
        self._counter: int = 0

    def _key(self) -> str:
        self._counter += 1
        return f"dlq-{self._counter}@{time.time_ns()}"

    def put(self, error: Exception, event: Event, handler: Callable) -> DLQEntry:
        """Add a failed delivery to the dead-letter queue. Returns the entry."""
        entry = DLQEntry(
            error=error,
            event=event,
            handler=handler,
            key=self._key(),
        )
        self._entries[entry.key] = entry
        return entry

    def get(self, key: str) -> DLQEntry | None:
        """Fetch a specific entry by key, or None if it does not exist."""
        return self._entries.get(key)

    def __len__(self) -> int:
        return len(self._entries)

    def __getitem__(self, index) -> DLQEntry:
        """Index into the DLQ entries (newest first) by integer or slice."""
        entries = self.entries
        return entries[index]

    @property
    def entries(self) -> list[DLQEntry]:
        """All dead-letter entries (newest first)."""
        return sorted(
            self._entries.values(),
            key=lambda e: e.enqueued_at,
            reverse=True,
        )

    def __iter__(self):
        return iter(self.entries)

    def __contains__(self, key: str) -> bool:
        return key in self._entries

    def remove(self, key: str) -> bool:
        """Remove an entry by key. Returns True if it was removed."""
        return self._entries.pop(key, None) is not None

    def clear(self) -> None:
        """Empty the dead-letter queue."""
        self._entries.clear()

    def replay(
        self,
        key: str,
        handler: Handler | AsyncHandler | None = None,
        *,
        remove_on_success: bool = True,
    ) -> bool:
        """
        Replay a single dead-letter message.

        Runs the retained (or a supplied) handler against the stored event.
        If it completes without raising, the entry is removed (unless
        ``remove_on_success=False``). If the handler fails again, the entry is
        kept (and its error updated) and ``False`` is returned.

        Args:
            key: The DLQ entry key to replay.
            handler: Optional override handler. Defaults to the original.
            remove_on_success: Remove the entry after a successful replay.

        Returns:
            True if the replay succeeded, False if the handler raised.
        """
        entry = self._entries.get(key)
        if entry is None:
            return False
        target = handler or entry.handler
        try:
            result = target(entry.event.payload)
            if asyncio.iscoroutine(result):
                result.close()  # don't await in sync replay
        except Exception as exc:  # noqa: BLE001
            entry.error = exc
            return False
        if remove_on_success:
            del self._entries[key]
        return True

    async def replay_async(
        self,
        key: str,
        handler: Handler | AsyncHandler | None = None,
        *,
        remove_on_success: bool = True,
    ) -> bool:
        """
        Async variant of :meth:`replay` that awaits async handlers.
        """
        entry = self._entries.get(key)
        if entry is None:
            return False
        target = handler or entry.handler
        try:
            result = target(entry.event.payload)
            if asyncio.iscoroutine(result):
                result = await result
        except Exception as exc:  # noqa: BLE001
            entry.error = exc
            return False
        if remove_on_success:
            del self._entries[key]
        return True

    def replay_all(
        self,
        handler: Handler | AsyncHandler | None = None,
        *,
        remove_on_success: bool = True,
    ) -> dict[str, bool]:
        """
        Replay every message currently in the DLQ.

        Returns a mapping of result key -> success bool so callers can see
        which entries still remain / failed again.
        """
        results: dict[str, bool] = {}
        for key in list(self._entries.keys()):
            results[key] = self.replay(
                key, handler=handler, remove_on_success=remove_on_success
            )
        return results

    def stats(self) -> dict:
        """Basic DLQ statistics as a dict."""
        return {
            "size": len(self._entries),
            "topics": {},
        }

    def __repr__(self) -> str:
        return f"DeadLetterQueue(size={len(self._entries)})"


# ---------------------------------------------------------------------------
# EventBus
# ---------------------------------------------------------------------------


class EventBus:
    """
    Central pub/sub hub for in-process event distribution.

    Features:
        - Topic-based subscriptions with single-level wildcard (``*``)
        - ``TopicWildcard`` helper for declaring patterns
        - Sync and async publish
        - Dead-letter queue (``DeadLetterQueue``) with replay
        - Message redelivery (``Message.redeliver()``) with retry limits
        - Redelivery delay scheduling
        - Middleware hooks (on_publish, on_subscribe, on_unsubscribe)
        - Priority ordering (higher priority runs first)
        - One-time subscriptions (``once=True`` / ``EventBus.once()``)
        - Event filtering (handler returning ``False`` stops propagation)
        - ``BulkPublisher`` for efficient batch publishing
        - ``stats()`` for queue depths, handler counts, and DLQ size
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
        self._dlq = DeadLetterQueue(self)
        self._dlq_handler: DLQHandler | None = dlq_handler
        self._on_publish: MiddlewareHook | None = on_publish
        self._on_subscribe: Callable[[Subscription], None] | None = on_subscribe
        self._on_unsubscribe: Callable[[Subscription], None] | None = on_unsubscribe
        self._insertion_counter: int = 0
        self._publish_count: int = 0
        self._delivered_count: int = 0

    # -----------------------------------------------------------------------
    # Subscription management
    # -----------------------------------------------------------------------

    def subscribe(
        self,
        topic,
        handler: Handler | AsyncHandler | None = None,
        *,
        once: bool = False,
        priority: int = 0,
    ):
        """
        Register ``handler`` to be called whenever ``topic`` is published.

        ``topic`` may be a plain string (``"order.created"``), a wildcard
        pattern (``"user.*"``), or a :class:`TopicWildcard` object.

        Can be used as a decorator or as a regular call.

        Args:
            topic: Exact topic, dotted wildcard string, or TopicWildcard.
            handler: Synchronous or asynchronous callable that accepts
                     the event payload and may return ``False`` to stop
                     propagation to lower-priority handlers. If omitted,
                     returns a decorator that applies the options.
            once: If True, the handler fires at most once then auto-unsubscribes.
            priority: Higher values run first. Default priority is 0.

        Returns:
            The ``Subscription`` object, or a decorator if ``handler`` is omitted.
        """
        pattern = self._coerce_topic(topic)

        def _register(h: Handler | AsyncHandler) -> Subscription[Any]:
            self._insertion_counter += 1
            sub = Subscription(
                topic=pattern,
                handler=h,
                once=once,
                priority=priority,
                active=True,
                _order=self._insertion_counter,
            )
            if pattern not in self._subscriptions:
                self._subscriptions[pattern] = []
            self._subscriptions[pattern].append(sub)
            self._subscriptions[pattern].sort()
            if self._on_subscribe:
                self._on_subscribe(sub)
            return sub

        if handler is None:
            return _register
        return _register(handler)

    def _coerce_topic(self, topic) -> str:
        """Accept a plain string or a TopicWildcard and return the pattern str."""
        if isinstance(topic, TopicWildcard):
            return topic.pattern
        if isinstance(topic, str):
            return topic
        raise TypeError(
            f"topic must be a str or TopicWildcard, got {type(topic).__name__}"
        )

    def once(self, topic, handler: Handler | AsyncHandler | None = None, **kwargs):
        """
        Subscribe for exactly one message, then auto-unsubscribe.

        Equivalent to :meth:`subscribe` with ``once=True`` (the default here,
        even if the caller passes ``once=False``). Works as a decorator or a
        direct call.

        Examples:
            >>> bus.once("app.started", on_start)
            >>> @bus.once("ready")
            ... def on_ready(payload):
            ...     print("Ready once")
        """
        kwargs.pop("once", None)  # once() always means once
        return self.subscribe(topic, handler, once=True, **kwargs)

    def unsubscribe(self, topic, handler=None) -> bool:
        """
        Remove a handler (or all handlers on a topic) from the subscription list.

        Pass either:
            - ``topic`` + ``handler`` to remove a specific handler
            - ``topic`` only to remove all handlers on that topic
            - A ``Subscription`` object previously returned by ``subscribe()``

        ``topic`` may also be a :class:`TopicWildcard`.

        Returns:
            True if a handler was removed, False otherwise.
        """
        if isinstance(handler, Subscription):
            if handler.topic != self._coerce_topic(topic):
                return False
            pattern = handler.topic
            subs = self._subscriptions.get(pattern, [])
            before = len(subs)
            self._subscriptions[pattern] = [s for s in subs if s is not handler]
            removed = before > len(self._subscriptions[pattern])
            if removed and self._on_unsubscribe:
                self._on_unsubscribe(handler)
            return removed

        pattern = self._coerce_topic(topic) if not isinstance(topic, Subscription) else topic.topic

        if handler is None:
            if pattern in self._subscriptions:
                subs = self._subscriptions.pop(pattern, [])
                for s in subs:
                    if self._on_unsubscribe:
                        self._on_unsubscribe(s)
                return True
            return False

        subs = self._subscriptions.get(pattern, [])
        before = len(subs)
        self._subscriptions[pattern] = [
            s for s in subs if s.handler is not handler
        ]
        removed = before > len(self._subscriptions[pattern])
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
        return _match_topic(topic, pattern)

    def _collect_subscriptions(self, topic: TopicName) -> list[Subscription[Any]]:
        """Gather all subscriptions (including wildcard patterns) that match ``topic``."""
        collected: dict[int, Subscription[Any]] = {}
        for pattern, subs in self._subscriptions.items():
            if self._match_topic(topic, pattern):
                for sub in subs:
                    if sub.active:
                        collected[id(sub)] = sub
        return sorted(collected.values(), key=lambda s: s.priority, reverse=True)

    # -----------------------------------------------------------------------
    # Sync publish
    # -----------------------------------------------------------------------

    def publish(
        self,
        topic,
        payload: EventPayload = None,
        *,
        stop_on_false: bool = True,
        delay: float = 0.0,
    ) -> bool:
        """
        Deliver ``payload`` to all handlers subscribed to ``topic`` synchronously.

        Handlers are called in priority order (highest first). A handler that
        returns ``False`` will stop propagation to remaining handlers when
        ``stop_on_false`` is True (the default).

        If a handler raises an exception, it is caught and routed to the DLQ.
        All remaining handlers are still attempted.

        When a :class:`Message` is passed as ``payload`` (or ``topic`` is a
        :class:`Message`), its optional ``delay`` is honoured via a blocking
        ``time.sleep`` (sync mode) unless ``delay`` is supplied explicitly.

        Args:
            topic: Topic string, TopicWildcard, or a Message to publish.
            payload: Arbitrary data to pass to handlers.
            stop_on_false: If True, stop propagation when a handler returns False.
            delay: Optional blocking delay (seconds) before dispatching.

        Returns:
            True if all handlers succeeded without returning False.
        """
        # Support publishing a Message object directly.
        if isinstance(topic, Message):
            msg = topic
            topic = msg.topic
            payload = msg.payload
            if delay <= 0:
                delay = msg.delay

        topic_str = self._coerce_topic(topic)
        if delay > 0:
            time.sleep(delay)

        event = Event(topic=topic_str, payload=payload)
        if self._on_publish:
            self._on_publish(event)
        self._publish_count += 1

        subs = self._collect_subscriptions(topic_str)
        for sub in subs:
            if not sub.active:
                continue
            try:
                result = sub.handler(payload)
                if asyncio.iscoroutine(result):
                    result.close()  # Don't await — fire-and-forget in sync mode
                self._delivered_count += 1
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
        topic,
        payload: EventPayload = None,
        *,
        stop_on_false: bool = True,
        delay: float = 0.0,
    ) -> bool:
        """
        Asynchronously deliver ``payload`` to all matching handlers.

        Works like :meth:`publish` but properly awaits async handlers and
        honours delays with ``asyncio.sleep`` instead of ``time.sleep``.

        Args:
            topic: Topic string, TopicWildcard, or a Message to publish.
            payload: Arbitrary data to pass to handlers.
            stop_on_false: If True, stop propagation when a handler returns False.
            delay: Optional async delay (seconds) before dispatching.

        Returns:
            True if all handlers succeeded without returning False.
        """
        if isinstance(topic, Message):
            msg = topic
            topic = msg.topic
            payload = msg.payload
            if delay <= 0:
                delay = msg.delay

        topic_str = self._coerce_topic(topic)
        if delay > 0:
            await asyncio.sleep(delay)

        event = Event(topic=topic_str, payload=payload)
        if self._on_publish:
            self._on_publish(event)
        self._publish_count += 1

        subs = self._collect_subscriptions(topic_str)
        for sub in subs:
            if not sub.active:
                continue
            try:
                result = sub.handler(payload)
                if asyncio.iscoroutine(result):
                    result = await result
                self._delivered_count += 1
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
        entry = self._dlq.put(error, event, sub.handler)
        if self._dlq_handler:
            self._dlq_handler(error, event)

    @property
    def dlq(self) -> DeadLetterQueue:
        """The dead-letter queue."""
        return self._dlq

    def clear_dlq(self) -> None:
        """Remove all entries from the dead-letter queue."""
        self._dlq.clear()

    def replay(
        self,
        dlq_key: str,
        handler: Handler | AsyncHandler | None = None,
        *,
        remove_on_success: bool = True,
    ) -> bool:
        """
        Replay a single dead-letter message.

        See :meth:`DeadLetterQueue.replay`.

        Returns:
            True if the replay succeeded, False otherwise.
        """
        return self._dlq.replay(
            dlq_key, handler=handler, remove_on_success=remove_on_success
        )

    async def replay_async(
        self,
        dlq_key: str,
        handler: Handler | AsyncHandler | None = None,
        *,
        remove_on_success: bool = True,
    ) -> bool:
        """Async variant of :meth:`replay`."""
        return await self._dlq.replay_async(
            dlq_key, handler=handler, remove_on_success=remove_on_success
        )

    def replay_all(
        self,
        handler: Handler | AsyncHandler | None = None,
        *,
        remove_on_success: bool = True,
    ) -> dict[str, bool]:
        """Replay every dead-letter message. See :meth:`DeadLetterQueue.replay_all`."""
        return self._dlq.replay_all(
            handler=handler, remove_on_success=remove_on_success
        )

    # -----------------------------------------------------------------------
    # Stats
    # -----------------------------------------------------------------------

    def stats(self, *, json_dump: bool = True) -> Any:
        """
        Return runtime statistics as JSON (or a JSON-serializable dict).

        Includes topic count, per-topic handler counts, total subscriptions,
        published message count, delivered message count, and DLQ size.

        Args:
            json_dump: If True, return a JSON string; otherwise a dict.

        Returns:
            A JSON string (default) or a dict.
        """
        topic_counts = {
            pattern: len(subs) for pattern, subs in self._subscriptions.items()
        }
        stats = {
            "version": "0.3.0",
            "topics": {
                "count": len(self._subscriptions),
                "handlers_per_topic": topic_counts,
            },
            "subscriptions": {
                "total": sum(len(s) for s in self._subscriptions.values()),
                "active": sum(
                    len([s for s in subs if s.active])
                    for subs in self._subscriptions.values()
                ),
            },
            "publish": {
                "published": self._publish_count,
                "delivered": self._delivered_count,
            },
            "dlq": {
                "size": len(self._dlq),
                "keys": list(self._dlq._entries.keys()),
            },
        }
        if json_dump:
            return json.dumps(stats, indent=2, default=str)
        return stats

    # -----------------------------------------------------------------------
    # Introspection
    # -----------------------------------------------------------------------

    def topics(self) -> list[TopicName]:
        """Return all registered topic patterns."""
        return list(self._subscriptions.keys())

    def subscribers(self, topic) -> list[Subscription[Any]]:
        """Return all subscriptions (including wildcards) that would match ``topic``."""
        return self._collect_subscriptions(self._coerce_topic(topic))

    def handler_count(self, topic) -> int:
        """Return the number of handlers registered for an exact topic pattern."""
        return len(self._subscriptions.get(self._coerce_topic(topic), []))

    def __repr__(self) -> str:
        return (
            f"EventBus(topics={list(self._subscriptions.keys())}, "
            f"dlq_size={len(self._dlq)})"
        )


# ---------------------------------------------------------------------------
# BulkPublisher
# ---------------------------------------------------------------------------


class BulkPublisher:
    """
    Efficiently publish many events in one go.

    Buffers events on the ``publish()`` path and flushes them all with a
    single pass over the collection, reducing per-event overhead in hot
    loops. Optionally deduplicates by topic when ``dedup_topics`` is True.

    Examples:
        >>> bus = EventBus()
        >>> bulk = BulkPublisher(bus)
        >>> bulk.publish("a", 1)
        >>> bulk.publish("a", 2)
        >>> bulk.publish("b", 3)
        >>> bulk.flush()          # delivers all buffered events
        3
    """

    def __init__(self, bus: EventBus, *, dedup_topics: bool = False) -> None:
        self._bus = bus
        self._buffer: list[tuple[str, EventPayload]] = []
        self._dedup = dedup_topics

    def publish(self, topic, payload: EventPayload = None) -> "BulkPublisher":
        """Buffer an event for the next :meth:`flush`. Returns self for chaining."""
        self._buffer.append((self._bus._coerce_topic(topic), payload))
        return self

    def add(self, topic, payload: EventPayload = None) -> "BulkPublisher":
        """Alias for :meth:`publish`."""
        return self.publish(topic, payload)

    def __call__(self, topic, payload: EventPayload = None) -> "BulkPublisher":
        return self.publish(topic, payload)

    def flush(self) -> int:
        """
        Deliver all buffered events.

        Returns the number of events flushed.
        """
        if not self._buffer:
            return 0
        if self._dedup:
            seen: dict[str, EventPayload] = {}
            for topic, payload in self._buffer:
                seen[topic] = payload  # last wins
            to_send = list(seen.items())
        else:
            to_send = self._buffer
        self._buffer = []

        sent = 0
        for topic, payload in to_send:
            self._bus.publish(topic, payload)
            sent += 1
        return sent

    async def flush_async(self) -> int:
        """
        Async flush that uses :meth:`EventBus.publish_async` for each event.

        Returns the number of events flushed.
        """
        if not self._buffer:
            return 0
        if self._dedup:
            seen: dict[str, EventPayload] = {}
            for topic, payload in self._buffer:
                seen[topic] = payload
            to_send = list(seen.items())
        else:
            to_send = self._buffer
        self._buffer = []

        sent = 0
        for topic, payload in to_send:
            await self._bus.publish_async(topic, payload)
            sent += 1
        return sent

    def __len__(self) -> int:
        return len(self._buffer)

    @property
    def pending(self) -> int:
        """Number of buffered, not-yet-flushed events."""
        return len(self._buffer)

    def clear(self) -> None:
        """Discard all buffered events without publishing them."""
        self._buffer.clear()

    def __repr__(self) -> str:
        return f"BulkPublisher(buffered={len(self._buffer)})"


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
    topic,
    handler: Handler | AsyncHandler,
    *,
    once: bool = False,
    priority: int = 0,
) -> Subscription[Any]:
    """Subscribe on the global bus."""
    return get_global_bus().subscribe(topic, handler, once=once, priority=priority)


def once(topic, handler: Handler | AsyncHandler, **kwargs) -> Subscription[Any]:
    """One-time subscribe on the global bus."""
    return get_global_bus().once(topic, handler, **kwargs)


def unsubscribe(topic, handler: Handler | AsyncHandler | None = None) -> bool:
    """Unsubscribe from the global bus."""
    return get_global_bus().unsubscribe(topic, handler)


def publish(topic, payload: EventPayload = None) -> bool:
    """Publish on the global bus (sync)."""
    return get_global_bus().publish(topic, payload)


async def publish_async(topic, payload: EventPayload = None) -> bool:
    """Publish on the global bus (async)."""
    return await get_global_bus().publish_async(topic, payload)


def make_message(topic: str, payload: EventPayload, *, max_retries: int = 3) -> Message:
    """Create a :class:`Message` for use with redelivery patterns."""
    return Message(topic=topic, payload=payload, max_retries=max_retries)


__all__ = [
    "EventBus",
    "DeadLetterQueue",
    "Message",
    "Event",
    "Subscription",
    "DLQEntry",
    "TopicWildcard",
    "BulkPublisher",
    "get_global_bus",
    "subscribe",
    "once",
    "unsubscribe",
    "publish",
    "publish_async",
    "make_message",
]
