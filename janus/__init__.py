import asyncio
import sys
import threading
from asyncio import QueueEmpty as AsyncQueueEmpty
from asyncio import QueueFull as AsyncQueueFull
from asyncio import shield
from collections import deque
from concurrent.futures import Future as ThreadFuture
from functools import partial
from heapq import heappop, heappush
from queue import Empty as SyncQueueEmpty
from queue import Full as SyncQueueFull
from time import monotonic
from typing import Any, Callable, Generic, Optional, Protocol, TypeVar

if sys.version_info >= (3, 13):
    from asyncio import QueueShutDown as AsyncQueueShutDown
    from queue import ShutDown as SyncQueueShutDown
else:
    class QueueShutDown(Exception):
        pass

    AsyncQueueShutDown = QueueShutDown

    class ShutDown(Exception):
        pass

    SyncQueueShutDown = ShutDown


__version__ = "2.0.0"
__all__ = (
    "Queue",
    "PriorityQueue",
    "LifoQueue",
    "SyncQueue",
    "SyncQueueEmpty",
    "SyncQueueFull",
    "SyncQueueShutDown",
    "AsyncQueue",
    "AsyncQueueEmpty",
    "AsyncQueueFull",
    "AsyncQueueShutDown",
    "BaseQueue",
)


T = TypeVar("T")
OptFloat = Optional[float]


class _RendezvousWaiter(Generic[T]):
    __slots__ = (
        "is_async",
        "is_producer",
        "item",
        "condition",
        "future",
        "handoff_event",
        "handoff_future",
        "handoff_ready",
        "committed",
        "peer",
        "exception",
    )

    def __init__(
        self,
        *,
        is_async: bool,
        is_producer: bool,
        item: Optional[T],
        condition: Optional[threading.Condition] = None,
        future: Optional[asyncio.Future[Any]] = None,
        handoff_event: Optional[threading.Event] = None,
        handoff_future: Optional[ThreadFuture[None]] = None,
        handoff_ready: bool = False,
    ) -> None:
        self.is_async = is_async
        self.is_producer = is_producer
        self.item = item
        self.condition = condition
        self.future = future
        self.handoff_event = handoff_event
        self.handoff_future = handoff_future
        self.handoff_ready = handoff_ready
        self.committed = False
        self.peer: Optional["_RendezvousWaiter[T]"] = None
        self.exception: Optional[BaseException] = None


class BaseQueue(Protocol[T]):
    @property
    def maxsize(self) -> int: ...

    @property
    def closed(self) -> bool: ...

    def task_done(self) -> None: ...

    def qsize(self) -> int: ...

    @property
    def unfinished_tasks(self) -> int: ...

    def empty(self) -> bool: ...

    def full(self) -> bool: ...

    def put_nowait(self, item: T) -> None: ...

    def get_nowait(self) -> T: ...

    def shutdown(self, immediate: bool = False) -> None: ...


class SyncQueue(BaseQueue[T], Protocol[T]):

    def put(self, item: T, block: bool = True, timeout: OptFloat = None) -> None: ...

    def get(self, block: bool = True, timeout: OptFloat = None) -> T: ...

    def join(self) -> None: ...


class AsyncQueue(BaseQueue[T], Protocol[T]):
    async def put(self, item: T) -> None: ...

    async def get(self) -> T: ...

    async def join(self) -> None: ...


class Queue(Generic[T]):
    _loop: Optional[asyncio.AbstractEventLoop] = None

    def __init__(self, maxsize: int = 0) -> None:
        if sys.version_info < (3, 10):
            self._loop = asyncio.get_running_loop()

        self._maxsize = maxsize
        self._is_shutdown = False
        self._rendezvous = False

        self._init(maxsize)

        self._unfinished_tasks = 0

        self._sync_mutex = threading.Lock()
        self._sync_not_empty = threading.Condition(self._sync_mutex)
        self._sync_not_empty_waiting = 0
        self._sync_not_full = threading.Condition(self._sync_mutex)
        self._sync_not_full_waiting = 0
        self._sync_tasks_done = threading.Condition(self._sync_mutex)
        self._sync_tasks_done_waiting = 0

        self._async_mutex = asyncio.Lock()
        if sys.version_info[:3] == (3, 10, 0):
            # Workaround for Python 3.10 bug, see #358:
            getattr(self._async_mutex, "_get_loop", lambda: None)()
        self._async_not_empty = asyncio.Condition(self._async_mutex)
        self._async_not_empty_waiting = 0
        self._async_not_full = asyncio.Condition(self._async_mutex)
        self._async_not_full_waiting = 0
        self._async_tasks_done = asyncio.Condition(self._async_mutex)
        self._async_tasks_done_waiting = 0

        self._pending: deque[asyncio.Future[None]] = deque()
        self._rendezvous_producers: deque["_RendezvousWaiter[T]"] = deque()
        self._rendezvous_consumers: deque["_RendezvousWaiter[T]"] = deque()
        self._rendezvous_handoffs: deque["_RendezvousWaiter[T]"] = deque()
        self._rendezvous_handoff_lock = threading.Lock()

        self._sync_queue = _SyncQueueProxy(self)
        self._async_queue = _AsyncQueueProxy(self)

    @classmethod
    def rendezvous(cls) -> "Queue[T]":
        """Create a queue with no buffered slots."""
        queue = cls(0)
        queue._rendezvous = True
        try:
            queue._loop = asyncio.get_running_loop()
        except RuntimeError:
            pass
        return queue

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        # Warning!
        # The function should be called when self._sync_mutex is locked,
        # otherwise the code is not thread-safe
        loop = asyncio.get_running_loop()

        if self._loop is None:
            self._loop = loop
        if loop is not self._loop:
            raise RuntimeError(f"{self!r} is bound to a different event loop")
        return loop

    def shutdown(self, immediate: bool = False) -> None:
        """Shut-down the queue, making queue gets and puts raise an exception.

        By default, gets will only raise once the queue is empty. Set
        'immediate' to True to make gets raise immediately instead.

        All blocked callers of put() and get() will be unblocked. If
        'immediate', a task is marked as done for each item remaining in
        the queue, which may unblock callers of join().

        The raise exception is SyncQueueShutDown for sync api and AsyncQueueShutDown
        for async one.
        """
        with self._sync_mutex:
            self._is_shutdown = True
            if immediate:
                if self._rendezvous:
                    self._abort_rendezvous_waiters()
                else:
                    while self._qsize():
                        self._get()
                        if self._unfinished_tasks > 0:
                            self._unfinished_tasks -= 1
                # release all blocked threads in `join()`
                if self._sync_tasks_done_waiting:
                    self._sync_tasks_done.notify_all()
                if self._async_tasks_done_waiting:
                    self._notify_async(self._async_tasks_done.notify_all)
            if self._rendezvous:
                return
            # All getters need to re-check queue-empty to raise ShutDown
            if self._sync_not_empty_waiting:
                self._sync_not_empty.notify_all()
            if self._sync_not_full_waiting:
                self._sync_not_full.notify_all()
            if self._async_not_empty_waiting:
                self._notify_async(self._async_not_empty.notify_all)
            if self._async_not_full_waiting:
                self._notify_async(self._async_not_full.notify_all)

    def close(self) -> None:
        """Close the queue.

        The method is a shortcut for .shutdown(immediate=True)
        """
        self.shutdown(immediate=True)

    async def wait_closed(self) -> None:
        """Wait for finishing all pending activities"""
        # should be called from loop after close().
        # Nobody should put/get at this point,
        # so lock acquiring is not required
        if not self._is_shutdown:
            raise RuntimeError("Waiting for non-closed queue")
        # give a chance for the task-done callbacks
        # of async tasks created inside
        # _notify_async()
        # methods to be executed.
        await asyncio.sleep(0)
        if not self._pending:
            return
        await asyncio.wait(self._pending)

    async def aclose(self) -> None:
        """Shutdown the queue and wait for actual shutting down"""
        self.close()
        await self.wait_closed()

    @property
    def closed(self) -> bool:
        return self._is_shutdown and not self._pending

    @property
    def maxsize(self) -> int:
        return self._maxsize

    @property
    def sync_q(self) -> "_SyncQueueProxy[T]":
        return self._sync_queue

    @property
    def async_q(self) -> "_AsyncQueueProxy[T]":
        return self._async_queue

    # Override these methods to implement other queue organizations
    # (e.g. stack or priority queue).
    # These will only be called with appropriate locks held

    def _init(self, maxsize: int) -> None:
        self._queue: deque[T] = deque()

    def _qsize(self) -> int:
        return len(self._queue)

    # Put a new item in the queue
    def _put(self, item: T) -> None:
        self._queue.append(item)

    # Get an item from the queue
    def _get(self) -> T:
        return self._queue.popleft()

    def _put_internal(self, item: T) -> None:
        self._put(item)
        self._unfinished_tasks += 1

    def _abort_rendezvous_waiters(self) -> None:
        for waiter in self._rendezvous_producers:
            exception = (
                AsyncQueueShutDown() if waiter.is_async else SyncQueueShutDown()
            )
            if waiter.handoff_event is not None:
                waiter.handoff_event.set()
            self._resolve_rendezvous_waiter(waiter, exception)
        for waiter in self._rendezvous_consumers:
            exception = (
                AsyncQueueShutDown() if waiter.is_async else SyncQueueShutDown()
            )
            self._resolve_rendezvous_waiter(waiter, exception)
        self._flush_rendezvous_handoffs()
        self._rendezvous_producers.clear()
        self._rendezvous_consumers.clear()

    def _resolve_rendezvous_waiter(
        self,
        waiter: "_RendezvousWaiter[T]",
        exception: Optional[BaseException] = None,
        item: Optional[T] = None,
    ) -> None:
        waiter.exception = exception
        if waiter.condition is not None:
            waiter.condition.notify()
        if waiter.future is not None:
            future = waiter.future
            if exception is None:
                callback = partial(future.set_result, item)
            else:
                callback = partial(future.set_exception, exception)
            loop = self._loop
            if loop is None or loop.is_closed():
                return
            if threading.get_ident() == loop._thread_id:
                callback()
            else:
                loop.call_soon_threadsafe(callback)

    def _commit_rendezvous(
        self,
        producer: "_RendezvousWaiter[T]",
        consumer: "_RendezvousWaiter[T]",
    ) -> T:
        item = producer.item

        producer.committed = True
        consumer.committed = True
        producer.peer = consumer
        consumer.peer = producer
        consumer.item = item
        if producer in self._rendezvous_producers:
            self._rendezvous_producers.remove(producer)
        if consumer in self._rendezvous_consumers:
            self._rendezvous_consumers.remove(consumer)
        self._rendezvous_handoffs.append(producer)
        self._unfinished_tasks += 1

        if consumer.condition is not None:
            consumer.condition.notify()

        if producer.condition is not None:
            producer.condition.notify()

        return item

    def _new_sync_rendezvous_waiter(
        self, is_producer: bool, item: Optional[T] = None
    ) -> "_RendezvousWaiter[T]":
        return _RendezvousWaiter(
            is_async=False,
            is_producer=is_producer,
            item=item,
            condition=threading.Condition(self._sync_mutex),
            handoff_event=threading.Event() if is_producer else None,
        )

    def _new_async_rendezvous_waiter(
        self, is_producer: bool, item: Optional[T] = None
    ) -> "_RendezvousWaiter[T]":
        handoff_event = threading.Event() if is_producer else None
        handoff_future = None
        if is_producer and handoff_event is not None:
            handoff_future = ThreadFuture()

            def wait_for_handoff() -> None:
                handoff_event.wait()  # type: ignore[union-attr]
                handoff_future.set_result(None)  # type: ignore[union-attr]

            threading.Thread(target=wait_for_handoff, daemon=True).start()
        return _RendezvousWaiter(
            is_async=True,
            is_producer=is_producer,
            item=item,
            future=self._get_loop().create_future(),
            handoff_event=handoff_event,
            handoff_future=handoff_future,
        )

    def _signal_rendezvous_handoff(self, producer: "_RendezvousWaiter[T]") -> None:
        with self._rendezvous_handoff_lock:
            producer.handoff_ready = True
            if self._rendezvous_handoffs and self._rendezvous_handoffs[0] is producer:
                if producer.handoff_event is not None:
                    producer.handoff_event.set()

    def _finish_rendezvous_handoff(self, producer: "_RendezvousWaiter[T]") -> None:
        with self._rendezvous_handoff_lock:
            if self._rendezvous_handoffs and self._rendezvous_handoffs[0] is producer:
                self._rendezvous_handoffs.popleft()
            if (
                self._rendezvous_handoffs
                and self._rendezvous_handoffs[0].handoff_ready
            ):
                candidate = self._rendezvous_handoffs[0]
                if candidate.handoff_event is not None:
                    candidate.handoff_event.set()

    def _flush_rendezvous_handoffs(self) -> None:
        with self._rendezvous_handoff_lock:
            while self._rendezvous_handoffs:
                producer = self._rendezvous_handoffs[0]
                if producer.handoff_event is None or not producer.handoff_event.is_set():
                    producer.handoff_event.set()
                self._rendezvous_handoffs.popleft()

    def _pop_rendezvous_waiter(
        self, waiters: deque["_RendezvousWaiter[T]"]
    ) -> Optional["_RendezvousWaiter[T]"]:
        while waiters:
            waiter = waiters.popleft()
            if waiter.future is not None and waiter.future.done():
                continue
            return waiter
        return None

    def _sync_rendezvous_put(
        self, item: T, block: bool, timeout: OptFloat
    ) -> None:
        if timeout is not None and timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")

        endtime: Optional[float] = None
        if timeout is not None:
            endtime = monotonic() + timeout

        committed = False
        consumer: Optional[_RendezvousWaiter[T]] = None
        producer: _RendezvousWaiter[T]
        with self._sync_mutex:
            if self._is_shutdown:
                raise SyncQueueShutDown
            consumer = self._pop_rendezvous_waiter(self._rendezvous_consumers)
            if consumer is None:
                if not block:
                    raise SyncQueueFull
                producer = self._new_sync_rendezvous_waiter(True, item)
                self._rendezvous_producers.append(producer)
                while not producer.committed:
                    remaining = None
                    if endtime is not None:
                        remaining = endtime - monotonic()
                        if remaining <= 0:
                            self._rendezvous_producers.remove(producer)
                            raise SyncQueueFull
                    producer.condition.wait(remaining)  # type: ignore[union-attr]
                    if producer.exception is not None:
                        if producer in self._rendezvous_producers:
                            self._rendezvous_producers.remove(producer)
                        raise producer.exception
                while (
                    self._rendezvous_handoffs
                    and self._rendezvous_handoffs[0] is not producer
                ):
                    producer.condition.wait()
                if producer.handoff_event is not None:
                    producer.handoff_event.wait()
                self._finish_rendezvous_handoff(producer)
                consumer = producer.peer
            else:
                producer = self._new_sync_rendezvous_waiter(True, item)
                self._commit_rendezvous(producer, consumer)
                committed = True

        assert consumer is not None
        assert producer is not None
        if consumer.future is not None:
            if committed:
                self._resolve_rendezvous_waiter(consumer, item=item)
        if block and producer.handoff_event is not None:
            producer.handoff_event.wait()
            self._finish_rendezvous_handoff(producer)

    def _sync_rendezvous_get(
        self, block: bool, timeout: OptFloat
    ) -> T:
        if timeout is not None and timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")

        endtime: Optional[float] = None
        if timeout is not None:
            endtime = monotonic() + timeout

        committed = False
        producer: Optional[_RendezvousWaiter[T]] = None
        consumer: _RendezvousWaiter[T]
        with self._sync_mutex:
            if self._is_shutdown:
                raise SyncQueueShutDown
            producer = self._pop_rendezvous_waiter(self._rendezvous_producers)
            if producer is None:
                if not block:
                    raise SyncQueueEmpty
                consumer = self._new_sync_rendezvous_waiter(False)
                self._rendezvous_consumers.append(consumer)
                while not consumer.committed:
                    remaining = None
                    if endtime is not None:
                        remaining = endtime - monotonic()
                        if remaining <= 0:
                            self._rendezvous_consumers.remove(consumer)
                            raise SyncQueueEmpty
                    consumer.condition.wait(remaining)  # type: ignore[union-attr]
                    if consumer.exception is not None:
                        if consumer in self._rendezvous_consumers:
                            self._rendezvous_consumers.remove(consumer)
                        raise consumer.exception
                producer = consumer.peer
                item = consumer.item
            else:
                consumer = self._new_sync_rendezvous_waiter(False)
                item = self._commit_rendezvous(producer, consumer)
                committed = True

        assert producer is not None
        assert consumer is not None
        if producer.future is not None:
            self._resolve_rendezvous_waiter(producer)
        if producer.handoff_event is not None:
            producer.handoff_event.set()
        return item

    async def _async_rendezvous_put(self, item: T) -> None:
        producer: _RendezvousWaiter[T]
        committed = False
        consumer: Optional[_RendezvousWaiter[T]] = None
        with self._sync_mutex:
            if self._is_shutdown:
                raise AsyncQueueShutDown
            self._get_loop()
            consumer = self._pop_rendezvous_waiter(self._rendezvous_consumers)
            if consumer is None:
                producer = self._new_async_rendezvous_waiter(True, item)
                self._rendezvous_producers.append(producer)
            else:
                producer = self._new_async_rendezvous_waiter(True, item)
                self._commit_rendezvous(producer, consumer)
                if consumer.condition is not None:
                    consumer.condition.notify()
                committed = True

        if committed:
            assert consumer is not None
            if consumer.future is not None:
                self._resolve_rendezvous_waiter(consumer, item=item)
            await self._wait_async_rendezvous_handoff(producer)
            return

        while not producer.committed:
            try:
                await shield(producer.future)  # type: ignore[union-attr]
            except asyncio.CancelledError:
                with self._sync_mutex:
                    if producer in self._rendezvous_producers:
                        self._rendezvous_producers.remove(producer)
                    raise
        await self._wait_async_rendezvous_handoff(producer)

    async def _wait_async_rendezvous_handoff(
        self, producer: "_RendezvousWaiter[T]"
    ) -> None:
        if producer.handoff_future is None:
            return
        await asyncio.wrap_future(producer.handoff_future)
        self._finish_rendezvous_handoff(producer)

    def _async_rendezvous_put_nowait(self, item: T) -> None:
        with self._sync_mutex:
            if self._is_shutdown:
                raise AsyncQueueShutDown
            self._get_loop()
            consumer = self._pop_rendezvous_waiter(self._rendezvous_consumers)
            if consumer is None:
                raise AsyncQueueFull
            producer = self._new_async_rendezvous_waiter(True, item)
            self._commit_rendezvous(producer, consumer)
            if consumer.condition is not None:
                consumer.condition.notify()

        if consumer.future is not None:
            self._resolve_rendezvous_waiter(consumer, item=item)
        elif producer.handoff_event is not None:
            producer.handoff_event.set()
        self._finish_rendezvous_handoff(producer)

    async def _async_rendezvous_get(self) -> T:
        consumer: _RendezvousWaiter[T]
        committed = False
        producer: Optional[_RendezvousWaiter[T]] = None

        with self._sync_mutex:
            if self._is_shutdown:
                raise AsyncQueueShutDown
            self._get_loop()

            producer = self._pop_rendezvous_waiter(self._rendezvous_producers)
            if producer is None:
                consumer = self._new_async_rendezvous_waiter(False)
                self._rendezvous_consumers.append(consumer)
            else:
                consumer = self._new_async_rendezvous_waiter(False)
                self._commit_rendezvous(producer, consumer)
                committed = True

        if committed:
            assert producer is not None
            if producer.future is not None:
                self._resolve_rendezvous_waiter(producer)
            self._signal_rendezvous_handoff(producer)
            return producer.item

        while not consumer.committed:
            try:
                await shield(consumer.future)  # type: ignore[union-attr]
            except asyncio.CancelledError:
                with self._sync_mutex:
                    if consumer in self._rendezvous_consumers:
                        self._rendezvous_consumers.remove(consumer)
                    raise

        producer = consumer.peer
        assert producer is not None
        if producer.future is not None:
            self._resolve_rendezvous_waiter(producer)
        self._signal_rendezvous_handoff(producer)
        return consumer.item  # type: ignore[return-value]

    def _async_rendezvous_get_nowait(self) -> T:
        with self._sync_mutex:
            if self._is_shutdown:
                raise AsyncQueueShutDown
            self._get_loop()
            producer = self._pop_rendezvous_waiter(self._rendezvous_producers)
            if producer is None:
                raise AsyncQueueEmpty
            consumer = self._new_async_rendezvous_waiter(False)
            self._commit_rendezvous(producer, consumer)

        if producer.future is not None:
            self._resolve_rendezvous_waiter(producer)
        self._signal_rendezvous_handoff(producer)
        return producer.item

    async def _do_async_notifier(self, method: Callable[[], None]) -> None:
        async with self._async_mutex:
            method()

    def _setup_async_notifier(
        self, loop: asyncio.AbstractEventLoop, method: Callable[[], None]
    ) -> None:
        task = loop.create_task(self._do_async_notifier(method))
        task.add_done_callback(self._pending.remove)
        self._pending.append(task)

    def _notify_async(self, method: Callable[[], None]) -> None:
        # Warning!
        # The function should be called when self._sync_mutex is locked,
        # otherwise the code is not thread-safe
        loop = self._loop
        if loop is None or loop.is_closed():
            # async API is not available, nothing to notify
            return
        loop.call_soon_threadsafe(self._setup_async_notifier, loop, method)

class _SyncQueueProxy(SyncQueue[T]):
    """Create a queue object with a given maximum size.

    If maxsize is <= 0, the queue size is infinite.
    """

    def __init__(self, parent: Queue[T]):
        self._parent = parent

    @property
    def maxsize(self) -> int:
        return self._parent._maxsize

    @property
    def closed(self) -> bool:
        return self._parent.closed

    def task_done(self) -> None:
        """Indicate that a formerly enqueued task is complete.

        Used by Queue consumer threads.  For each get() used to fetch a task,
        a subsequent call to task_done() tells the queue that the processing
        on the task is complete.

        If a join() is currently blocking, it will resume when all items
        have been processed (meaning that a task_done() call was received
        for every item that had been put() into the queue).

        Raises a ValueError if called more times than there were items
        placed in the queue.
        """
        parent = self._parent
        with parent._sync_tasks_done:
            unfinished = parent._unfinished_tasks - 1
            if unfinished <= 0:
                if unfinished < 0:
                    raise ValueError("task_done() called too many times")
                if parent._sync_tasks_done_waiting:
                    parent._sync_tasks_done.notify_all()
                if parent._async_tasks_done_waiting:
                    parent._notify_async(parent._async_tasks_done.notify_all)
            parent._unfinished_tasks = unfinished

    def join(self) -> None:
        """Blocks until all items in the Queue have been gotten and processed.

        The count of unfinished tasks goes up whenever an item is added to the
        queue. The count goes down whenever a consumer thread calls task_done()
        to indicate the item was retrieved and all work on it is complete.

        When the count of unfinished tasks drops to zero, join() unblocks.
        """
        parent = self._parent
        with parent._sync_tasks_done:
            while parent._unfinished_tasks:
                parent._sync_tasks_done_waiting += 1
                try:
                    parent._sync_tasks_done.wait()
                finally:
                    parent._sync_tasks_done_waiting -= 1

    def qsize(self) -> int:
        """Return the approximate size of the queue (not reliable!)."""
        if self._parent._rendezvous:
            return 0
        return self._parent._qsize()

    @property
    def unfinished_tasks(self) -> int:
        """Return the number of unfinished tasks."""
        return self._parent._unfinished_tasks

    def empty(self) -> bool:
        """Return True if the queue is empty, False otherwise (not reliable!).

        This method is likely to be removed at some point.  Use qsize() == 0
        as a direct substitute, but be aware that either approach risks a race
        condition where a queue can grow before the result of empty() or
        qsize() can be used.

        To create code that needs to wait for all queued tasks to be
        completed, the preferred technique is to use the join() method.
        """
        if self._parent._rendezvous:
            return True
        return not self._parent._qsize()

    def full(self) -> bool:
        """Return True if the queue is full, False otherwise (not reliable!).

        This method is likely to be removed at some point.  Use qsize() >= n
        as a direct substitute, but be aware that either approach risks a race
        condition where a queue can shrink before the result of full() or
        qsize() can be used.
        """
        parent = self._parent
        if parent._rendezvous:
            with parent._sync_mutex:
                return bool(parent._rendezvous_consumers)
        return 0 < parent._maxsize <= parent._qsize()

    def put(self, item: T, block: bool = True, timeout: OptFloat = None) -> None:
        """Put an item into the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until a free slot is available. If 'timeout' is
        a non-negative number, it blocks at most 'timeout' seconds and raises
        the Full exception if no free slot was available within that time.
        Otherwise ('block' is false), put an item on the queue if a free slot
        is immediately available, else raise the Full exception ('timeout'
        is ignored in that case).
        """
        parent = self._parent
        if parent._rendezvous:
            return parent._sync_rendezvous_put(item, block, timeout)
        with parent._sync_not_full:
            if parent._is_shutdown:
                raise SyncQueueShutDown
            if parent._maxsize > 0:
                if not block:
                    if parent._qsize() >= parent._maxsize:
                        raise SyncQueueFull
                elif timeout is None:
                    while parent._qsize() >= parent._maxsize:
                        parent._sync_not_full_waiting += 1
                        try:
                            parent._sync_not_full.wait()
                        finally:
                            parent._sync_not_full_waiting -= 1
                        if parent._is_shutdown:
                            raise SyncQueueShutDown
                elif timeout < 0:
                    raise ValueError("'timeout' must be a non-negative number")
                else:
                    endtime = monotonic() + timeout
                    while parent._qsize() >= parent._maxsize:
                        remaining = endtime - monotonic()
                        if remaining <= 0.0:
                            raise SyncQueueFull
                        parent._sync_not_full_waiting += 1
                        try:
                            parent._sync_not_full.wait(remaining)
                        finally:
                            parent._sync_not_full_waiting -= 1
                        if parent._is_shutdown:
                            raise SyncQueueShutDown
            parent._put_internal(item)
            if parent._sync_not_empty_waiting:
                parent._sync_not_empty.notify()
            if parent._async_not_empty_waiting:
                parent._notify_async(parent._async_not_empty.notify)

    def get(self, block: bool = True, timeout: OptFloat = None) -> T:
        """Remove and return an item from the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until an item is available. If 'timeout' is
        a non-negative number, it blocks at most 'timeout' seconds and raises
        the Empty exception if no item was available within that time.
        Otherwise ('block' is false), return an item if one is immediately
        available, else raise the Empty exception ('timeout' is ignored
        in that case).
        """
        parent = self._parent
        if parent._rendezvous:
            return parent._sync_rendezvous_get(block, timeout)
        with parent._sync_not_empty:
            if parent._is_shutdown and not parent._qsize():
                raise SyncQueueShutDown
            if not block:
                if not parent._qsize():
                    raise SyncQueueEmpty
            elif timeout is None:
                while not parent._qsize():
                    parent._sync_not_empty_waiting += 1
                    try:
                        parent._sync_not_empty.wait()
                    finally:
                        parent._sync_not_empty_waiting -= 1
                    if parent._is_shutdown and not parent._qsize():
                        raise SyncQueueShutDown
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                endtime = monotonic() + timeout
                while not parent._qsize():
                    remaining = endtime - monotonic()
                    if remaining <= 0.0:
                        raise SyncQueueEmpty
                    parent._sync_not_empty_waiting += 1
                    try:
                        parent._sync_not_empty.wait(remaining)
                    finally:
                        parent._sync_not_empty_waiting -= 1
                    if parent._is_shutdown and not parent._qsize():
                        raise SyncQueueShutDown
            item = parent._get()
            if parent._sync_not_full_waiting:
                parent._sync_not_full.notify()
            if parent._async_not_full_waiting:
                parent._notify_async(parent._async_not_full.notify)
            return item

    def put_nowait(self, item: T) -> None:
        """Put an item into the queue without blocking.

        Only enqueue the item if a free slot is immediately available.
        Otherwise raise the Full exception.
        """
        return self.put(item, block=False)

    def get_nowait(self) -> T:
        """Remove and return an item from the queue without blocking.

        Only get an item if one is immediately available. Otherwise
        raise the Empty exception.
        """
        return self.get(block=False)

    def shutdown(self, immediate: bool = False) -> None:
        """Shut-down the queue, making queue gets and puts raise an exception.

        By default, gets will only raise once the queue is empty. Set
        'immediate' to True to make gets raise immediately instead.

        All blocked callers of put() and get() will be unblocked. If
        'immediate', a task is marked as done for each item remaining in
        the queue, which may unblock callers of join().

        The raise exception is SyncQueueShutDown for sync api and AsyncQueueShutDown
        for async one.
        """
        self._parent.shutdown(immediate)


class _AsyncQueueProxy(AsyncQueue[T]):
    """Create a queue object with a given maximum size.

    If maxsize is <= 0, the queue size is infinite.
    """

    def __init__(self, parent: Queue[T]):
        self._parent = parent

    @property
    def closed(self) -> bool:
        parent = self._parent
        return parent.closed

    def qsize(self) -> int:
        """Number of items in the queue."""
        parent = self._parent
        if parent._rendezvous:
            return 0
        return parent._qsize()

    @property
    def unfinished_tasks(self) -> int:
        """Return the number of unfinished tasks."""
        parent = self._parent
        return parent._unfinished_tasks

    @property
    def maxsize(self) -> int:
        """Number of items allowed in the queue."""
        parent = self._parent
        return parent._maxsize

    def empty(self) -> bool:
        """Return True if the queue is empty, False otherwise."""
        return self.qsize() == 0

    def full(self) -> bool:
        """Return True if there are maxsize items in the queue.

        Note: if the Queue was initialized with maxsize=0 (the default),
        then full() is never True.
        """
        parent = self._parent
        if parent._rendezvous:
            with parent._sync_mutex:
                return bool(parent._rendezvous_consumers)
        if parent._maxsize <= 0:
            return False
        else:
            return parent._qsize() >= parent._maxsize

    async def put(self, item: T) -> None:
        """Put an item into the queue.

        Put an item into the queue. If the queue is full, wait until a free
        slot is available before adding item.

        This method is a coroutine.
        """
        parent = self._parent
        if parent._rendezvous:
            await parent._async_rendezvous_put(item)
            return
        async with parent._async_not_full:
            with parent._sync_mutex:
                if parent._is_shutdown:
                    raise AsyncQueueShutDown
                parent._get_loop()  # check the event loop
                while 0 < parent._maxsize <= parent._qsize():
                    parent._async_not_full_waiting += 1
                    parent._sync_mutex.release()
                    try:
                        await parent._async_not_full.wait()
                    finally:
                        parent._sync_mutex.acquire()
                        parent._async_not_full_waiting -= 1
                    if parent._is_shutdown:
                        raise AsyncQueueShutDown

                parent._put_internal(item)
                if parent._async_not_empty_waiting:
                    parent._async_not_empty.notify()
                if parent._sync_not_empty_waiting:
                    parent._sync_not_empty.notify()

    def put_nowait(self, item: T) -> None:
        """Put an item into the queue without blocking.

        If no free slot is immediately available, raise QueueFull.
        """
        parent = self._parent
        if parent._rendezvous:
            parent._async_rendezvous_put_nowait(item)
            return
        with parent._sync_mutex:
            if parent._is_shutdown:
                raise AsyncQueueShutDown

            parent._get_loop()
            if 0 < parent._maxsize <= parent._qsize():
                raise AsyncQueueFull

            parent._put_internal(item)
            if parent._async_not_empty_waiting:
                parent._notify_async(parent._async_not_empty.notify)
            if parent._sync_not_empty_waiting:
                parent._sync_not_empty.notify()

    async def get(self) -> T:
        """Remove and return an item from the queue.

        If queue is empty, wait until an item is available.

        This method is a coroutine.
        """
        parent = self._parent
        if parent._rendezvous:
            return await parent._async_rendezvous_get()
        async with parent._async_not_empty:
            with parent._sync_mutex:
                if parent._is_shutdown and not parent._qsize():
                    raise AsyncQueueShutDown
                parent._get_loop()  # check the event loop
                while not parent._qsize():
                    parent._async_not_empty_waiting += 1
                    parent._sync_mutex.release()
                    try:
                        await parent._async_not_empty.wait()
                    finally:
                        parent._sync_mutex.acquire()
                        parent._async_not_empty_waiting -= 1
                    if parent._is_shutdown and not parent._qsize():
                        raise AsyncQueueShutDown

                item = parent._get()
                if parent._async_not_full_waiting:
                    parent._async_not_full.notify()
                if parent._sync_not_full_waiting:
                    parent._sync_not_full.notify()
                return item

    def get_nowait(self) -> T:
        """Remove and return an item from the queue.

        Return an item if one is immediately available, else raise QueueEmpty.
        """
        parent = self._parent
        if parent._rendezvous:
            return parent._async_rendezvous_get_nowait()
        with parent._sync_mutex:
            if parent._is_shutdown and not parent._qsize():
                raise AsyncQueueShutDown
            if not parent._qsize():
                raise AsyncQueueEmpty

            parent._get_loop()
            item = parent._get()
            if parent._async_not_full_waiting:
                parent._notify_async(parent._async_not_full.notify)
            if parent._sync_not_full_waiting:
                parent._sync_not_full.notify()
            return item

    def task_done(self) -> None:
        """Indicate that a formerly enqueued task is complete.

        Used by queue consumers. For each get() used to fetch a task,
        a subsequent call to task_done() tells the queue that the processing
        on the task is complete.

        If a join() is currently blocking, it will resume when all items have
        been processed (meaning that a task_done() call was received for every
        item that had been put() into the queue).

        Raises ValueError if called more times than there were items placed in
        the queue.
        """
        parent = self._parent
        with parent._sync_tasks_done:
            if parent._unfinished_tasks <= 0:
                raise ValueError("task_done() called too many times")
            parent._unfinished_tasks -= 1
            if parent._unfinished_tasks == 0:
                if parent._async_tasks_done_waiting:
                    parent._notify_async(parent._async_tasks_done.notify_all)
                if parent._sync_tasks_done_waiting:
                    parent._sync_tasks_done.notify_all()

    async def join(self) -> None:
        """Block until all items in the queue have been gotten and processed.

        The count of unfinished tasks goes up whenever an item is added to the
        queue. The count goes down whenever a consumer calls task_done() to
        indicate that the item was retrieved and all work on it is complete.
        When the count of unfinished tasks drops to zero, join() unblocks.
        """
        parent = self._parent
        async with parent._async_tasks_done:
            with parent._sync_mutex:
                parent._get_loop()  # check the event loop
                while parent._unfinished_tasks:
                    parent._async_tasks_done_waiting += 1
                    parent._sync_mutex.release()
                    try:
                        await parent._async_tasks_done.wait()
                    finally:
                        parent._sync_mutex.acquire()
                        parent._async_tasks_done_waiting -= 1

    def shutdown(self, immediate: bool = False) -> None:
        """Shut-down the queue, making queue gets and puts raise an exception.

        By default, gets will only raise once the queue is empty. Set
        'immediate' to True to make gets raise immediately instead.

        All blocked callers of put() and get() will be unblocked. If
        'immediate', a task is marked as done for each item remaining in
        the queue, which may unblock callers of join().

        The raise exception is SyncQueueShutDown for sync api and AsyncQueueShutDown
        for async one.
        """
        self._parent.shutdown(immediate)


class PriorityQueue(Queue[T]):
    """Variant of Queue that retrieves open entries in priority order
    (lowest first).

    Entries are typically tuples of the form:  (priority number, data).

    """

    def _init(self, maxsize: int) -> None:
        self._heap_queue: list[T] = []

    def _qsize(self) -> int:
        return len(self._heap_queue)

    def _put(self, item: T) -> None:
        heappush(self._heap_queue, item)

    def _get(self) -> T:
        return heappop(self._heap_queue)


class LifoQueue(Queue[T]):
    """Variant of Queue that retrieves most recently added entries first."""

    def _qsize(self) -> int:
        return len(self._queue)

    def _put(self, item: T) -> None:
        self._queue.append(item)

    def _get(self) -> T:
        return self._queue.pop()
