import asyncio
import threading

import pytest

import janus


class ObservingEvent:
    def __init__(self, observed: threading.Event):
        self._observed = observed
        self._event = threading.Event()

    def wait(self, timeout=None):
        self._observed.set()
        return self._event.wait(timeout)

    def set(self):
        self._event.set()


class TestZeroQueueBasic:
    def test_entry_is_distinct_from_unbounded_queue(self):
        q = janus.ZeroQueue()

        assert isinstance(q, janus.Queue)
        assert q.maxsize == 0
        assert q.sync_q.full()
        assert q.async_q.full()
        assert q.sync_q.empty()
        assert q.async_q.empty()

    def test_nowait_requires_waiting_counterpart(self):
        q = janus.ZeroQueue()

        with pytest.raises(janus.SyncQueueFull):
            q.sync_q.put_nowait(1)
        with pytest.raises(janus.SyncQueueEmpty):
            q.sync_q.get_nowait()

    @pytest.mark.asyncio
    async def test_async_nowait_requires_waiting_counterpart(self):
        q = janus.ZeroQueue()

        with pytest.raises(janus.AsyncQueueFull):
            q.async_q.put_nowait(1)
        with pytest.raises(janus.AsyncQueueEmpty):
            q.async_q.get_nowait()

        await q.aclose()

    @pytest.mark.asyncio
    async def test_nowait_pairs_with_waiting_sync_getter(self):
        q = janus.ZeroQueue()
        item = object()
        result = {}
        getter_registered = threading.Event()
        q._zero_event_factory = lambda: ObservingEvent(getter_registered)

        def consume():
            result["item"] = q.sync_q.get()

        consumer = threading.Thread(target=consume)
        consumer.start()
        await asyncio.wait_for(
            asyncio.to_thread(getter_registered.wait), 1
        )
        q.async_q.put_nowait(item)
        consumer.join(1)

        assert not consumer.is_alive()
        assert result["item"] is item
        q.sync_q.task_done()
        await q.aclose()


class TestZeroQueueRendezvous:
    @pytest.mark.asyncio
    async def test_sync_put_returns_after_async_get_takes_item(self):
        q = janus.ZeroQueue()
        item = object()
        result = {}
        producer_registered = threading.Event()
        q._zero_event_factory = lambda: ObservingEvent(
            producer_registered
        )

        def produce():
            q.sync_q.put(item)
            result["returned"] = True

        producer = threading.Thread(target=produce)
        producer.start()
        await asyncio.wait_for(
            asyncio.to_thread(producer_registered.wait), 1
        )

        received = await asyncio.wait_for(q.async_q.get(), 1)
        producer.join(1)

        assert received is item
        assert result["returned"] is True
        assert not producer.is_alive()
        q.sync_q.task_done()
        assert q.sync_q.unfinished_tasks == 0
        await q.aclose()

    @pytest.mark.asyncio
    async def test_async_put_returns_after_sync_get_takes_item(self):
        q = janus.ZeroQueue()
        item = object()
        result = {}

        def consume():
            result["item"] = q.sync_q.get()

        consumer = threading.Thread(target=consume)
        consumer.start()
        await q.async_q.put(item)
        consumer.join(1)

        assert not consumer.is_alive()
        assert result["item"] is item
        q.sync_q.task_done()
        assert q.sync_q.unfinished_tasks == 0
        await q.aclose()

    @pytest.mark.asyncio
    async def test_async_producer_and_consumer_rendezvous(self):
        q = janus.ZeroQueue()
        item = object()
        barrier = asyncio.Barrier(2)

        async def produce():
            await barrier.wait()
            await q.async_q.put(item)

        producer = asyncio.create_task(produce())
        await barrier.wait()
        received = await q.async_q.get()
        await producer

        assert received is item
        q.async_q.task_done()
        assert q.async_q.unfinished_tasks == 0
        await q.aclose()

    @pytest.mark.asyncio
    async def test_sync_producer_and_consumer_rendezvous(self):
        q = janus.ZeroQueue()
        item = object()
        result = {}
        producer_returned = threading.Event()

        def consume():
            result["item"] = q.sync_q.get()

        def produce():
            q.sync_q.put(item)
            producer_returned.set()

        consumer = threading.Thread(target=consume)
        producer = threading.Thread(target=produce)
        consumer.start()
        producer.start()
        producer_returned.wait(1)
        producer.join(1)
        consumer.join(1)

        assert producer_returned.is_set()
        assert result["item"] is item
        q.sync_q.task_done()
        assert q.sync_q.unfinished_tasks == 0
        await q.aclose()


class TestZeroQueueCancellation:
    @pytest.mark.asyncio
    async def test_unpaired_async_getter_is_removable(self):
        q = janus.ZeroQueue()
        barrier = asyncio.Barrier(2)

        async def consume():
            await barrier.wait()
            return await q.async_q.get()

        q._on_zero_async_wait = lambda side: barrier.wait()
        getter = asyncio.create_task(consume())
        await barrier.wait()
        getter.cancel()

        with pytest.raises(asyncio.CancelledError):
            await getter
        assert not q._zero_getters
        await q.aclose()

    @pytest.mark.asyncio
    async def test_unpaired_async_putter_is_removable(self):
        q = janus.ZeroQueue()
        item = object()
        barrier = asyncio.Barrier(2)

        async def produce():
            await barrier.wait()
            await q.async_q.put(item)

        q._on_zero_async_wait = lambda side: barrier.wait()
        putter = asyncio.create_task(produce())
        await barrier.wait()
        putter.cancel()

        with pytest.raises(asyncio.CancelledError):
            await putter
        assert not q._zero_putters
        assert q.sync_q.unfinished_tasks == 0
        await q.aclose()

    @pytest.mark.asyncio
    async def test_committed_item_stays_with_selected_consumer(self):
        q = janus.ZeroQueue()
        item = object()
        at_paired_point = asyncio.Event()

        async def paired_hook(side):
            assert q.sync_q.unfinished_tasks == 1
            assert not q._zero_putters
            assert not q._zero_getters
            at_paired_point.set()

        q._on_zero_async_paired = paired_hook

        producer = asyncio.create_task(q.async_q.put(item))
        received = await q.async_q.get()
        await at_paired_point.wait()
        await producer

        assert received is item
        assert q.sync_q.unfinished_tasks == 1
        assert not q._zero_putters
        assert not q._zero_getters
        q.shutdown()
        with pytest.raises(janus.AsyncQueueShutDown):
            await q.async_q.get()
        q.async_q.task_done()
        await q.wait_closed()


class TestZeroQueueShutdown:
    @pytest.mark.asyncio
    async def test_graceful_shutdown_finishes_committed_handoff(self):
        q = janus.ZeroQueue()
        item = object()
        at_paired_point = asyncio.Event()

        async def produce():
            await q.async_q.put(item)

        async def paired_hook(side):
            at_paired_point.set()
            q.shutdown()

        q._on_zero_async_paired = paired_hook
        producer = asyncio.create_task(produce())
        getter = asyncio.create_task(q.async_q.get())

        await at_paired_point.wait()
        assert await getter is item
        await producer
        with pytest.raises(janus.AsyncQueueShutDown):
            await q.async_q.put(1)
        with pytest.raises(janus.AsyncQueueShutDown):
            await q.async_q.get()
        q.async_q.task_done()
        await q.wait_closed()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("side", ("put", "get"))
    async def test_immediate_shutdown_wakes_unpaired_waiter(self, side):
        q = janus.ZeroQueue()
        waiter_at_wait = asyncio.Event()
        allow_waiter = asyncio.Event()

        async def operation():
            if side == "put":
                await q.async_q.put(1)
            else:
                await q.async_q.get()

        async def wait_hook(waited_side):
            assert waited_side == side
            waiter_at_wait.set()
            await allow_waiter.wait()

        q._on_zero_async_wait = wait_hook
        waiter = asyncio.create_task(operation())
        await waiter_at_wait.wait()
        q.shutdown(immediate=True)
        allow_waiter.set()

        with pytest.raises(janus.AsyncQueueShutDown):
            await waiter
        assert q.sync_q.unfinished_tasks == 0
        await q.wait_closed()


class TestZeroQueueFairness:
    @pytest.mark.asyncio
    async def test_waiters_are_served_in_fifo_order(self):
        q = janus.ZeroQueue()
        items = [object() for _ in range(8)]
        all_waiters_registered = asyncio.Event()
        started = 0

        async def wait_hook(side):
            nonlocal started
            started += 1
            if started == len(items):
                all_waiters_registered.set()

        q._on_zero_async_wait = wait_hook

        async def produce(item):
            await q.async_q.put(item)

        producers = [asyncio.create_task(produce(item)) for item in items]
        await all_waiters_registered.wait()
        consumers = [asyncio.create_task(q.async_q.get()) for _ in items]

        received = await asyncio.gather(*consumers)
        await asyncio.gather(*producers)

        assert received == items
        for _ in items:
            q.async_q.task_done()
        assert q.sync_q.unfinished_tasks == 0
        await q.aclose()
