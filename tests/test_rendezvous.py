import asyncio
import threading

import pytest

import janus


class AsyncBarrier:
    def __init__(self, parties: int):
        self._parties = parties
        self._waiting = 0
        self._futures = []
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        async with self._lock:
            self._waiting += 1
            if self._waiting == self._parties:
                futures = self._futures
                self._futures = []
                self._waiting = 0
                for waiter in futures:
                    waiter.set_result(None)
                future.set_result(None)
            else:
                self._futures.append(future)
        await future

class TestRendezvous:
    def test_explicit_entry_does_not_change_zero_maxsize(self):
        queue = janus.Queue.rendezvous()
        try:
            assert queue.maxsize == 0
            assert queue.sync_q.qsize() == 0
            assert queue.sync_q.empty()
            assert not queue.sync_q.full()
        finally:
            queue.shutdown(immediate=True)

    def test_nowait_requires_waiting_peer(self):
        queue = janus.Queue.rendezvous()
        try:
            with pytest.raises(janus.SyncQueueFull):
                queue.sync_q.put_nowait(1)
            with pytest.raises(janus.SyncQueueEmpty):
                queue.sync_q.get_nowait()
        finally:
            queue.shutdown(immediate=True)

    @pytest.mark.asyncio
    async def test_async_nowait_requires_waiting_peer(self):
        queue = janus.Queue.rendezvous()
        try:
            with pytest.raises(janus.AsyncQueueFull):
                queue.async_q.put_nowait(1)
            with pytest.raises(janus.AsyncQueueEmpty):
                queue.async_q.get_nowait()
        finally:
            queue.shutdown(immediate=True)
            await queue.wait_closed()

    @pytest.mark.asyncio
    async def test_sync_producer_returns_after_async_consumer_takes_item(self):
        queue = janus.Queue.rendezvous()
        producer_started = threading.Event()
        producer_blocked = threading.Event()
        producer_returned = threading.Event()
        item = object()

        def produce():
            producer_started.wait(1)
            producer_blocked.set()
            queue.sync_q.put(item)
            producer_returned.set()

        thread = threading.Thread(target=produce, daemon=True)
        thread.start()
        consumer_ready = threading.Event()

        async def consume():
            consumer_ready.set()
            return await queue.async_q.get()

        consumer = asyncio.create_task(consume())
        consumer_ready.wait(1)
        producer_started.set()
        producer_blocked.wait(1)
        assert not producer_returned.is_set()
        received = await asyncio.wait_for(consumer, timeout=1)
        assert producer_returned.wait(1)
        queue.sync_q.task_done()
        queue.shutdown(immediate=True)
        await queue.wait_closed()
        assert received is item

    @pytest.mark.asyncio
    async def test_async_producer_returns_after_sync_consumer_takes_item(self):
        queue = janus.Queue.rendezvous()
        consumer_arrived = threading.Event()
        consumer_returned = threading.Event()
        producer_registered = AsyncBarrier(2)
        consumed = {}
        item = object()

        def consume():
            consumer_arrived.set()
            consumed["item"] = queue.sync_q.get()
            consumer_returned.set()

        async def produce():
            await producer_registered.wait()
            await queue.async_q.put(item)

        thread = threading.Thread(target=consume, daemon=True)
        thread.start()
        consumer_arrived.wait(1)

        producer = asyncio.create_task(produce())
        await producer_registered.wait()

        consumer_returned.wait(1)
        await asyncio.wait_for(producer, timeout=1)
        assert consumed["item"] is item
        queue.sync_q.task_done()
        queue.shutdown(immediate=True)
        await queue.wait_closed()

    @pytest.mark.asyncio
    async def test_join_counts_successful_handovers_only(self):
        queue = janus.Queue.rendezvous()
        items = []
        tasks = [asyncio.create_task(queue.async_q.get()) for _ in range(3)]

        def produce(item):
            queue.sync_q.put(item)

        threads = [
            threading.Thread(target=produce, args=(index,), daemon=True)
            for index in range(3)
        ]
        for thread in threads:
            thread.start()
        items.extend(await asyncio.gather(*tasks))

        assert sorted(items) == [0, 1, 2]
        assert queue.async_q.unfinished_tasks == 3
        for _ in items:
            queue.sync_q.task_done()
        await queue.async_q.join()
        queue.shutdown(immediate=True)
        await queue.wait_closed()

    @pytest.mark.asyncio
    async def test_waiters_are_served_in_fifo_order(self):
        queue = janus.Queue.rendezvous()
        started = []
        finished = []

        threads = []
        for item in range(8):
            ready = threading.Event()

            def produce_with_ready(value, event):
                started.append(value)
                event.set()
                queue.sync_q.put(value)
                finished.append(value)

            thread = threading.Thread(
                target=produce_with_ready, args=(item, ready), daemon=True
            )
            thread.start()
            threads.append(thread)
            ready.wait(1)

        received = []
        consumers = [asyncio.create_task(queue.async_q.get()) for _ in range(8)]
        for future in consumers:
            received.append(await future)
        for thread in threads:
            thread.join(1)
            assert not thread.is_alive()
        assert received == list(range(8))
        assert finished == list(range(8))
        for _ in range(8):
            queue.sync_q.task_done()
        queue.shutdown(immediate=True)
        await queue.wait_closed()

    @pytest.mark.asyncio
    async def test_cancel_before_commit_withdraws_producer(self):
        queue = janus.Queue.rendezvous()
        ready = AsyncBarrier(2)

        async def produce():
            await ready.wait()
            await queue.async_q.put(object())

        producer = asyncio.create_task(produce())
        await ready.wait()
        producer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await producer

        with pytest.raises(janus.AsyncQueueEmpty):
            queue.async_q.get_nowait()

        item = object()
        ready = AsyncBarrier(2)

        async def consume_item():
            await ready.wait()
            return await queue.async_q.get()

        consumer = asyncio.create_task(consume_item())
        await ready.wait()
        queue.sync_q.put_nowait(item)
        assert await consumer is item
        queue.sync_q.task_done()
        queue.shutdown(immediate=True)
        await queue.wait_closed()

    @pytest.mark.asyncio
    async def test_cancel_before_commit_withdraws_consumer(self):
        queue = janus.Queue.rendezvous()
        ready = AsyncBarrier(2)

        async def consume():
            await ready.wait()
            await queue.async_q.get()

        consumer = asyncio.create_task(consume())
        await ready.wait()
        consumer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await consumer

        ready = AsyncBarrier(2)

        async def consume_registered():
            await ready.wait()
            return await queue.async_q.get()

        consumer = asyncio.create_task(consume_registered())
        await ready.wait()
        item = object()
        assert queue.sync_q.put_nowait(item) is None
        assert await consumer is item
        queue.sync_q.task_done()
        queue.shutdown(immediate=True)
        await queue.wait_closed()

    @pytest.mark.asyncio
    async def test_cancel_and_immediate_shutdown_are_atomic_for_unpaired(self):
        queue = janus.Queue.rendezvous()
        barrier = AsyncBarrier(3)

        async def put():
            await barrier.wait()
            await queue.async_q.put(object())

        async def close_now():
            await barrier.wait()
            queue.shutdown(immediate=True)

        producer = asyncio.create_task(put())
        closer = asyncio.create_task(close_now())
        await barrier.wait()

        with pytest.raises((asyncio.CancelledError, janus.AsyncQueueShutDown)):
            await producer
        await closer
        await queue.wait_closed()
        assert queue.sync_q.qsize() == 0

    @pytest.mark.asyncio
    async def test_immediate_shutdown_wakes_unpaired_waiters(self):
        sync_put_queue = janus.Queue.rendezvous()
        sync_get_queue = janus.Queue.rendezvous()
        async_queue = janus.Queue.rendezvous()
        sync_put_ready = threading.Event()
        sync_get_ready = threading.Event()
        async_put_ready = AsyncBarrier(2)
        async_get_ready = AsyncBarrier(2)

        def sync_put():
            sync_put_ready.set()
            with pytest.raises(janus.SyncQueueShutDown):
                sync_put_queue.sync_q.put(1)

        def sync_get():
            sync_get_ready.set()
            with pytest.raises(janus.SyncQueueShutDown):
                sync_get_queue.sync_q.get()

        async def async_put():
            await async_put_ready.wait()
            with pytest.raises(janus.AsyncQueueShutDown):
                await async_queue.async_q.put(1)

        async def async_get():
            await async_get_ready.wait()
            with pytest.raises(janus.AsyncQueueShutDown):
                await async_queue.async_q.get()

        put_thread = threading.Thread(target=sync_put, daemon=True)
        get_thread = threading.Thread(target=sync_get, daemon=True)
        put_thread.start()
        get_thread.start()
        put_task = asyncio.create_task(async_put())
        get_task = asyncio.create_task(async_get())
        sync_put_ready.wait(1)
        sync_get_ready.wait(1)
        await async_put_ready.wait()
        await async_get_ready.wait()

        sync_put_queue.shutdown(immediate=True)
        sync_get_queue.shutdown(immediate=True)
        async_queue.shutdown(immediate=True)
        await put_task
        await get_task
        await async_queue.wait_closed()
        put_thread.join(1)
        get_thread.join(1)
        assert not put_thread.is_alive()
        assert not get_thread.is_alive()

    @pytest.mark.asyncio
    async def test_normal_shutdown_allows_committed_handoff_to_finish(self):
        item = object()
        producer_returned = threading.Event()

        def produce():
            queue.sync_q.put(item)
            producer_returned.set()

        queue = janus.Queue.rendezvous()
        async def consume():
            return await queue.async_q.get()

        consumer = asyncio.create_task(consume())
        thread = threading.Thread(target=produce, daemon=True)
        thread.start()
        received = await asyncio.wait_for(consumer, timeout=1)
        assert received is item
        assert producer_returned.wait(1)
        assert producer_returned.is_set()

        queue.sync_q.task_done()
        queue.shutdown()
        await queue.async_q.join()
        queue.shutdown(immediate=True)
        await queue.wait_closed()
