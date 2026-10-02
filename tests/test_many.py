"""Tests for put_many()/get_many() batch operations."""

import asyncio
import threading
import time

from concurrent.futures import ThreadPoolExecutor

import pytest

import janus


def _cross_thread_wakeup_works():
    # Some sandboxes silently break loop.call_soon_threadsafe() wakeups
    # (the self-pipe send is rejected), which makes any mixed
    # thread/event-loop test hang.  Probe for it once.
    async def main():
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        timer = threading.Timer(
            0.05, lambda: loop.call_soon_threadsafe(fut.set_result, None)
        )
        timer.start()
        try:
            await asyncio.wait_for(asyncio.shield(fut), timeout=1)
        except asyncio.TimeoutError:
            return False
        finally:
            timer.cancel()
        return True

    try:
        return asyncio.run(main())
    except Exception:
        return False


mixed_mode = pytest.mark.skipif(
    not _cross_thread_wakeup_works(),
    reason="the environment breaks loop.call_soon_threadsafe() wakeups",
)


class TestSyncBatch:
    def test_fifo_order(self):
        q = janus.Queue()
        q.sync_q.put_many([1, 2, 3])
        q.sync_q.put_many([4, 5])
        assert q.sync_q.qsize() == 5
        assert q.sync_q.get_many(2, 3) == [1, 2, 3]
        assert q.sync_q.get_many(1) == [4, 5]
        assert q.sync_q.empty()

    def test_lifo_order(self):
        q = janus.LifoQueue()
        q.sync_q.put_many([1, 2, 3])
        assert q.sync_q.get_many(1, 2) == [3, 2]
        assert q.sync_q.get_many(1) == [1]

    def test_priority_order(self):
        q = janus.PriorityQueue()
        q.sync_q.put_many([(3, "c"), (1, "a"), (2, "b")])
        q.sync_q.put_many([(0, "z"), (4, "d")])
        batch = q.sync_q.get_many(3, 3)
        assert batch == [(0, "z"), (1, "a"), (2, "b")]
        assert q.sync_q.get_many(1) == [(3, "c"), (4, "d")]

    def test_put_many_requires_bounded_sequence(self):
        q = janus.Queue()
        with pytest.raises(TypeError, match="bounded sequence"):
            q.sync_q.put_many(x for x in range(3))
        with pytest.raises(TypeError, match="bounded sequence"):
            q.sync_q.put_many(iter([1, 2]))
        assert q.sync_q.empty()
        assert q.sync_q.unfinished_tasks == 0

    def test_put_many_batch_larger_than_maxsize(self):
        q = janus.Queue(maxsize=2)
        with pytest.raises(ValueError, match="never fit"):
            q.sync_q.put_many([1, 2, 3])
        assert q.sync_q.empty()

    def test_put_many_nowait_full_inserts_nothing(self):
        q = janus.Queue(maxsize=3)
        q.sync_q.put_many([1, 2])
        with pytest.raises(janus.SyncQueueFull):
            q.sync_q.put_many([3, 4], block=False)
        # no partial commit
        assert q.sync_q.qsize() == 2
        assert q.sync_q.unfinished_tasks == 2
        assert q.sync_q.get_many(1) == [1, 2]

    def test_put_many_timeout_inserts_nothing(self):
        q = janus.Queue(maxsize=2)
        q.sync_q.put_many([1, 2])
        with pytest.raises(janus.SyncQueueFull):
            q.sync_q.put_many([3], block=True, timeout=0.05)
        assert q.sync_q.qsize() == 2
        assert q.sync_q.unfinished_tasks == 2

    def test_put_many_waits_for_whole_batch(self):
        q = janus.Queue(maxsize=4)
        q.sync_q.put_many([1, 2, 3])
        done = threading.Event()

        def putter():
            q.sync_q.put_many([4, 5, 6])
            done.set()

        t = threading.Thread(target=putter, daemon=True)
        t.start()
        time.sleep(0.1)
        # one free slot is not enough for a batch of 3
        assert not done.is_set()
        assert q.sync_q.qsize() == 3
        q.sync_q.get()
        time.sleep(0.1)
        # two free slots are still not enough, nothing partially inserted
        assert not done.is_set()
        assert q.sync_q.qsize() == 2
        q.sync_q.get()
        t.join(timeout=5)
        assert done.is_set()
        assert q.sync_q.get_many(1) == [3, 4, 5, 6]

    def test_get_many_waits_for_min_items(self):
        q = janus.Queue()

        def producer():
            time.sleep(0.1)
            q.sync_q.put(1)
            time.sleep(0.1)
            q.sync_q.put_many([2, 3])

        t = threading.Thread(target=producer, daemon=True)
        t.start()
        start = time.monotonic()
        batch = q.sync_q.get_many(3, 5)
        assert time.monotonic() - start >= 0.15
        assert batch == [1, 2, 3]
        t.join(timeout=5)

    def test_get_many_nowait_below_min_removes_nothing(self):
        q = janus.Queue()
        q.sync_q.put_many([1, 2])
        with pytest.raises(janus.SyncQueueEmpty):
            q.sync_q.get_many(3, block=False)
        assert q.sync_q.qsize() == 2

    def test_get_many_timeout(self):
        q = janus.Queue()
        q.sync_q.put(1)
        with pytest.raises(janus.SyncQueueEmpty):
            q.sync_q.get_many(2, timeout=0.05)
        assert q.sync_q.qsize() == 1

    def test_get_many_invalid_bounds(self):
        q = janus.Queue()
        with pytest.raises(ValueError, match="min_items"):
            q.sync_q.get_many(0)
        with pytest.raises(ValueError, match="max_items"):
            q.sync_q.get_many(3, 2)

    def test_get_many_closed_tail_partial(self):
        q = janus.Queue()
        q.sync_q.put_many([1, 2])
        q.shutdown()
        # explicit opt-in: take the tail batch smaller than min_items
        assert q.sync_q.get_many(3, allow_partial=True) == [1, 2]
        with pytest.raises(janus.SyncQueueShutDown):
            q.sync_q.get_many(1, allow_partial=True)

    def test_get_many_closed_tail_raises_by_default(self):
        q = janus.Queue()
        q.sync_q.put_many([1, 2])
        q.shutdown()
        with pytest.raises(janus.SyncQueueShutDown):
            q.sync_q.get_many(3)
        # the tail is still there for an explicit partial read
        assert q.sync_q.get_many(3, allow_partial=True) == [1, 2]

    def test_get_many_closed_drains_full_batches_first(self):
        q = janus.Queue()
        q.sync_q.put_many([1, 2, 3, 4, 5])
        q.shutdown()
        assert q.sync_q.get_many(2, 2) == [1, 2]
        assert q.sync_q.get_many(2, 2) == [3, 4]
        with pytest.raises(janus.SyncQueueShutDown):
            q.sync_q.get_many(2)
        assert q.sync_q.get_many(2, allow_partial=True) == [5]

    def test_priority_comparison_failure_is_atomic(self):
        q = janus.PriorityQueue()
        q.sync_q.put((1, "a"))
        # equal priorities force payload comparison, str vs int fails
        with pytest.raises(TypeError):
            q.sync_q.put_many([(2, "b"), (1, 5)])
        # no partial commit: neither content nor task counter changed
        assert q.sync_q.qsize() == 1
        assert q.sync_q.unfinished_tasks == 1
        assert q.sync_q.get() == (1, "a")

    def test_priority_get_many_comparison_failure_is_atomic(self):
        class Bad:
            def __lt__(self, other):
                raise RuntimeError("cannot compare")

        q = janus.PriorityQueue()
        q.sync_q.put_many([(1, "a"), (1, "b")])
        q._heap_queue.append((1, Bad()))
        with pytest.raises(RuntimeError, match="cannot compare"):
            q.sync_q.get_many(1, 2)
        assert q.sync_q.qsize() == 3

    def test_task_done_per_record_and_join(self):
        q = janus.Queue()
        q.sync_q.put_many([1, 2, 3])
        assert q.sync_q.unfinished_tasks == 3
        batch = q.sync_q.get_many(1)
        assert batch == [1, 2, 3]
        # get_many does not mark tasks done by itself
        assert q.sync_q.unfinished_tasks == 3

        joined = threading.Event()

        def joiner():
            q.sync_q.join()
            joined.set()

        t = threading.Thread(target=joiner, daemon=True)
        t.start()
        time.sleep(0.1)
        assert not joined.is_set()
        for _ in batch:
            q.sync_q.task_done()
        t.join(timeout=5)
        assert joined.is_set()


class TestAsyncBatch:
    @pytest.mark.asyncio
    async def test_put_many_get_many(self):
        _q = janus.Queue()
        q = _q.async_q
        await q.put_many([1, 2, 3])
        assert q.qsize() == 3
        assert q.unfinished_tasks == 3
        assert await q.get_many(2, 2) == [1, 2]
        assert q.get_many_nowait(1) == [3]
        _q.close()
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_put_many_nowait_full_inserts_nothing(self):
        _q = janus.Queue(maxsize=2)
        q = _q.async_q
        q.put_nowait(1)
        with pytest.raises(janus.AsyncQueueFull):
            q.put_many_nowait([2, 3])
        assert q.qsize() == 1
        assert q.unfinished_tasks == 1
        _q.close()
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_get_many_closed_tail_partial(self):
        _q = janus.Queue()
        q = _q.async_q
        await q.put_many([1, 2])
        _q.shutdown()
        with pytest.raises(janus.AsyncQueueShutDown):
            await q.get_many(3)
        assert await q.get_many(3, allow_partial=True) == [1, 2]
        with pytest.raises(janus.AsyncQueueShutDown):
            q.get_many_nowait(1, allow_partial=True)
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_cancelled_put_many_holds_no_slots(self):
        _q = janus.Queue(maxsize=2)
        q = _q.async_q
        await q.put_many([1, 2])
        putter = asyncio.ensure_future(q.put_many([3, 4]))
        await asyncio.sleep(0.1)
        putter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await putter
        # the cancelled batch inserted nothing and reserved no slots
        assert q.qsize() == 2
        assert q.unfinished_tasks == 2
        assert await q.get() == 1
        q.put_nowait(5)  # would raise QueueFull if a slot was held
        assert q.qsize() == 2
        assert q.unfinished_tasks == 3
        _q.close()
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_cancelled_get_many_loses_no_records(self):
        _q = janus.Queue()
        q = _q.async_q
        getter = asyncio.ensure_future(q.get_many(2, 5))
        await asyncio.sleep(0.1)
        getter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await getter
        await q.put_many([1, 2, 3])
        # the cancelled getter removed nothing
        assert q.qsize() == 3
        assert q.unfinished_tasks == 3
        # join must not finish while the records are unprocessed
        joiner = asyncio.ensure_future(q.join())
        await asyncio.sleep(0.1)
        assert not joiner.done()
        batch = await q.get_many(1, 10)
        assert batch == [1, 2, 3]
        for _ in batch:
            q.task_done()
        await asyncio.wait_for(joiner, timeout=1)
        _q.close()
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_join_waits_for_every_record_of_a_batch(self):
        _q = janus.Queue()
        q = _q.async_q
        await q.put_many([1, 2, 3])
        joiner = asyncio.ensure_future(q.join())
        await asyncio.sleep(0.1)
        assert not joiner.done()
        batch = await q.get_many(3)
        # taking the batch is not enough, every record needs task_done()
        q.task_done()
        q.task_done()
        await asyncio.sleep(0.1)
        assert not joiner.done()
        q.task_done()
        await asyncio.wait_for(joiner, timeout=1)
        _q.close()
        await _q.wait_closed()


@mixed_mode
class TestMixedBatch:
    @pytest.mark.asyncio
    async def test_thread_produces_async_consumes(self):
        _q = janus.Queue(maxsize=4)
        records = list(range(10))

        def producer():
            # every batch is handed over as a single queue operation
            _q.sync_q.put_many(records[:3])
            _q.sync_q.put_many(records[3:7])
            _q.sync_q.put_many(records[7:])

        async def consumer():
            got = []
            while len(got) < len(records):
                batch = await _q.async_q.get_many(2, 4)
                for _ in batch:
                    _q.async_q.task_done()
                got.extend(batch)
            return got

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(producer)
            got = await consumer()
            future.result(timeout=5)
        assert got == records
        await _q.async_q.join()
        _q.close()
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_async_produces_thread_consumes(self):
        _q = janus.Queue(maxsize=4)
        records = list(range(10))

        async def producer():
            await _q.async_q.put_many(records[:3])
            await _q.async_q.put_many(records[3:7])
            await _q.async_q.put_many(records[7:])

        def consumer():
            got = []
            while len(got) < len(records):
                batch = _q.sync_q.get_many(2, 4)
                for _ in batch:
                    _q.sync_q.task_done()
                got.extend(batch)
            return got

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(consumer)
            await producer()
            got = future.result(timeout=5)
        assert got == records
        await _q.async_q.join()
        _q.close()
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_cancelled_async_batch_does_not_disturb_thread_side(self):
        _q = janus.Queue(maxsize=2)
        _q.sync_q.put_many([1, 2])
        putter = asyncio.ensure_future(_q.async_q.put_many([3, 4]))
        await asyncio.sleep(0.1)
        putter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await putter
        # a thread-side consumer/producer pair sees a consistent queue
        with ThreadPoolExecutor(max_workers=1) as pool:
            assert _q.sync_q.qsize() == 2
            batch = await _q.async_q.get_many(1, 1)
            assert batch == [1]
            future = pool.submit(_q.sync_q.put_many, [5])
            future.result(timeout=5)
        assert _q.sync_q.qsize() == 2
        assert _q.sync_q.unfinished_tasks == 2
        _q.close()
        await _q.wait_closed()
