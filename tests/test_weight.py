import asyncio
import threading
import time

import pytest

import janus


class TestConstructor:
    def test_weight_without_max_weight(self):
        with pytest.raises(ValueError, match="without max_weight"):
            janus.Queue(weight=lambda x: 1)

    def test_max_weight_without_weight_function(self):
        with pytest.raises(TypeError, match="callable weight"):
            janus.Queue(max_weight=10)

    def test_max_weight_non_callable_weight(self):
        with pytest.raises(TypeError, match="callable weight"):
            janus.Queue(max_weight=10, weight=42)

    @pytest.mark.parametrize("bad", [0, -1, float("nan"), float("inf")])
    def test_invalid_max_weight_value(self, bad):
        with pytest.raises(ValueError, match="positive finite"):
            janus.Queue(max_weight=bad, weight=lambda x: 1)

    @pytest.mark.parametrize("bad", ["10", None, True])
    def test_invalid_max_weight_type(self, bad):
        with pytest.raises((TypeError, ValueError)):
            janus.Queue(max_weight=bad, weight=lambda x: 1)

    def test_defaults_disable_weighting(self):
        q = janus.Queue()
        assert q.max_weight is None
        assert q.sync_q.max_weight is None
        assert q.current_weight == 0

    def test_budget_visible_from_both_ends(self):
        q = janus.Queue(max_weight=10, weight=lambda x: x)
        assert q.max_weight == 10
        assert q.sync_q.max_weight == 10
        assert q.async_q.max_weight == 10


class TestWeightComputation:
    def test_weight_computed_once_and_snapshotted(self):
        calls = []

        class Item:
            def __init__(self, size):
                self.size = size

        def weight_fn(item):
            calls.append(item.size)
            return item.size

        q = janus.Queue(max_weight=100, weight=weight_fn)
        item = Item(40)
        q.sync_q.put(item)
        item.size = 1000  # mutation after enqueue must not change accounting
        assert q.sync_q.current_weight == 40
        assert q.sync_q.get() is item
        assert q.sync_q.current_weight == 0
        assert calls == [40]

    def test_blocking_put_computes_weight_once(self):
        calls = []

        def weight_fn(item):
            calls.append(item)
            return item

        q = janus.Queue(max_weight=10, weight=weight_fn)
        q.sync_q.put(10)
        t = threading.Thread(target=lambda: q.sync_q.put(5))
        t.start()
        time.sleep(0.2)
        # the weight is computed before the put blocks
        assert calls == [10, 5]
        assert q.sync_q.get() == 10
        t.join(timeout=5)
        assert not t.is_alive()
        # waking up does not recompute the weight
        assert calls == [10, 5]
        assert q.sync_q.current_weight == 5

    @pytest.mark.parametrize("bad", [-1, -0.5, float("nan"), float("inf")])
    def test_invalid_weight_value(self, bad):
        q = janus.Queue(max_weight=10, weight=lambda x: bad)
        with pytest.raises(ValueError):
            q.sync_q.put_nowait(object())
        assert q.sync_q.qsize() == 0
        assert q.sync_q.current_weight == 0

    @pytest.mark.parametrize("bad", ["1", None, object(), True])
    def test_invalid_weight_type(self, bad):
        q = janus.Queue(max_weight=10, weight=lambda x: bad)
        with pytest.raises(TypeError):
            q.sync_q.put_nowait(object())
        assert q.sync_q.qsize() == 0
        assert q.sync_q.current_weight == 0

    def test_weight_function_failure(self):
        def boom(item):
            raise RuntimeError("cannot weigh")

        q = janus.Queue(max_weight=10, weight=boom)
        with pytest.raises(RuntimeError, match="cannot weigh"):
            q.sync_q.put_nowait(1)
        assert q.sync_q.qsize() == 0
        assert q.sync_q.current_weight == 0

    def test_item_heavier_than_budget_rejected_immediately(self):
        q = janus.Queue(max_weight=10, weight=lambda x: x)
        with pytest.raises(ValueError, match="exceeds max_weight"):
            q.sync_q.put_nowait(11)
        # a blocking put of an overweight item fails instead of waiting forever
        with pytest.raises(ValueError, match="exceeds max_weight"):
            q.sync_q.put(11)
        assert q.sync_q.qsize() == 0
        assert q.sync_q.current_weight == 0

    @pytest.mark.asyncio
    async def test_async_item_heavier_than_budget_rejected_immediately(self):
        q = janus.Queue(max_weight=10, weight=lambda x: x)
        with pytest.raises(ValueError, match="exceeds max_weight"):
            q.async_q.put_nowait(11)
        with pytest.raises(ValueError, match="exceeds max_weight"):
            await q.async_q.put(11)
        assert q.async_q.qsize() == 0
        q.close()
        await q.wait_closed()


class TestCapacity:
    def test_maxsize_and_weight_both_enforced(self):
        q = janus.Queue(maxsize=2, max_weight=100, weight=lambda x: x)
        q.sync_q.put_nowait(10)
        q.sync_q.put_nowait(10)
        with pytest.raises(janus.SyncQueueFull):
            q.sync_q.put_nowait(1)  # count limit hit first

        q2 = janus.Queue(maxsize=5, max_weight=25, weight=lambda x: x)
        q2.sync_q.put_nowait(10)
        q2.sync_q.put_nowait(10)
        with pytest.raises(janus.SyncQueueFull):
            q2.sync_q.put_nowait(10)  # weight limit hit with count available

    def test_zero_weight_items_only_count_against_maxsize(self):
        q = janus.Queue(maxsize=2, max_weight=10, weight=lambda x: 0)
        q.sync_q.put_nowait("a")
        q.sync_q.put_nowait("b")
        with pytest.raises(janus.SyncQueueFull):
            q.sync_q.put_nowait("c")
        assert q.sync_q.current_weight == 0

    def test_same_object_put_twice(self):
        q = janus.Queue(max_weight=100, weight=lambda x: 10)
        obj = object()
        q.sync_q.put_nowait(obj)
        q.sync_q.put_nowait(obj)
        assert q.sync_q.current_weight == 20
        assert q.sync_q.get() is obj
        assert q.sync_q.current_weight == 10
        assert q.sync_q.get() is obj
        assert q.sync_q.current_weight == 0

    def test_lifo_weight_accounting(self):
        q = janus.LifoQueue(max_weight=30, weight=lambda x: x)
        q.sync_q.put_nowait(10)
        q.sync_q.put_nowait(20)
        assert q.sync_q.current_weight == 30
        assert q.sync_q.get() == 20
        assert q.sync_q.current_weight == 10

    def test_priority_weight_accounting(self):
        q = janus.PriorityQueue(max_weight=30, weight=lambda x: x[1])
        q.sync_q.put_nowait((2, 20))
        q.sync_q.put_nowait((1, 10))
        assert q.sync_q.current_weight == 30
        assert q.sync_q.get() == (1, 10)
        assert q.sync_q.current_weight == 20


class TestBlockingAndCancellation:
    def test_sync_put_timeout_keeps_budget(self):
        q = janus.Queue(max_weight=5, weight=lambda x: x)
        q.sync_q.put(5)
        with pytest.raises(janus.SyncQueueFull):
            q.sync_q.put(1, timeout=0.1)
        assert q.sync_q.qsize() == 1
        assert q.sync_q.current_weight == 5

    def test_small_item_overtakes_blocked_big_put(self):
        # Rule: budget is never reserved by a waiting put.  Any put that
        # fits at the moment it is executed is enqueued immediately, so a
        # small item may overtake a blocked bigger one.
        q = janus.Queue(max_weight=10, weight=lambda x: x)
        q.sync_q.put(5)
        done = []

        def big_put():
            q.sync_q.put(8)
            done.append(True)

        t = threading.Thread(target=big_put)
        t.start()
        time.sleep(0.2)
        assert not done  # 5 + 8 > 10, the big put is blocked

        # the small item fits into the remaining 5 and overtakes the big one
        q.sync_q.put_nowait(3)
        assert q.sync_q.current_weight == 8
        assert q.sync_q.qsize() == 2
        time.sleep(0.2)
        assert not done

        # freeing 5 is not enough: 3 + 8 > 10, the big put keeps waiting
        assert q.sync_q.get() == 5
        time.sleep(0.2)
        assert not done
        assert q.sync_q.current_weight == 3

        # draining the small item finally unblocks the big put
        assert q.sync_q.get() == 3
        t.join(timeout=5)
        assert done
        assert q.sync_q.current_weight == 8
        assert q.sync_q.get() == 8
        assert q.sync_q.current_weight == 0

    @pytest.mark.asyncio
    async def test_cancelled_async_put_keeps_budget(self):
        q = janus.Queue(max_weight=10, weight=lambda x: x)
        await q.async_q.put(10)
        task = asyncio.create_task(q.async_q.put(5))
        await asyncio.sleep(0.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # cancellation did not consume budget nor enqueue anything
        assert q.async_q.qsize() == 1
        assert q.async_q.current_weight == 10
        # the budget is still fully usable afterwards
        assert await q.async_q.get() == 10
        await q.async_q.put(10)
        assert q.async_q.current_weight == 10
        q.close()
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_async_put_unblocked_by_sync_get(self):
        q = janus.Queue(max_weight=10, weight=lambda x: x)
        q.sync_q.put_nowait(10)
        task = asyncio.create_task(q.async_q.put(6))
        await asyncio.sleep(0.1)
        assert not task.done()
        assert q.sync_q.get() == 10
        await asyncio.wait_for(task, timeout=5)
        assert q.async_q.current_weight == 6
        q.close()
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_sync_put_unblocked_by_async_get(self):
        q = janus.Queue(max_weight=10, weight=lambda x: x)
        q.async_q.put_nowait(10)
        done = []

        def blocking_put():
            q.sync_q.put(6)
            done.append(True)

        t = threading.Thread(target=blocking_put)
        t.start()
        await asyncio.sleep(0.2)
        assert not done
        assert await q.async_q.get() == 10
        t.join(timeout=5)
        assert done
        assert q.async_q.current_weight == 6
        q.close()
        await q.wait_closed()


class TestShutdown:
    def test_immediate_shutdown_releases_all_weight(self):
        q = janus.Queue(max_weight=50, weight=lambda x: x)
        q.sync_q.put(20)
        q.sync_q.put(25)
        assert q.sync_q.current_weight == 45
        q.shutdown(immediate=True)
        assert q.sync_q.current_weight == 0
        assert q.sync_q.qsize() == 0

    def test_immediate_shutdown_releases_blocked_sync_put(self):
        q = janus.Queue(max_weight=10, weight=lambda x: x)
        q.sync_q.put(10)
        errors = []

        def putter():
            try:
                q.sync_q.put(5)
            except janus.SyncQueueShutDown:
                errors.append("shutdown")

        t = threading.Thread(target=putter)
        t.start()
        time.sleep(0.2)
        q.shutdown(immediate=True)
        t.join(timeout=5)
        assert errors == ["shutdown"]
        # the unblocked put did not sneak its item in
        assert q.sync_q.qsize() == 0
        assert q.sync_q.current_weight == 0

    @pytest.mark.asyncio
    async def test_immediate_shutdown_releases_blocked_async_put(self):
        q = janus.Queue(max_weight=10, weight=lambda x: x)
        await q.async_q.put(10)
        task = asyncio.create_task(q.async_q.put(5))
        await asyncio.sleep(0.1)
        q.shutdown(immediate=True)
        with pytest.raises(janus.AsyncQueueShutDown):
            await task
        assert q.async_q.qsize() == 0
        assert q.async_q.current_weight == 0
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_graceful_shutdown_drains_weight_per_get(self):
        q = janus.Queue(max_weight=50, weight=lambda x: x)
        await q.async_q.put(20)
        q.sync_q.put_nowait(20)
        q.shutdown()  # non-immediate: remaining items stay gettable
        assert q.sync_q.current_weight == 40
        assert await q.async_q.get() == 20
        assert q.sync_q.current_weight == 20
        assert q.sync_q.get() == 20
        assert q.async_q.current_weight == 0
        with pytest.raises(janus.AsyncQueueShutDown):
            await q.async_q.get()
        with pytest.raises(janus.SyncQueueShutDown):
            q.sync_q.get()
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_put_after_shutdown_raises_without_touching_budget(self):
        q = janus.Queue(max_weight=10, weight=lambda x: x)
        q.shutdown(immediate=True)
        with pytest.raises(janus.SyncQueueShutDown):
            q.sync_q.put(1)
        with pytest.raises(janus.AsyncQueueShutDown):
            await q.async_q.put(1)
        assert q.sync_q.current_weight == 0
        await q.wait_closed()


class TestMixedEnds:
    @pytest.mark.asyncio
    async def test_interleaved_capacity_checks_from_both_ends(self):
        q = janus.Queue(max_weight=60, weight=lambda x: x)
        await q.async_q.put(20)
        q.sync_q.put_nowait(30)
        # both ends share one budget and see the same remaining capacity
        assert q.async_q.current_weight == 50
        assert q.sync_q.current_weight == 50
        with pytest.raises(janus.AsyncQueueFull):
            q.async_q.put_nowait(20)
        with pytest.raises(janus.SyncQueueFull):
            q.sync_q.put_nowait(20)
        q.sync_q.put_nowait(10)  # exactly fills the budget
        assert q.async_q.current_weight == 60
        # a sync get frees budget visible from the async end
        assert q.sync_q.get() == 20
        assert q.async_q.current_weight == 40
        # an async get frees budget visible from the sync end
        assert await q.async_q.get() == 30
        assert q.sync_q.current_weight == 10
        # immediate shutdown drops the rest and releases everything
        q.shutdown(immediate=True)
        assert q.sync_q.current_weight == 0
        assert q.async_q.current_weight == 0
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_sync_producer_async_consumer_shared_budget(self):
        # Note: the consumer polls with get_nowait instead of awaiting
        # async_q.get() because this test environment cannot deliver
        # loop.call_soon_threadsafe() wakeups from foreign threads.
        q = janus.Queue(maxsize=2, max_weight=100, weight=lambda x: x)
        items = [60, 50, 70, 30, 90, 10, 40, 80]

        def producer():
            for it in items:
                q.sync_q.put(it)

        t = threading.Thread(target=producer)
        t.start()
        got = []
        while len(got) < len(items):
            try:
                got.append(q.async_q.get_nowait())
            except janus.AsyncQueueEmpty:
                await asyncio.sleep(0.01)
        t.join(timeout=5)
        assert got == items
        assert q.async_q.current_weight == 0
        assert q.sync_q.current_weight == 0
        q.close()
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_async_producer_sync_consumer_shared_budget(self):
        # The queue is dimensioned so the async producer never blocks:
        # waking a blocked async putter from a foreign thread relies on
        # loop.call_soon_threadsafe(), which this environment cannot deliver.
        q = janus.Queue(maxsize=10, max_weight=1000, weight=lambda x: x)
        items = [60, 50, 70, 30, 90, 10, 40, 80]
        got = []

        def consumer():
            for _ in items:
                got.append(q.sync_q.get())

        t = threading.Thread(target=consumer)
        t.start()
        for it in items:
            await q.async_q.put(it)
        t.join(timeout=5)
        assert got == items
        assert q.async_q.current_weight == 0
        assert q.sync_q.current_weight == 0
        q.close()
        await q.wait_closed()
