import asyncio
import contextlib
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

import janus


def by_first(item):
    return item[0]


@contextlib.asynccontextmanager
async def pumped_loop():
    # Keep a timer pending so the event loop regularly wakes up and
    # processes callbacks scheduled from other threads.
    async def pump():
        while True:
            await asyncio.sleep(0.01)

    task = asyncio.create_task(pump())
    try:
        yield
    finally:
        task.cancel()


class TestKeyedSync:
    def test_merge_keeps_position(self):
        q = janus.KeyedQueue(key=by_first)
        q.sync_q.put(("a", 1))
        q.sync_q.put(("b", 1))
        q.sync_q.put(("a", 2))
        assert q.sync_q.qsize() == 2
        assert q.sync_q.unfinished_tasks == 2
        assert q.sync_q.get() == ("a", 2)
        assert q.sync_q.get() == ("b", 1)
        q.sync_q.task_done()
        q.sync_q.task_done()
        q.sync_q.join()

    def test_reput_after_get_is_new_task(self):
        q = janus.KeyedQueue(key=by_first)
        q.sync_q.put(("a", 1))
        assert q.sync_q.get() == ("a", 1)
        q.sync_q.put(("a", 2))
        assert q.sync_q.qsize() == 1
        assert q.sync_q.unfinished_tasks == 2
        assert q.sync_q.get() == ("a", 2)
        q.sync_q.task_done()
        q.sync_q.task_done()
        q.sync_q.join()
        with pytest.raises(ValueError):
            q.sync_q.task_done()

    def test_merge_by_key_not_by_equality(self):
        # equal objects with different keys do not merge
        q = janus.KeyedQueue(key=id)
        q.sync_q.put([1])
        q.sync_q.put([1])
        assert q.sync_q.qsize() == 2
        assert q.sync_q.unfinished_tasks == 2
        # unequal objects with the same key do merge
        q2 = janus.KeyedQueue(key=by_first)
        q2.sync_q.put(["a", 1])
        q2.sync_q.put(["a", 2])
        assert q2.sync_q.qsize() == 1
        assert q2.sync_q.unfinished_tasks == 1
        assert q2.sync_q.get_nowait() == ["a", 2]

    def test_key_failure_keeps_state(self):
        def key(item):
            if item == "boom":
                raise RuntimeError("key failed")
            return item

        q = janus.KeyedQueue(1, key=key)
        q.sync_q.put("a")
        # the queue is full, but the key function fails before blocking
        with pytest.raises(RuntimeError, match="key failed"):
            q.sync_q.put("boom")
        assert q.sync_q.qsize() == 1
        assert q.sync_q.unfinished_tasks == 1
        assert q.sync_q.get_nowait() == "a"

    def test_full_queue_merge_and_new_key_rules(self):
        q = janus.KeyedQueue(1, key=by_first)
        q.sync_q.put(("a", 1))
        # merging an existing key succeeds even on a full queue
        q.sync_q.put_nowait(("a", 2))
        assert q.sync_q.qsize() == 1
        assert q.sync_q.unfinished_tasks == 1
        # a new key follows the regular blocking rules
        with pytest.raises(janus.SyncQueueFull):
            q.sync_q.put_nowait(("b", 1))
        with pytest.raises(janus.SyncQueueFull):
            q.sync_q.put(("b", 1), timeout=0.05)
        with pytest.raises(ValueError):
            q.sync_q.put(("b", 1), timeout=-1)
        assert q.sync_q.get() == ("a", 2)

    def test_blocking_put_new_key_woken_by_get(self):
        q = janus.KeyedQueue(1, key=by_first)
        q.sync_q.put(("a", 1))
        with ThreadPoolExecutor(1) as executor:
            putter = executor.submit(q.sync_q.put, ("b", 1))
            while q._sync_not_full_waiting != 1:
                time.sleep(0.001)
            assert q.sync_q.get() == ("a", 1)
            putter.result(timeout=5)
        assert q.sync_q.get_nowait() == ("b", 1)
        assert q.sync_q.unfinished_tasks == 2

    def test_shutdown_then_replace_raises(self):
        q = janus.KeyedQueue(key=by_first)
        q.sync_q.put(("a", 1))
        q.shutdown()
        with pytest.raises(janus.SyncQueueShutDown):
            q.sync_q.put(("a", 2))
        # the queued value survives a non-immediate shutdown
        assert q.sync_q.get() == ("a", 1)
        q.sync_q.task_done()

    def test_close_drains_merged_slots(self):
        q = janus.KeyedQueue(key=by_first)
        q.sync_q.put(("a", 1))
        q.sync_q.put(("b", 1))
        q.sync_q.put(("a", 2))
        assert q.sync_q.unfinished_tasks == 2
        q.close()
        assert q.sync_q.unfinished_tasks == 0
        q.sync_q.join()
        with pytest.raises(janus.SyncQueueShutDown):
            q.sync_q.get_nowait()


class TestKeyedAsync:
    @pytest.mark.asyncio
    async def test_merge_keeps_position(self):
        q = janus.KeyedQueue(key=by_first)
        await q.async_q.put(("a", 1))
        await q.async_q.put(("b", 1))
        await q.async_q.put(("a", 2))
        assert q.async_q.qsize() == 2
        assert q.async_q.unfinished_tasks == 2
        assert await q.async_q.get() == ("a", 2)
        assert await q.async_q.get() == ("b", 1)
        q.async_q.task_done()
        q.async_q.task_done()
        await asyncio.wait_for(q.async_q.join(), 1)
        await q.aclose()

    @pytest.mark.asyncio
    async def test_full_queue_merge_and_new_key_rules(self):
        q = janus.KeyedQueue(1, key=by_first)
        await q.async_q.put(("a", 1))
        # merging an existing key does not block on a full queue
        await asyncio.wait_for(q.async_q.put(("a", 2)), 1)
        assert q.async_q.qsize() == 1
        assert q.async_q.unfinished_tasks == 1
        # a new key follows the regular blocking rules
        with pytest.raises(janus.AsyncQueueFull):
            q.async_q.put_nowait(("b", 1))
        putter = asyncio.create_task(q.async_q.put(("b", 1)))
        await asyncio.sleep(0.05)
        assert not putter.done()
        assert await q.async_q.get() == ("a", 2)
        await asyncio.wait_for(putter, 1)
        assert await q.async_q.get() == ("b", 1)
        await q.aclose()

    @pytest.mark.asyncio
    async def test_key_failure_keeps_state(self):
        def key(item):
            if item[0] == "boom":
                raise RuntimeError("key failed")
            return item[0]

        q = janus.KeyedQueue(1, key=key)
        await q.async_q.put(("a", 1))
        with pytest.raises(RuntimeError, match="key failed"):
            await asyncio.wait_for(q.async_q.put(("boom", 1)), 1)
        with pytest.raises(RuntimeError, match="key failed"):
            q.async_q.put_nowait(("boom", 1))
        assert q.async_q.qsize() == 1
        assert q.async_q.unfinished_tasks == 1
        assert await q.async_q.get() == ("a", 1)
        await q.aclose()

    @pytest.mark.asyncio
    async def test_close_then_replace_raises(self):
        q = janus.KeyedQueue(key=by_first)
        await q.async_q.put(("a", 1))
        q.close()
        with pytest.raises(janus.AsyncQueueShutDown):
            await q.async_q.put(("a", 2))
        with pytest.raises(janus.AsyncQueueShutDown):
            q.async_q.put_nowait(("a", 2))
        await q.wait_closed()


class TestKeyedMixed:
    @pytest.mark.asyncio
    async def test_sync_get_woken_by_async_put(self):
        q = janus.KeyedQueue(key=by_first)
        with ThreadPoolExecutor(1) as executor:
            getter = executor.submit(q.sync_q.get)
            while q._sync_not_empty_waiting != 1:
                await asyncio.sleep(0.001)
            await q.async_q.put(("a", 1))
            while not getter.done():
                await asyncio.sleep(0.001)
            assert getter.result() == ("a", 1)
        await q.aclose()

    @pytest.mark.asyncio
    async def test_async_get_woken_by_sync_put(self):
        q = janus.KeyedQueue(key=by_first)
        getter = asyncio.create_task(q.async_q.get())
        while q._async_not_empty_waiting != 1:
            await asyncio.sleep(0)
        with ThreadPoolExecutor(1) as executor:
            async with pumped_loop():
                executor.submit(q.sync_q.put, ("a", 1))
                assert await asyncio.wait_for(getter, 5) == ("a", 1)
        await q.aclose()

    @pytest.mark.asyncio
    async def test_blocking_sync_put_woken_by_async_get(self):
        q = janus.KeyedQueue(1, key=by_first)
        q.sync_q.put(("a", 1))
        with ThreadPoolExecutor(1) as executor:
            putter = executor.submit(q.sync_q.put, ("b", 1))
            while q._sync_not_full_waiting != 1:
                await asyncio.sleep(0.001)
            assert await q.async_q.get() == ("a", 1)
            while not putter.done():
                await asyncio.sleep(0.001)
            putter.result()
        assert q.sync_q.get_nowait() == ("b", 1)
        await q.aclose()

    @pytest.mark.asyncio
    async def test_merge_counts_sync_join_async_done(self):
        # A1, B1, A2 are put; A2 replaces A1 so only two tasks are pending
        q = janus.KeyedQueue(key=by_first)
        q.sync_q.put(("a", 1))
        q.sync_q.put(("b", 1))
        q.sync_q.put(("a", 2))
        assert q.sync_q.unfinished_tasks == 2
        with ThreadPoolExecutor(1) as executor:
            joined = executor.submit(q.sync_q.join)
            while q._sync_tasks_done_waiting != 1:
                await asyncio.sleep(0.001)
            assert await q.async_q.get() == ("a", 2)
            q.async_q.task_done()
            await asyncio.sleep(0.05)
            assert not joined.done()
            assert await q.async_q.get() == ("b", 1)
            q.async_q.task_done()
            while not joined.done():
                await asyncio.sleep(0.001)
            joined.result()
        await q.aclose()

    @pytest.mark.asyncio
    async def test_reput_after_get_counts_async_join_sync_done(self):
        # A1 is taken before A2 is put, so A2 is another task to confirm
        q = janus.KeyedQueue(key=by_first)
        q.sync_q.put(("a", 1))
        assert q.sync_q.get() == ("a", 1)
        q.sync_q.put(("a", 2))
        assert q.async_q.unfinished_tasks == 2
        joiner = asyncio.create_task(q.async_q.join())
        await asyncio.sleep(0.05)
        assert not joiner.done()
        assert q.sync_q.get_nowait() == ("a", 2)
        q.sync_q.task_done()
        await asyncio.sleep(0.05)
        assert not joiner.done()
        q.sync_q.task_done()
        await asyncio.wait_for(joiner, 1)
        with pytest.raises(ValueError):
            q.async_q.task_done()
        await q.aclose()
