"""Tests for janus.KeyedQueue: a FIFO queue merging items with equal keys."""

import asyncio
import threading
import time

import pytest

import janus


def keyed_queue(maxsize=0):
    return janus.KeyedQueue(maxsize, key=lambda item: item[0])


class TestSyncKeyedQueue:
    @pytest.mark.asyncio
    async def test_merge_keeps_position_and_task_count(self):
        # A1, B1, A2 are put back to back: A2 replaces A1 in place,
        # so the queue reads A2, B1 and counts two tasks, not three.
        _q = keyed_queue()
        q = _q.sync_q
        q.put(("A", 1))
        q.put(("B", 1))
        q.put(("A", 2))
        assert q.qsize() == 2
        assert q.unfinished_tasks == 2
        assert q.get() == ("A", 2)
        assert q.get() == ("B", 1)
        q.task_done()
        q.task_done()
        q.join()
        _q.close()
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_consumed_key_is_new_task(self):
        # A1 is consumed before A2 is put: A2 no longer merges, it is
        # a new tail entry and a new task to be confirmed.
        _q = keyed_queue()
        q = _q.sync_q
        q.put(("A", 1))
        q.put(("B", 1))
        assert q.get() == ("A", 1)
        q.put(("A", 2))
        assert q.qsize() == 2
        assert q.unfinished_tasks == 3
        assert q.get() == ("B", 1)
        assert q.get() == ("A", 2)
        for _ in range(3):
            q.task_done()
        q.join()
        _q.close()
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_thread_get_notified(self):
        _q = keyed_queue()
        q = _q.sync_q
        started = threading.Event()
        result = []

        def getter():
            started.set()
            result.append(q.get())

        t = threading.Thread(target=getter)
        t.start()
        assert started.wait(1)
        time.sleep(0.1)
        q.put(("A", 1))
        t.join(5)
        assert not t.is_alive()
        assert result == [("A", 1)]
        _q.close()
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_thread_join_notified_after_merge(self):
        # join() must unblock after exactly two task_done() calls:
        # the merged put must not have counted as a third task.
        _q = keyed_queue()
        q = _q.sync_q
        q.put(("A", 1))
        q.put(("B", 1))
        q.put(("A", 2))
        done = threading.Event()

        def joiner():
            q.join()
            done.set()

        t = threading.Thread(target=joiner)
        t.start()
        time.sleep(0.1)
        assert not done.is_set()
        assert q.get() == ("A", 2)
        q.task_done()
        time.sleep(0.1)
        assert not done.is_set()
        assert q.get() == ("B", 1)
        q.task_done()
        t.join(5)
        assert not t.is_alive()
        assert done.is_set()
        _q.close()
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_full_queue_merge_and_blocking(self):
        # A full queue still accepts updates to pending keys, while new
        # keys obey the regular blocking rules.
        _q = keyed_queue(1)
        q = _q.sync_q
        q.put_nowait(("A", 1))
        q.put_nowait(("A", 2))
        assert q.qsize() == 1
        assert q.unfinished_tasks == 1
        with pytest.raises(janus.SyncQueueFull):
            q.put_nowait(("B", 1))
        started = threading.Event()

        def putter():
            started.set()
            q.put(("B", 1))

        t = threading.Thread(target=putter)
        t.start()
        assert started.wait(1)
        time.sleep(0.1)
        assert t.is_alive()
        assert q.get() == ("A", 2)
        t.join(5)
        assert not t.is_alive()
        assert q.get() == ("B", 1)
        _q.close()
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_key_function_failure_keeps_state(self):
        def key(item):
            if item[0] == "bad":
                raise RuntimeError("no key")
            return item[0]

        _q = janus.KeyedQueue(key=key)
        q = _q.sync_q
        q.put(("ok", 1))
        with pytest.raises(RuntimeError, match="no key"):
            q.put(("bad", 1))
        assert q.qsize() == 1
        assert q.unfinished_tasks == 1
        assert q.get() == ("ok", 1)
        _q.close()
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_merge_uses_key_not_item_equality(self):
        class Entry:
            def __init__(self, name, value):
                self.name = name
                self.value = value

            def __eq__(self, other):
                return isinstance(other, Entry) and self.value == other.value

            def __hash__(self):
                return hash(self.value)

        _q = janus.KeyedQueue(key=lambda entry: entry.name)
        q = _q.sync_q
        q.put(Entry("A", 1))
        q.put(Entry("B", 1))  # equal by value, different key: new entry
        assert q.qsize() == 2
        q.put(Entry("A", 2))  # same key, not equal: merged
        assert q.qsize() == 2
        assert q.unfinished_tasks == 2
        first = q.get()
        assert (first.name, first.value) == ("A", 2)
        second = q.get()
        assert (second.name, second.value) == ("B", 1)
        _q.close()
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_shutdown_immediate_drains_merged_entries(self):
        _q = keyed_queue()
        q = _q.sync_q
        q.put(("A", 1))
        q.put(("B", 1))
        q.put(("A", 2))
        _q.shutdown(immediate=True)
        assert q.qsize() == 0
        assert q.unfinished_tasks == 0
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_shutdown_keeps_merged_entry_gettable(self):
        _q = keyed_queue()
        q = _q.sync_q
        q.put(("A", 1))
        q.put(("A", 2))
        _q.shutdown()
        assert q.get() == ("A", 2)
        with pytest.raises(janus.SyncQueueShutDown):
            q.get()
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_replace_after_shutdown_raises(self):
        _q = keyed_queue()
        q = _q.sync_q
        q.put(("A", 1))
        _q.shutdown(immediate=True)
        with pytest.raises(janus.SyncQueueShutDown):
            q.put(("A", 2))
        assert q.qsize() == 0
        assert q.unfinished_tasks == 0
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_shutdown_and_replace_race_is_deterministic(self):
        # Whichever side wins the mutex, the outcome is consistent:
        # the replace either happened before draining or raised ShutDown,
        # and the queue always ends up empty with no unfinished tasks.
        for _ in range(50):
            _q = keyed_queue()
            q = _q.sync_q
            q.put(("A", 1))
            errors = []

            def replacer():
                try:
                    q.put(("A", 2))
                except janus.SyncQueueShutDown:
                    pass
                except Exception as exc:
                    errors.append(exc)

            t = threading.Thread(target=replacer)
            t.start()
            _q.shutdown(immediate=True)
            t.join(5)
            assert not t.is_alive()
            assert not errors
            assert q.qsize() == 0
            assert q.unfinished_tasks == 0
            await _q.wait_closed()


class TestAsyncKeyedQueue:
    @pytest.mark.asyncio
    async def test_merge_keeps_position_and_task_count(self):
        _q = keyed_queue()
        q = _q.async_q
        await q.put(("A", 1))
        await q.put(("B", 1))
        await q.put(("A", 2))
        assert q.qsize() == 2
        assert q.unfinished_tasks == 2
        assert await q.get() == ("A", 2)
        assert await q.get() == ("B", 1)
        q.task_done()
        q.task_done()
        await asyncio.wait_for(q.join(), 1)
        await _q.aclose()

    @pytest.mark.asyncio
    async def test_consumed_key_is_new_task(self):
        _q = keyed_queue()
        q = _q.async_q
        q.put_nowait(("A", 1))
        q.put_nowait(("B", 1))
        assert await q.get() == ("A", 1)
        q.put_nowait(("A", 2))
        assert q.qsize() == 2
        assert q.unfinished_tasks == 3
        assert await q.get() == ("B", 1)
        assert await q.get() == ("A", 2)
        for _ in range(3):
            q.task_done()
        await asyncio.wait_for(q.join(), 1)
        await _q.aclose()

    @pytest.mark.asyncio
    async def test_coroutine_get_notified(self):
        _q = keyed_queue()
        q = _q.async_q
        getter = asyncio.ensure_future(q.get())
        await asyncio.sleep(0.05)
        assert not getter.done()
        await q.put(("A", 1))
        assert await asyncio.wait_for(getter, 1) == ("A", 1)
        await _q.aclose()

    @pytest.mark.asyncio
    async def test_coroutine_join_notified_after_merge(self):
        _q = keyed_queue()
        q = _q.async_q
        q.put_nowait(("A", 1))
        q.put_nowait(("B", 1))
        q.put_nowait(("A", 2))
        joiner = asyncio.ensure_future(q.join())
        await asyncio.sleep(0.05)
        assert not joiner.done()
        assert await q.get() == ("A", 2)
        q.task_done()
        await asyncio.sleep(0.05)
        assert not joiner.done()
        assert await q.get() == ("B", 1)
        q.task_done()
        await asyncio.wait_for(joiner, 1)
        await _q.aclose()

    @pytest.mark.asyncio
    async def test_full_queue_merge_and_blocking(self):
        _q = keyed_queue(1)
        q = _q.async_q
        q.put_nowait(("A", 1))
        q.put_nowait(("A", 2))
        assert q.qsize() == 1
        assert q.unfinished_tasks == 1
        with pytest.raises(janus.AsyncQueueFull):
            q.put_nowait(("B", 1))
        putter = asyncio.ensure_future(q.put(("B", 1)))
        await asyncio.sleep(0.05)
        assert not putter.done()
        assert await q.get() == ("A", 2)
        await asyncio.wait_for(putter, 1)
        assert await q.get() == ("B", 1)
        await _q.aclose()

    @pytest.mark.asyncio
    async def test_key_function_failure_keeps_state(self):
        def key(item):
            if item[0] == "bad":
                raise RuntimeError("no key")
            return item[0]

        _q = janus.KeyedQueue(key=key)
        q = _q.async_q
        q.put_nowait(("ok", 1))
        with pytest.raises(RuntimeError, match="no key"):
            q.put_nowait(("bad", 1))
        with pytest.raises(RuntimeError, match="no key"):
            await q.put(("bad", 2))
        assert q.qsize() == 1
        assert q.unfinished_tasks == 1
        assert await q.get() == ("ok", 1)
        await _q.aclose()

    @pytest.mark.asyncio
    async def test_replace_after_shutdown_raises(self):
        _q = keyed_queue()
        q = _q.async_q
        q.put_nowait(("A", 1))
        _q.shutdown(immediate=True)
        with pytest.raises(janus.AsyncQueueShutDown):
            q.put_nowait(("A", 2))
        with pytest.raises(janus.AsyncQueueShutDown):
            await q.put(("A", 3))
        await _q.wait_closed()


class TestMixedKeyedQueue:
    @pytest.mark.asyncio
    async def test_sync_put_async_get(self):
        _q = keyed_queue()
        sq = _q.sync_q
        aq = _q.async_q
        sq.put(("A", 1))
        sq.put(("B", 1))
        sq.put(("A", 2))
        assert aq.qsize() == 2
        assert await aq.get() == ("A", 2)
        assert await aq.get() == ("B", 1)
        await _q.aclose()

    @pytest.mark.asyncio
    async def test_sync_put_notifies_async_getter(self):
        _q = keyed_queue()
        sq = _q.sync_q
        aq = _q.async_q
        getter = asyncio.ensure_future(aq.get())
        await asyncio.sleep(0.05)
        assert not getter.done()
        sq.put(("A", 1))
        assert await asyncio.wait_for(getter, 1) == ("A", 1)
        await _q.aclose()

    @pytest.mark.asyncio
    async def test_async_put_notifies_sync_getter(self):
        _q = keyed_queue()
        sq = _q.sync_q
        aq = _q.async_q
        started = threading.Event()
        result = []

        def getter():
            started.set()
            result.append(sq.get())

        t = threading.Thread(target=getter)
        t.start()
        assert started.wait(1)
        await asyncio.sleep(0.1)
        await aq.put(("A", 1))
        t.join(5)
        assert not t.is_alive()
        assert result == [("A", 1)]
        await _q.aclose()
