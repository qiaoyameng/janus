import asyncio
import sys
import threading

from concurrent.futures import ThreadPoolExecutor

import pytest

import janus


class TestMixedMode:
    @pytest.mark.skipif(
        sys.version_info >= (3, 10),
        reason="Python 3.10+ supports delayed initialization",
    )
    def test_ctor_noloop(self):
        with pytest.raises(RuntimeError):
            janus.Queue()

    @pytest.mark.asyncio
    async def test_get_loop_ok(self):
        q = janus.Queue()
        loop = asyncio.get_running_loop()
        assert q._get_loop() is loop
        assert q._loop is loop

    @pytest.mark.asyncio
    async def test_get_loop_different_loop(self):
        q = janus.Queue()
        # emulate binding another loop
        loop = q._loop = asyncio.new_event_loop()
        with pytest.raises(RuntimeError, match="is bound to a different event loop"):
            q._get_loop()
        loop.close()

    @pytest.mark.asyncio
    async def test_maxsize(self):
        q = janus.Queue(5)
        assert 5 == q.maxsize

    @pytest.mark.asyncio
    async def test_maxsize_named_param(self):
        q = janus.Queue(maxsize=7)
        assert 7 == q.maxsize

    @pytest.mark.asyncio
    async def test_maxsize_default(self):
        q = janus.Queue()
        assert 0 == q.maxsize

    @pytest.mark.asyncio
    async def test_unfinished(self):
        q = janus.Queue()
        assert q.sync_q.unfinished_tasks == 0
        assert q.async_q.unfinished_tasks == 0
        q.sync_q.put(1)
        assert q.sync_q.unfinished_tasks == 1
        assert q.async_q.unfinished_tasks == 1
        q.sync_q.get()
        assert q.sync_q.unfinished_tasks == 1
        assert q.async_q.unfinished_tasks == 1
        q.sync_q.task_done()
        assert q.sync_q.unfinished_tasks == 0
        assert q.async_q.unfinished_tasks == 0
        q.close()
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_sync_put_async_get(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue()

        def threaded():
            for i in range(5):
                q.sync_q.put(i)

        async def go():
            f = loop.run_in_executor(None, threaded)
            for i in range(5):
                val = await q.async_q.get()
                assert val == i

            assert q.async_q.empty()

            await f

        for i in range(3):
            await go()

        q.close()
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_sync_put_async_join(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue()

        for i in range(5):
            q.sync_q.put(i)

        async def do_work():
            await asyncio.sleep(0.1)
            while not q.async_q.empty():
                await q.async_q.get()
                q.async_q.task_done()

        task = loop.create_task(do_work())

        async def wait_for_empty_queue():
            await q.async_q.join()
            await task

        await wait_for_empty_queue()

        q.close()
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_async_put_sync_get(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue()

        def threaded():
            for i in range(5):
                val = q.sync_q.get()
                assert val == i

        async def go():
            f = loop.run_in_executor(None, threaded)
            for i in range(5):
                await q.async_q.put(i)

            await f
            assert q.async_q.empty()

        for i in range(3):
            await go()

        q.close()
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_sync_join_async_done(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue()

        def threaded():
            for i in range(5):
                q.sync_q.put(i)
            q.sync_q.join()

        async def go():
            f = loop.run_in_executor(None, threaded)
            for i in range(5):
                val = await q.async_q.get()
                assert val == i
                q.async_q.task_done()

            assert q.async_q.empty()

            await f

        for i in range(3):
            await go()

        q.close()
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_async_join_async_done(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue()

        def threaded():
            for i in range(5):
                val = q.sync_q.get()
                assert val == i
                q.sync_q.task_done()

        async def go():
            f = loop.run_in_executor(None, threaded)
            for i in range(5):
                await q.async_q.put(i)

            await q.async_q.join()

            await f
            assert q.async_q.empty()

        for i in range(3):
            await go()

        q.close()
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_wait_without_closing(self):
        q = janus.Queue()

        with pytest.raises(RuntimeError, match="Waiting for non-closed queue"):
            await q.wait_closed()

        q.close()
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_modifying_forbidden_after_closing(self):
        q = janus.Queue()
        q.close()

        with pytest.raises(
            janus.SyncQueueShutDown
        ):
            q.sync_q.put(5)

        with pytest.raises(
            janus.SyncQueueShutDown
        ):
            q.sync_q.get()

        with pytest.raises(
            janus.AsyncQueueShutDown
        ):
            await q.async_q.put(5)

        with pytest.raises(
            janus.AsyncQueueShutDown
        ):
            q.async_q.put_nowait(5)

        with pytest.raises(
            janus.AsyncQueueShutDown
        ):
            q.async_q.get_nowait()

        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_double_closing(self):
        q = janus.Queue()
        q.close()
        q.close()
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_closed(self):
        q = janus.Queue()
        assert not q.closed
        assert not q.async_q.closed
        assert not q.sync_q.closed
        q.close()
        await q.wait_closed()
        assert q.closed
        assert q.async_q.closed
        assert q.sync_q.closed

    @pytest.mark.asyncio
    async def test_async_join_after_closing(self):
        q = janus.Queue()
        q.close()
        await asyncio.wait_for(q.async_q.join(), timeout=0.1)

        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_close_after_async_join(self):
        q = janus.Queue()
        q.sync_q.put(1)

        task = asyncio.create_task(q.async_q.join())
        await asyncio.sleep(0.01)  # ensure tasks are blocking

        q.close()
        await asyncio.wait_for(task, timeout=0.1)

        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_sync_join_after_closing(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue()
        q.sync_q.put(1)

        q.close()
        await asyncio.wait_for(loop.run_in_executor(None, q.sync_q.join), timeout=0.1)

        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_close_after_sync_join(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue()
        q.sync_q.put(1)

        fut = loop.run_in_executor(None, q.sync_q.join)
        await asyncio.sleep(0.1)  # ensure tasks are blocking

        q.close()

        await asyncio.wait_for(fut, timeout=0.1)

        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_put_notifies_sync_not_empty(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue()

        with ThreadPoolExecutor(4) as executor:
            for _ in range(4):
                executor.submit(q.sync_q.get)

            while q._sync_not_empty_waiting != 4:
                await asyncio.sleep(0.001)

            q.sync_q.put_nowait(1)
            q.async_q.put_nowait(2)
            await loop.run_in_executor(executor, q.sync_q.put, 3)
            await q.async_q.put(4)

        assert q.sync_q.empty()
        await q.aclose()

    @pytest.mark.asyncio
    async def test_put_notifies_async_not_empty(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue()

        tasks = [loop.create_task(q.async_q.get()) for _ in range(4)]

        while q._async_not_empty_waiting != 4:
            await asyncio.sleep(0)

        q.sync_q.put_nowait(1)
        q.async_q.put_nowait(2)
        await loop.run_in_executor(None, q.sync_q.put, 3)
        await q.async_q.put(4)

        await asyncio.gather(*tasks)
        assert q.sync_q.empty()
        await q.aclose()

    @pytest.mark.asyncio
    async def test_get_notifies_sync_not_full(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue(2)
        q.sync_q.put_nowait(1)
        q.sync_q.put_nowait(2)

        with ThreadPoolExecutor(4) as executor:
            for _ in range(4):
                executor.submit(q.sync_q.put, object())

            while q._sync_not_full_waiting != 4:
                await asyncio.sleep(0.001)

            q.sync_q.get_nowait()
            q.async_q.get_nowait()
            await loop.run_in_executor(executor, q.sync_q.get)
            await q.async_q.get()

        assert q.sync_q.qsize() == 2
        await q.aclose()

    @pytest.mark.asyncio
    async def test_get_notifies_async_not_full(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue(2)
        q.sync_q.put_nowait(1)
        q.sync_q.put_nowait(2)

        tasks = [loop.create_task(q.async_q.put(object())) for _ in range(4)]

        while q._async_not_full_waiting != 4:
            await asyncio.sleep(0)

        q.sync_q.get_nowait()
        q.async_q.get_nowait()
        await loop.run_in_executor(None, q.sync_q.get)
        await q.async_q.get()

        await asyncio.gather(*tasks)
        assert q.sync_q.qsize() == 2
        await q.aclose()

    @pytest.mark.asyncio
    async def test_wait_closed_with_pending_tasks(self):
        q = janus.Queue()

        async def getter():
            await q.async_q.get()

        task = asyncio.create_task(getter())
        await asyncio.sleep(0.01)
        q.shutdown()
        # q._pending is not empty now
        await q.wait_closed()

        with pytest.raises(janus.AsyncQueueShutDown):
            await task

    @pytest.mark.asyncio
    @pytest.mark.parametrize("blocking", [False, True])
    async def test_priority_queue_sync_put_rolls_back_failed_sift(
        self, blocking
    ):
        q = janus.PriorityQueue()
        initial = [ExplodingPriority(value) for value in (2, 4, 6, 8, 10)]
        for item in initial:
            q.sync_q.put(item)

        original_heap = list(q._heap_queue)
        bad_item = ExplodingPriority(1, fail_after=1)

        with pytest.raises(TypeError, match="priority comparison failed"):
            if blocking:
                q.sync_q.put(bad_item)
            else:
                q.sync_q.put_nowait(bad_item)

        assert bad_item.fail_after == -1
        assert q.sync_q.qsize() == len(initial)
        assert q.sync_q.unfinished_tasks == len(initial)
        assert q._heap_queue == original_heap
        assert bad_item not in q._heap_queue

        q.sync_q.put(ExplodingPriority(1))
        items = [q.sync_q.get_nowait() for _ in range(len(initial) + 1)]
        assert [item.value for item in items] == [1, 2, 4, 6, 8, 10]
        assert items[1:] == initial
        for item in items:
            q.sync_q.task_done()
        q.sync_q.join()
        assert q.sync_q.unfinished_tasks == 0

        await q.aclose()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("blocking", [False, True])
    async def test_priority_queue_async_put_rolls_back_failed_sift(
        self, blocking
    ):
        q = janus.PriorityQueue()
        initial = [ExplodingPriority(value) for value in (2, 4, 6, 8, 10)]
        for item in initial:
            q.async_q.put_nowait(item)

        original_heap = list(q._heap_queue)
        bad_item = ExplodingPriority(1, fail_after=1)

        with pytest.raises(TypeError, match="priority comparison failed"):
            if blocking:
                await q.async_q.put(bad_item)
            else:
                q.async_q.put_nowait(bad_item)

        assert bad_item.fail_after == -1
        assert q.async_q.qsize() == len(initial)
        assert q.async_q.unfinished_tasks == len(initial)
        assert q._heap_queue == original_heap
        assert bad_item not in q._heap_queue

        await q.async_q.put(ExplodingPriority(1))
        items = [q.async_q.get_nowait() for _ in range(len(initial) + 1)]
        assert [item.value for item in items] == [1, 2, 4, 6, 8, 10]
        assert items[1:] == initial
        for item in items:
            q.async_q.task_done()
        await q.async_q.join()
        assert q.async_q.unfinished_tasks == 0

        await q.aclose()

    @pytest.mark.asyncio
    async def test_priority_queue_failed_sift_usable_by_thread_and_async(self):
        q = janus.PriorityQueue()
        initial = [ExplodingPriority(value) for value in (2, 4, 6, 8, 10)]
        for item in initial:
            q.sync_q.put_nowait(item)

        original_heap = list(q._heap_queue)
        bad_item = ExplodingPriority(1, fail_after=1)
        failure_seen = threading.Event()
        producer_can_continue = threading.Event()
        failures = []

        def producer():
            try:
                q.sync_q.put(bad_item)
            except TypeError as exc:
                failures.append(exc)
                failure_seen.set()
            producer_can_continue.wait()
            for value in (11, 12, 13):
                q.sync_q.put(ExplodingPriority(value))

        async def consume_later():
            values = []
            for _ in range(3):
                item = await q.async_q.get()
                values.append(item.value)
                q.async_q.task_done()
            return values

        producer_thread = threading.Thread(target=producer)
        producer_thread.start()
        failure_seen.wait(1)
        assert failure_seen.is_set()

        assert len(failures) == 1
        assert q.sync_q.qsize() == len(initial)
        assert q.sync_q.unfinished_tasks == len(initial)
        assert q._heap_queue == original_heap
        assert bad_item not in q._heap_queue

        existing_items = [q.sync_q.get_nowait() for _ in initial]
        existing_values = [item.value for item in existing_items]
        for _ in existing_items:
            q.sync_q.task_done()
        q.sync_q.join()

        producer_can_continue.set()
        producer_thread.join(1)
        assert not producer_thread.is_alive()

        sync_values = [
            q.sync_q.get_nowait().value for _ in range(3)
        ]
        for _ in range(3):
            q.sync_q.task_done()
        q.sync_q.join()

        await q.async_q.put(ExplodingPriority(20))
        consumer_task = asyncio.create_task(consume_later())
        await q.async_q.put(ExplodingPriority(21))
        await q.async_q.put(ExplodingPriority(22))

        values = await consumer_task
        await q.async_q.join()

        assert existing_values == [2, 4, 6, 8, 10]
        assert sync_values == [11, 12, 13]
        assert values == [20, 21, 22]
        assert q.async_q.qsize() == 0
        assert q.async_q.unfinished_tasks == 0

        await q.aclose()


class ExplodingPriority:
    def __init__(self, value, fail_after=None):
        self.value = value
        self.fail_after = fail_after

    def __lt__(self, other):
        if self.fail_after is not None:
            self.fail_after -= 1
            if self.fail_after < 0:
                raise TypeError("priority comparison failed")
        return self.value < other.value
