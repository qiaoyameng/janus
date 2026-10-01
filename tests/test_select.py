import asyncio
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import pytest

import janus


async def drain_queues(*queues):
    for queue in queues:
        while not queue.empty():
            item = queue.get_nowait()
            queue.task_done()
        queue._parent.close()
        await queue._parent.wait_closed()


class TestSelectGet:
    @pytest.mark.asyncio
    async def test_round_robin_ready_queues(self):
        first = janus.Queue()
        second = janus.Queue()
        third = janus.Queue()
        queues = (first.async_q, second.async_q, third.async_q)

        for index, queue in enumerate(queues):
            queue.put_nowait((index, 0))

        selected, item = await janus.select_get(*queues)
        assert selected is queues[0]
        assert item == (0, 0)
        selected.task_done()
        assert [queue.qsize() for queue in queues] == [0, 1, 1]
        assert [queue.unfinished_tasks for queue in queues] == [0, 1, 1]

        queues[0].put_nowait((0, 1))
        selected, item = await janus.select_get(*queues)
        assert selected is queues[1]
        assert item == (1, 0)
        selected.task_done()
        assert [queue.qsize() for queue in queues] == [1, 0, 1]
        assert [queue.unfinished_tasks for queue in queues] == [1, 0, 1]

        queues[1].put_nowait((1, 1))
        selected, item = await janus.select_get(*queues)
        assert selected is queues[2]
        assert item == (2, 0)
        selected.task_done()
        assert [queue.qsize() for queue in queues] == [1, 1, 0]
        assert [queue.unfinished_tasks for queue in queues] == [1, 1, 0]

        queues[2].put_nowait((2, 1))
        selected, item = await janus.select_get(*queues)
        assert selected is queues[0]
        assert item == (0, 1)
        selected.task_done()
        assert queues[0].empty()
        assert queues[1].get_nowait() == (1, 1)
        assert queues[2].get_nowait() == (2, 1)
        queues[1].task_done()
        queues[2].task_done()
        assert all(queue.unfinished_tasks == 0 for queue in queues)

        await drain_queues(*queues)

    @pytest.mark.asyncio
    async def test_threaded_producers_put_to_multiple_queues(self):
        first = janus.Queue()
        second = janus.Queue()
        third = janus.Queue()
        queues = (first.async_q, second.async_q, third.async_q)
        selected_task = asyncio.ensure_future(janus.select_get(*queues))

        while any(
            len(queue._parent._async_select_get_waiters) != 1 for queue in queues
        ):
            await asyncio.sleep(0)

        barrier = threading.Barrier(3)

        def producer(queue, item):
            barrier.wait()
            queue.sync_q.put(item)

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(producer, queue._parent, index)
                for index, queue in enumerate(queues)
            ]
            selected, item = await selected_task
            for future in futures:
                future.result()

        assert selected in queues
        assert item is queues.index(selected)
        assert selected.qsize() == 0
        assert sum(queue.qsize() for queue in queues) == 2

        selected.task_done()
        for _ in range(2):
            queue, queued_item = await janus.select_get(*queues)
            assert queued_item is queues.index(queue)
            queue.task_done()

        assert all(queue.empty() for queue in queues)
        assert all(queue.unfinished_tasks == 0 for queue in queues)
        await drain_queues(*queues)

    @pytest.mark.asyncio
    async def test_competes_with_normal_get(self):
        first = janus.Queue()
        second = janus.Queue()
        get_task = asyncio.ensure_future(first.async_q.get())

        while first._async_not_empty_waiting != 1:
            await asyncio.sleep(0)

        select_task = asyncio.ensure_future(
            janus.select_get(first.async_q, second.async_q)
        )
        while any(
            len(queue._parent._async_select_get_waiters) != 1
            for queue in (first.async_q, second.async_q)
        ):
            await asyncio.sleep(0)

        first.async_q.put_nowait("normal")
        assert await get_task == "normal"
        assert not select_task.done()
        assert first.async_q.empty()

        second.async_q.put_nowait("select")
        selected, item = await select_task
        assert selected is second.async_q
        assert item == "select"

        await drain_queues(first.async_q, second.async_q)

    @pytest.mark.asyncio
    async def test_cancel_at_wakeup_does_not_consume_item(self):
        first = janus.Queue()
        second = janus.Queue()
        select_task = asyncio.ensure_future(
            janus.select_get(first.async_q, second.async_q)
        )

        while any(
            len(queue._parent._async_select_get_waiters) != 1
            for queue in (first.async_q, second.async_q)
        ):
            await asyncio.sleep(0)

        loop = asyncio.get_running_loop()
        loop.call_soon(first.sync_q.put_nowait, "x")
        select_task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await select_task

        assert not any(
            queue._parent._async_select_get_waiters
            for queue in (first.async_q, second.async_q)
        )
        assert first.sync_q.qsize() == 1
        assert first.sync_q.get_nowait() == "x"

        await drain_queues(first.async_q, second.async_q)

    @pytest.mark.asyncio
    async def test_cancel_recheck_notifies_remaining_waiter(self):
        first = janus.Queue()
        second = janus.Queue()
        third = janus.Queue()
        first_task = asyncio.ensure_future(
            janus.select_get(first.async_q, second.async_q, third.async_q)
        )
        second_task = asyncio.ensure_future(
            janus.select_get(second.async_q, third.async_q)
        )

        while (
            len(first._async_select_get_waiters) != 1
            or len(second._async_select_get_waiters) != 2
            or len(third._async_select_get_waiters) != 2
        ):
            await asyncio.sleep(0)

        first_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first_task

        second.sync_q.put_nowait("second")
        third.sync_q.put_nowait("third")

        selected, item = await second_task
        assert selected is second.async_q
        assert item == "second"
        selected, item = await janus.select_get(
            first.async_q, second.async_q, third.async_q
        )
        assert selected is third.async_q
        assert item == "third"

        await drain_queues(first.async_q, second.async_q, third.async_q)

    @pytest.mark.asyncio
    async def test_timeout_removes_registrations(self):
        first = janus.Queue()
        second = janus.Queue()

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                janus.select_get(first.async_q, second.async_q), 0.01
            )

        assert not any(
            queue._parent._async_select_get_waiters
            for queue in (first.async_q, second.async_q)
        )

        first.sync_q.put_nowait("later")
        selected, item = await janus.select_get(first.async_q, second.async_q)
        assert selected is first.async_q
        assert item == "later"

        await drain_queues(first.async_q, second.async_q)

    @pytest.mark.asyncio
    async def test_shut_down_empty_queues_are_skipped(self):
        first = janus.Queue()
        second = janus.Queue()
        third = janus.Queue()
        queues = (first.async_q, second.async_q, third.async_q)

        first.sync_q.put_nowait("first")
        first.shutdown()
        second.close()

        selected, item = await janus.select_get(*queues)
        assert selected is first.async_q
        assert item == "first"

        loop = asyncio.get_running_loop()
        loop.call_later(
            0.01,
            lambda: (third.sync_q.put_nowait("third"), third.shutdown()),
        )
        selected, item = await janus.select_get(*queues)
        assert selected is third.async_q
        assert item == "third"

        with pytest.raises(janus.AsyncQueueShutDown):
            await janus.select_get(*queues)

        for queue in (first, second, third):
            await queue.wait_closed()

    @pytest.mark.asyncio
    async def test_partial_immediate_close_keeps_waiting_on_other_queue(self):
        first = janus.Queue()
        second = janus.Queue()
        select_task = asyncio.ensure_future(
            janus.select_get(first.async_q, second.async_q)
        )

        while any(
            len(queue._parent._async_select_get_waiters) != 1
            for queue in (first.async_q, second.async_q)
        ):
            await asyncio.sleep(0)

        first.close()
        await asyncio.sleep(0.01)

        assert not first._async_select_get_waiters
        assert len(second._async_select_get_waiters) == 1
        assert not select_task.done()

        second.sync_q.put_nowait("second")
        selected, item = await select_task
        assert selected is second.async_q
        assert item == "second"

        await drain_queues(first.async_q, second.async_q)

    @pytest.mark.asyncio
    async def test_all_closed_wake_returns_shutdown_and_cleans_up(self):
        first = janus.Queue()
        second = janus.Queue()
        select_task = asyncio.ensure_future(
            janus.select_get(first.async_q, second.async_q)
        )

        while any(
            len(queue._parent._async_select_get_waiters) != 1
            for queue in (first.async_q, second.async_q)
        ):
            await asyncio.sleep(0)

        first.close()
        second.close()

        with pytest.raises(janus.AsyncQueueShutDown):
            await select_task
        assert not any(
            queue._parent._async_select_get_waiters
            for queue in (first.async_q, second.async_q)
        )

        for queue in (first, second):
            await queue.wait_closed()

    @pytest.mark.asyncio
    async def test_different_event_loop_rejected_before_registration(self):
        current = janus.Queue()
        foreign_loop = asyncio.new_event_loop()
        foreign = janus.Queue()
        foreign._loop = foreign_loop

        current_waiters = deque(current._async_select_get_waiters)
        try:
            with pytest.raises(RuntimeError, match="different event loop"):
                await janus.select_get(current.async_q, foreign.async_q)
            assert current._async_select_get_waiters == current_waiters
            assert not foreign._async_select_get_waiters
        finally:
            foreign_loop.close()

        await drain_queues(current.async_q)

    @pytest.mark.asyncio
    async def test_invalid_arguments(self):
        queue = janus.Queue()

        with pytest.raises(ValueError, match="at least one queue"):
            await janus.select_get()
        with pytest.raises(ValueError, match="same queue more than once"):
            await janus.select_get(queue.async_q, queue.async_q)
        with pytest.raises(TypeError, match="janus.AsyncQueue"):
            await janus.select_get(object())

        await drain_queues(queue.async_q)
