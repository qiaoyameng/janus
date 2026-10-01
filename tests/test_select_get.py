import asyncio
import threading

import pytest

import janus


async def close_queues(*queues):
    for queue in queues:
        await queue.aclose()


class TestSelectGet:
    @pytest.mark.asyncio
    async def test_ready_queues_rotate_without_touching_losers(self):
        first = janus.Queue()
        second = janus.Queue()
        q0 = first.async_q
        q1 = second.async_q
        q0.put_nowait("a0")
        q0.put_nowait("a1")
        q1.put_nowait("b0")
        q1.put_nowait("b1")

        get_counts = [0, 0]
        original_gets = [first._get, second._get]

        def counted_get(index):
            def get():
                get_counts[index] += 1
                return original_gets[index]()

            return get

        first._get = counted_get(0)
        second._get = counted_get(1)

        selected, item = await janus.select_get(q0, q1)
        assert (selected, item) == (q0, "a0")
        assert get_counts == [1, 0]
        assert list(second._queue) == ["b0", "b1"]
        assert q1.qsize() == 2
        assert q1.unfinished_tasks == 2

        assert await janus.select_get(q0, q1) == (q1, "b0")
        assert list(first._queue) == ["a1"]
        assert q0.qsize() == 1
        assert q0.unfinished_tasks == 2

        assert await janus.select_get(q0, q1) == (q0, "a1")
        assert list(second._queue) == ["b1"]
        assert q1.qsize() == 1
        assert q1.unfinished_tasks == 2

        assert await janus.select_get(q0, q1) == (q1, "b1")
        assert get_counts == [2, 2]
        assert q0.qsize() == 0
        assert q1.qsize() == 0
        assert q0.unfinished_tasks == 2
        assert q1.unfinished_tasks == 2

        await close_queues(first, second)

    @pytest.mark.asyncio
    async def test_method_selects_from_self_and_other_queues(self):
        first = janus.Queue()
        second = janus.Queue()
        second.async_q.put_nowait("second")

        assert await first.async_q.select_get(second.async_q) == (
            second.async_q,
            "second",
        )

        await close_queues(first, second)

    @pytest.mark.asyncio
    async def test_rotation_uses_selected_queue_as_next_start(self):
        queues = [janus.Queue() for _ in range(3)]
        proxies = [queue.async_q for queue in queues]

        proxies[0].put_nowait("a")
        assert await janus.select_get(*proxies) == (proxies[0], "a")

        proxies[2].put_nowait("c")
        assert await janus.select_get(*proxies) == (proxies[2], "c")

        proxies[1].put_nowait("b")
        assert await janus.select_get(*proxies) == (proxies[1], "b")

        proxies[2].put_nowait("c2")
        assert await janus.select_get(*proxies) == (proxies[2], "c2")

        await close_queues(*queues)

    @pytest.mark.asyncio
    async def test_threaded_producers_wake_selector(self):
        first = janus.Queue()
        second = janus.Queue()
        count = 50
        consumed = []
        start = asyncio.Event()

        async def consume():
            start.set()
            for _ in range(count * 2):
                queue, item = await janus.select_get(first.async_q, second.async_q)
                consumed.append((queue, item))

        consumer = asyncio.create_task(consume())
        await start.wait()
        while (
            first._async_not_empty_waiting != 1
            or second._async_not_empty_waiting != 1
        ):
            await asyncio.sleep(0)

        barrier = threading.Barrier(2)

        def produce(queue, label):
            for index in range(count):
                barrier.wait()
                queue.put(f"{label}{index}")

        threads = [
            threading.Thread(target=produce, args=(first.sync_q, "a")),
            threading.Thread(target=produce, args=(second.sync_q, "b")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        await consumer
        assert len(consumed) == count * 2
        assert len({item for _, item in consumed}) == count * 2
        assert first.async_q.qsize() == 0
        assert second.async_q.qsize() == 0
        assert first.async_q.unfinished_tasks == count
        assert second.async_q.unfinished_tasks == count

        await close_queues(first, second)

    @pytest.mark.asyncio
    async def test_competes_with_regular_get(self):
        first = janus.Queue()
        second = janus.Queue()
        q0 = first.async_q
        q1 = second.async_q

        normal_get = asyncio.create_task(q0.get())
        selected = asyncio.create_task(janus.select_get(q0, q1))
        await asyncio.sleep(0.01)

        q0.put_nowait("normal")
        q1.put_nowait("selected")
        done, _pending = await asyncio.wait(
            [normal_get, selected], return_when=asyncio.FIRST_COMPLETED
        )

        if normal_get in done:
            assert normal_get.result() == "normal"
            assert await selected == (q1, "selected")
            normal_get_count = 1
        else:
            assert await selected == (q0, "normal")
            assert list(first._queue) == []
            assert list(second._queue) == ["selected"]
            q0.put_nowait("normal-2")
            assert await normal_get == "normal-2"
            assert q1.get_nowait() == "selected"
            normal_get_count = 2

        await asyncio.gather(normal_get, selected)
        assert q0.qsize() == 0
        assert q1.qsize() == 0
        assert q0.unfinished_tasks == normal_get_count
        assert q1.unfinished_tasks == 1

        await close_queues(first, second)

    @pytest.mark.asyncio
    async def test_timeout_removes_registration_from_all_queues(self):
        first = janus.Queue()
        second = janus.Queue()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                janus.select_get(first.async_q, second.async_q), 0.01
            )

        assert first._async_not_empty_waiting == 0
        assert second._async_not_empty_waiting == 0

        first.sync_q.put("first")
        second.sync_q.put("second")
        assert await janus.select_get(first.async_q, second.async_q) == (
            first.async_q,
            "first",
        )
        assert await janus.select_get(first.async_q, second.async_q) == (
            second.async_q,
            "second",
        )

        await close_queues(first, second)

    @pytest.mark.asyncio
    async def test_cancel_at_wake_removes_no_item(self):
        first = janus.Queue()
        second = janus.Queue()
        q0 = first.async_q
        q1 = second.async_q
        loop = asyncio.get_running_loop()
        selector = asyncio.create_task(janus.select_get(q0, q1))
        await asyncio.sleep(0.01)

        loop.call_soon(selector.cancel)
        first.sync_q.put("first")

        with pytest.raises(asyncio.CancelledError):
            await selector

        assert list(first._queue) == ["first"]
        assert q0.qsize() == 1
        assert q0.unfinished_tasks == 1
        assert first._async_not_empty_waiting == 0
        assert second._async_not_empty_waiting == 0
        assert await janus.select_get(q0, q1) == (q0, "first")

        await close_queues(first, second)

    @pytest.mark.asyncio
    async def test_cancel_during_registration_removes_all_registrations(self):
        queues = [janus.Queue() for _ in range(4)]
        for _ in range(50):
            selector = asyncio.create_task(
                janus.select_get(*(queue.async_q for queue in queues))
            )
            await asyncio.sleep(0)
            selector.cancel()
            with pytest.raises(asyncio.CancelledError):
                await selector
            assert all(queue._async_not_empty_waiting == 0 for queue in queues)

        queues[0].sync_q.put("ready")
        assert await janus.select_get(
            *(queue.async_q for queue in queues)
        ) == (queues[0].async_q, "ready")
        await close_queues(*queues)

    @pytest.mark.asyncio
    async def test_closed_empty_queues_are_skipped(self):
        first = janus.Queue()
        second = janus.Queue()
        q0 = first.async_q
        q1 = second.async_q

        selector = asyncio.create_task(janus.select_get(q0, q1))
        await asyncio.sleep(0.01)
        first.shutdown()
        await asyncio.sleep(0.01)
        assert first._async_not_empty_waiting == 0
        assert second._async_not_empty_waiting == 1

        second.sync_q.put("second")
        assert await selector == (q1, "second")

        second.shutdown()
        with pytest.raises(janus.AsyncQueueShutDown):
            await janus.select_get(q0, q1)

        await first.wait_closed()
        await second.wait_closed()

    @pytest.mark.asyncio
    async def test_graceful_shutdown_drains_remaining_items(self):
        first = janus.Queue()
        second = janus.Queue()
        q0 = first.async_q
        q1 = second.async_q
        q0.put_nowait("left")
        first.shutdown()

        assert await janus.select_get(q0, q1) == (q0, "left")

        selector = asyncio.create_task(janus.select_get(q0, q1))
        await asyncio.sleep(0.01)
        q1.put_nowait("right")
        assert await selector == (q1, "right")

        second.shutdown()
        with pytest.raises(janus.AsyncQueueShutDown):
            await janus.select_get(q0, q1)

        await first.wait_closed()
        await second.wait_closed()

    @pytest.mark.asyncio
    async def test_rejects_queues_from_another_loop_before_state_change(self):
        ready = threading.Event()
        stopped = threading.Event()
        holder = {}

        async def other_loop_main():
            other_queue = janus.Queue()
            other_queue.async_q.put_nowait("other")
            await asyncio.sleep(0)
            holder["queue"] = other_queue
            ready.set()
            await asyncio.to_thread(stopped.wait)

        def run_other_loop():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            holder["loop"] = loop
            try:
                loop.run_until_complete(other_loop_main())
            finally:
                loop.close()

        thread = threading.Thread(target=run_other_loop)
        thread.start()
        ready.wait(1)
        assert ready.is_set()

        first = janus.Queue()
        other_queue = holder["queue"]
        with pytest.raises(RuntimeError, match="same event loop"):
            await janus.select_get(first.async_q, other_queue.async_q)

        assert first.async_q.qsize() == 0
        assert first.async_q.unfinished_tasks == 0
        assert first._async_not_empty_waiting == 0
        assert other_queue.async_q.qsize() == 1
        assert other_queue.async_q.unfinished_tasks == 1
        assert other_queue._async_not_empty_waiting == 0
        assert list(other_queue._queue) == ["other"]

        stopped.set()
        thread.join(1)
        assert not thread.is_alive()

    @pytest.mark.asyncio
    async def test_rejects_invalid_arguments(self):
        queue = janus.Queue()
        with pytest.raises(ValueError, match="at least one queue"):
            await janus.select_get()
        with pytest.raises(TypeError, match="only accepts janus.AsyncQueue"):
            await janus.select_get(queue.sync_q)
        with pytest.raises(ValueError, match="same queue more than once"):
            await janus.select_get(queue.async_q, queue.async_q)

        assert queue._async_not_empty_waiting == 0
        await queue.aclose()
