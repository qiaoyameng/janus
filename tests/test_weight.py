import asyncio
import queue
import threading

import pytest

import janus


class PutterThread(threading.Thread):
    def __init__(self, q, item, **kwargs):
        super().__init__(daemon=True)
        self.q = q
        self.item = item
        self.kwargs = kwargs
        self.started = threading.Event()
        self.error = None
        self.finished = threading.Event()

    def run(self):
        self.started.set()
        try:
            self.q.put(self.item, **self.kwargs)
        except BaseException as exc:
            self.error = exc
        finally:
            self.finished.set()


@pytest.mark.asyncio
async def test_weight_is_stored_once():
    q = janus.Queue(weight=lambda item: item[1], max_weight=10)

    item = ["small", 3]
    q.sync_q.put_nowait(item)
    item[1] = 99

    assert q.sync_q.current_weight == 3
    assert q.sync_q.free_weight == 7
    assert q.async_q.current_weight == 3
    assert q.async_q.get_nowait() is item
    assert q.current_weight == 0
    assert q.free_weight == 10
    await q.aclose()


@pytest.mark.asyncio
async def test_weight_callback_runs_once_before_enqueue():
    calls = []
    q = janus.Queue(weight=lambda item: calls.append(item) or item, max_weight=5)

    q.sync_q.put_nowait(3)
    assert calls == [3]
    assert q.async_q.get_nowait() == 3
    assert calls == [3]

    await q.aclose()


def test_weight_constructor_arguments_are_validated():
    with pytest.raises(TypeError, match="weight must be callable"):
        janus.Queue(weight=4)
    with pytest.raises(ValueError, match="max_weight requires a weight function"):
        janus.Queue(max_weight=4)
    with pytest.raises(ValueError, match="max_weight must be a positive integer"):
        janus.Queue(weight=lambda item: item, max_weight=0)
    with pytest.raises(ValueError, match="max_weight must be a positive integer"):
        janus.Queue(weight=lambda item: item, max_weight=1.5)


@pytest.mark.parametrize("bad_weight", [-1, 1.5, "heavy", None])
@pytest.mark.asyncio
async def test_invalid_weight_is_rejected(bad_weight):
    q = janus.Queue(weight=lambda item: bad_weight, max_weight=5)

    with pytest.raises(ValueError):
        q.sync_q.put_nowait(1)
    with pytest.raises(ValueError):
        q.async_q.put_nowait(1)
    assert q.current_weight == 0

    task = asyncio.create_task(q.async_q.put(1))
    with pytest.raises(ValueError):
        await task
    assert q.current_weight == 0
    await q.aclose()


@pytest.mark.asyncio
async def test_weight_function_error_is_rejected():
    def weight(item):
        raise RuntimeError("cannot measure")

    q = janus.Queue(weight=weight, max_weight=5)

    with pytest.raises(RuntimeError, match="cannot measure"):
        q.sync_q.put(1, timeout=0.01)
    with pytest.raises(RuntimeError, match="cannot measure"):
        await q.async_q.put(1)
    assert q.current_weight == 0
    assert q.free_weight == 5
    await q.aclose()


@pytest.mark.parametrize("put", ["sync", "async"])
@pytest.mark.asyncio
async def test_item_heavier_than_budget_is_never_enqueued(put):
    q = janus.Queue(weight=lambda item: item, max_weight=5)

    with pytest.raises(ValueError, match="exceeds max_weight"):
        if put == "sync":
            q.sync_q.put_nowait(6)
        else:
            q.async_q.put_nowait(6)

    task = asyncio.create_task(q.async_q.put(6))
    with pytest.raises(ValueError, match="exceeds max_weight"):
        await task
    assert q.sync_q.qsize() == 0
    assert q.current_weight == 0
    await q.aclose()


@pytest.mark.asyncio
async def test_count_and_weight_limits_both_apply():
    q = janus.Queue(maxsize=2, weight=lambda item: item, max_weight=10)

    q.sync_q.put_nowait(4)
    q.async_q.put_nowait(4)

    assert q.sync_q.qsize() == 2
    assert q.current_weight == 8
    assert q.sync_q.full()

    with pytest.raises(queue.Full):
        q.sync_q.put_nowait(1)
    with pytest.raises(asyncio.QueueFull):
        q.async_q.put_nowait(1)

    assert q.sync_q.get_nowait() == 4
    assert not q.sync_q.full()

    q.sync_q.put_nowait(2)
    assert q.current_weight == 6
    await q.aclose()


@pytest.mark.asyncio
async def test_weight_capacity_is_shared_between_sync_and_async_ends():
    q = janus.Queue(weight=lambda item: item, max_weight=10)

    q.sync_q.put_nowait(4)
    assert q.async_q.current_weight == 4
    assert q.async_q.free_weight == 6

    assert q.async_q.get_nowait() == 4
    assert q.sync_q.current_weight == 0
    assert q.sync_q.free_weight == 10

    q.async_q.put_nowait(3)
    assert q.sync_q.current_weight == 3
    assert q.sync_q.get_nowait() == 3
    assert q.async_q.free_weight == 10
    await q.aclose()


@pytest.mark.asyncio
async def test_sync_waiting_large_item_blocks_later_async_small_item():
    q = janus.Queue(weight=lambda item: item[1], max_weight=15)
    q.async_q.put_nowait(("occupied", 6))

    large = PutterThread(q.sync_q, ("large", 8))
    large.start()
    large.started.wait()
    await asyncio.sleep(0.05)

    with pytest.raises(asyncio.QueueFull):
        q.async_q.put_nowait(("small", 5))

    small_task = asyncio.create_task(q.async_q.put(("small", 5)))
    await asyncio.sleep(0.05)
    assert not small_task.done()
    assert q.current_weight == 6
    assert q.free_weight == 9

    assert q.sync_q.get_nowait() == ("occupied", 6)
    assert q.current_weight == 0
    assert large.finished.wait(1)
    assert not small_task.done()
    assert large.error is None
    assert q.current_weight == 8

    await small_task
    assert q.current_weight == 13
    assert q.free_weight == 2
    assert q.sync_q.get_nowait() == ("large", 8)
    assert q.current_weight == 5
    assert q.async_q.get_nowait() == ("small", 5)
    assert q.current_weight == 0
    await q.aclose()


@pytest.mark.asyncio
async def test_async_waiting_large_item_blocks_later_sync_small_item():
    q = janus.Queue(weight=lambda item: item[1], max_weight=10)
    q.sync_q.put_nowait(("occupied", 6))

    large = asyncio.create_task(q.async_q.put(("large", 8)))
    await asyncio.sleep(0.05)

    with pytest.raises(queue.Full):
        q.sync_q.put(("small", 5), block=False)

    small = PutterThread(q.sync_q, ("small", 5))
    small.start()
    small.started.wait()
    await asyncio.sleep(0.05)
    assert not small.finished.is_set()
    assert q.current_weight == 6

    assert q.async_q.get_nowait() == ("occupied", 6)
    assert q.current_weight == 0
    await asyncio.sleep(0.05)
    assert not small.finished.is_set()
    await large
    assert q.current_weight == 8
    assert q.sync_q.get_nowait() == ("large", 8)
    assert small.finished.wait(1)
    assert small.error is None
    assert q.current_weight == 5

    assert q.sync_q.get_nowait() == ("small", 5)
    assert q.current_weight == 0
    await q.aclose()


@pytest.mark.asyncio
async def test_async_cancelled_wait_does_not_consume_weight_or_block_queue():
    q = janus.Queue(weight=lambda item: item, max_weight=5)
    q.sync_q.put_nowait(4)

    task = asyncio.create_task(q.async_q.put(3))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert q.current_weight == 4
    assert q.free_weight == 1
    q.sync_q.put_nowait(1)
    assert q.current_weight == 5
    await q.aclose()


@pytest.mark.asyncio
async def test_sync_timeout_does_not_consume_weight_or_block_queue():
    q = janus.Queue(weight=lambda item: item, max_weight=5)
    q.async_q.put_nowait(4)

    with pytest.raises(queue.Full):
        q.sync_q.put(3, timeout=0.05)

    assert q.current_weight == 4
    assert q.free_weight == 1
    q.sync_q.put_nowait(1)
    assert q.current_weight == 5
    await q.aclose()


@pytest.mark.asyncio
async def test_sync_predecessor_timeout_keeps_waiting_queue_open():
    q = janus.Queue(weight=lambda item: item, max_weight=10)
    q.async_q.put_nowait(6)

    first = PutterThread(q.sync_q, 7)
    second = PutterThread(q.sync_q, 2, timeout=0.05)
    first.start()
    first.started.wait()
    second.start()
    second.started.wait()

    assert second.finished.wait(1)
    assert isinstance(second.error, queue.Full)
    assert q.current_weight == 6

    assert q.async_q.get_nowait() == 6
    assert first.finished.wait(1)
    assert first.error is None
    assert q.current_weight == 7
    await q.aclose()


@pytest.mark.asyncio
async def test_get_releases_weight_under_graceful_shutdown():
    q = janus.Queue(weight=lambda item: item, max_weight=5)
    q.sync_q.put_nowait(3)
    q.shutdown()

    assert q.sync_q.get_nowait() == 3
    assert q.current_weight == 0
    assert q.free_weight == 5
    with pytest.raises(janus.SyncQueueShutDown):
        q.sync_q.get_nowait()
    await q.wait_closed()


@pytest.mark.asyncio
async def test_immediate_shutdown_drains_and_releases_all_weight():
    q = janus.Queue(weight=lambda item: item, max_weight=5)
    q.sync_q.put_nowait(2)
    q.async_q.put_nowait(3)

    waiter = asyncio.create_task(q.async_q.put(1))
    await asyncio.sleep(0.05)
    q.close()

    with pytest.raises(janus.AsyncQueueShutDown):
        await waiter
    assert q.sync_q.qsize() == 0
    assert q.current_weight == 0
    assert q.free_weight == 5
    assert q.sync_q.unfinished_tasks == 0
    await q.wait_closed()


@pytest.mark.asyncio
async def test_immediate_shutdown_releases_sync_waiter_without_enqueue():
    q = janus.Queue(weight=lambda item: item, max_weight=5)
    q.sync_q.put_nowait(4)
    waiter = PutterThread(q.sync_q, 3)
    waiter.start()
    waiter.started.wait()
    await asyncio.sleep(0.05)

    q.close()
    assert waiter.finished.wait(1)
    assert isinstance(waiter.error, janus.SyncQueueShutDown)
    assert q.sync_q.qsize() == 0
    assert q.current_weight == 0
    assert q.free_weight == 5
    await q.wait_closed()


@pytest.mark.asyncio
async def test_weighted_lifo_and_priority_queues_unwrap_items():
    lifo = janus.LifoQueue(weight=lambda item: item[1], max_weight=5)
    lifo.sync_q.put_nowait(("a", 2))
    lifo.sync_q.put_nowait(("b", 2))
    assert lifo.sync_q.get_nowait() == ("b", 2)
    assert lifo.current_weight == 2
    await lifo.aclose()

    priority = janus.PriorityQueue(weight=lambda item: item[1][1], max_weight=5)
    priority.sync_q.put_nowait((2, ("b", 2)))
    priority.sync_q.put_nowait((1, ("a", 2)))
    assert priority.sync_q.get_nowait() == (1, ("a", 2))
    assert priority.current_weight == 2
    await priority.aclose()
