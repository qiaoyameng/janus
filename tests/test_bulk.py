import asyncio
import threading
import queue

import pytest

import janus

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    ("queue_cls", "expected"),
    [
        (janus.Queue, [1, 3, 4, 2]),
        (janus.LifoQueue, [2, 4, 3, 1]),
        (janus.PriorityQueue, [1, 2, 3, 4]),
    ],
)
async def test_sync_put_many_and_get_many_preserve_order(queue_cls, expected):
    q = queue_cls()
    q.sync_q.put_many([1, 3, 4, 2])

    assert q.sync_q.get_many(2, 3) == expected[:3]
    assert q.sync_q.get_many_nowait(1, tail_batch=True) == expected[3:]

    await q.aclose()


async def test_sync_put_many_requires_bounded_sequence():
    q = janus.Queue()
    with pytest.raises(TypeError):
        q.sync_q.put_many(iter([1, 2]))
    with pytest.raises(TypeError):
        q.async_q.put_many_nowait((x for x in [1, 2]))
    assert q.sync_q.empty()
    await q.aclose()


async def test_sync_put_many_full_does_not_insert_partial_batch():
    q = janus.Queue(maxsize=2)
    q.sync_q.put_many([1])
    with pytest.raises(janus.SyncQueueFull):
        q.sync_q.put_many_nowait([2, 3])

    assert q.sync_q.qsize() == 1
    with pytest.raises(ValueError):
        q.sync_q.put_many([1, 2, 3])
    assert q.sync_q.qsize() == 1
    await q.aclose()


async def test_sync_put_many_timeout_does_not_insert_partial_batch():
    q = janus.Queue(maxsize=2)
    q.sync_q.put_many([1])
    with pytest.raises(queue.Full):
        q.sync_q.put_many([2, 3], timeout=0.01)

    assert q.sync_q.qsize() == 1
    await q.aclose()


async def test_async_put_many_full_does_not_insert_partial_batch():
    q = janus.Queue(maxsize=2)
    q.async_q.put_many_nowait([1])
    with pytest.raises(janus.AsyncQueueFull):
        q.async_q.put_many_nowait([2, 3])

    assert q.sync_q.qsize() == 1
    with pytest.raises(ValueError):
        await q.async_q.put_many([1, 2, 3])
    assert q.sync_q.qsize() == 1
    await q.aclose()


async def test_priority_get_many_pops_global_priority_order():
    q = janus.PriorityQueue()
    q.sync_q.put_many([5, 1])
    q.sync_q.put_many([3, 2, 4])

    assert q.sync_q.get_many(2, 4) == [1, 2, 3, 4]
    assert q.sync_q.get_nowait() == 5
    await q.aclose()


async def test_get_many_validates_arguments():
    q = janus.Queue()
    with pytest.raises(ValueError):
        q.sync_q.get_many(0)
    with pytest.raises(ValueError):
        q.sync_q.get_many(2, 1)
    with pytest.raises(ValueError):
        q.sync_q.get_many(1, False)
    with pytest.raises(janus.SyncQueueEmpty):
        q.sync_q.get_many_nowait(1, 2)


async def test_priority_put_many_comparison_failure_is_atomic():
    q = janus.PriorityQueue()
    q.sync_q.put_many([1, 4])
    with pytest.raises(TypeError):
        q.sync_q.put_many([2, "bad"])

    assert q.sync_q.qsize() == 2
    assert q.sync_q.get_many(2) == [1, 4]
    await q.aclose()


async def test_sync_tail_batch_after_shutdown():
    q = janus.Queue()
    q.sync_q.put_many([1, 2])
    q.shutdown()

    with pytest.raises(janus.SyncQueueShutDown):
        q.sync_q.get_many(3)
    assert q.sync_q.get_many(3, tail_batch=True) == [1, 2]
    with pytest.raises(janus.SyncQueueShutDown):
        q.sync_q.get_many(1, tail_batch=True)


async def test_async_tail_batch_after_shutdown():
    q = janus.Queue()
    await q.async_q.put_many([1, 2])
    q.shutdown()

    with pytest.raises(janus.AsyncQueueShutDown):
        await q.async_q.get_many(3)
    assert await q.async_q.get_many(3, tail_batch=True) == [1, 2]
    with pytest.raises(janus.AsyncQueueShutDown):
        await q.async_q.get_many(1, tail_batch=True)


@pytest.mark.parametrize(
    ("queue_cls", "expected"),
    [
        (janus.Queue, [3, 1, 2]),
        (janus.LifoQueue, [2, 1, 3]),
        (janus.PriorityQueue, [1, 2, 3]),
    ],
)
async def test_tail_batch_preserves_order_and_task_count(queue_cls, expected):
    q = queue_cls()
    q.sync_q.put_many([3, 1, 2])
    q.shutdown()

    batch = q.async_q.get_many_nowait(2, 10, tail_batch=True)
    assert batch == expected
    assert q.async_q.unfinished_tasks == 3
    for _ in batch:
        q.async_q.task_done()
    assert q.async_q.unfinished_tasks == 0


async def test_single_put_notifies_waiting_batch_getter():
    q = janus.Queue()
    batch_getter = asyncio.create_task(q.async_q.get_many(2))
    await asyncio.sleep(0.01)
    q.sync_q.put(1)
    q.sync_q.put(2)
    assert await asyncio.wait_for(batch_getter, timeout=0.1) == [1, 2]
    for _ in range(2):
        q.sync_q.task_done()
    await q.aclose()


async def test_single_get_releases_waiting_batch_putter():
    q = janus.Queue(maxsize=3)
    q.sync_q.put_many([1, 2])
    batch_putter = asyncio.create_task(q.async_q.put_many([2, 3]))
    await asyncio.sleep(0.01)
    q.sync_q.get_nowait()
    await asyncio.wait_for(batch_putter, timeout=0.1)
    assert q.sync_q.get_many_nowait(3) == [2, 2, 3]
    await q.aclose()


async def test_sync_producer_async_consumer_batches():
    q = janus.Queue()
    producer_started = threading.Event()
    producer_finished = threading.Event()

    def produce():
        producer_started.set()
        for start in range(0, 8, 4):
            q.sync_q.put_many(list(range(start, start + 4)))
        producer_finished.set()

    async def consume():
        for start in range(0, 8, 4):
            batch = await q.async_q.get_many(2, 4)
            assert batch == list(range(start, start + 4))
            for _ in batch:
                q.async_q.task_done()
        await q.async_q.join()

    producer = threading.Thread(target=produce)
    producer.start()
    producer_started.wait(1)
    await consume()
    producer.join(1)
    await q.aclose()


async def test_async_producer_sync_consumer_batches():
    q = janus.Queue()
    consumer_started = threading.Event()
    consumer_finished = threading.Event()

    def consume():
        consumer_started.set()
        for start in range(0, 8, 4):
            batch = q.sync_q.get_many(2, 4)
            assert batch == list(range(start, start + 4))
            for _ in batch:
                q.sync_q.task_done()
        q.sync_q.join()
        consumer_finished.set()

    async def produce():
        for start in range(0, 8, 4):
            await q.async_q.put_many(list(range(start, start + 4)))

    consumer = threading.Thread(target=consume)
    consumer.start()
    consumer_started.wait(1)
    await produce()
    consumer.join(1)
    await q.aclose()


async def test_cancelled_put_batch_keeps_slot_available():
    q = janus.Queue(maxsize=2)
    q.async_q.put_many_nowait([1])

    async def put_batch():
        await q.async_q.put_many([2, 3])

    task = asyncio.create_task(put_batch())
    await asyncio.sleep(0.01)
    assert q.async_q.unfinished_tasks == 1

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    q.async_q.put_nowait(2)
    assert q.async_q.get_many_nowait(2) == [1, 2]
    assert q.sync_q.qsize() == 0
    await q.aclose()


async def test_cancelled_get_batch_keeps_records_and_join_blocked():
    q = janus.Queue()
    await q.async_q.put_many([1, 2, 3])

    async def get_batch():
        return await q.async_q.get_many(4)

    task = asyncio.create_task(get_batch())
    await asyncio.sleep(0.01)
    assert q.async_q.unfinished_tasks == 3

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert q.async_q.get_many_nowait(3) == [1, 2, 3]
    join_task = asyncio.create_task(q.async_q.join())
    done, pending = await asyncio.wait({join_task}, timeout=0.01)
    assert not done

    for _ in range(3):
        q.async_q.task_done()
    await asyncio.wait_for(join_task, timeout=0.1)
    await q.aclose()


async def test_cancelled_put_batch_does_not_resurrect():
    loop = asyncio.get_running_loop()
    q = janus.Queue(maxsize=2)
    q.sync_q.put_many([1, 2])

    async def put_batch():
        await q.async_q.put_many([3, 4])

    task = asyncio.create_task(put_batch())
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert q.sync_q.get_nowait() == 1
    q.sync_q.task_done()
    q.sync_q.put_nowait(3)
    assert q.sync_q.get_many(2) == [2, 3]
    q.sync_q.task_done()
    q.sync_q.task_done()
    assert q.sync_q.empty()
    await q.aclose()
