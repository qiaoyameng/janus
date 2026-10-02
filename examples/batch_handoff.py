"""Batch handoff between threads and coroutines with put_many()/get_many().

A sampling job produces records in batches; every batch crosses the
sync/async boundary as a single queue operation, so a consumer never
observes half of a batch.  Every record still gets its own task_done(),
and join() only unblocks once each record of each batch is processed.

Run:  python examples/batch_handoff.py
"""

import asyncio
import threading

import janus


def thread_producer(q, batches):
    """Runs in a plain thread: hand over whole batches at once."""
    for batch in batches:
        q.sync_q.put_many(batch)  # blocks until the *whole* batch fits
    q.sync_q.shutdown()  # no more batches: let the consumer drain the tail


async def async_consumer(q):
    """Runs in the event loop: take batches of 2..4 records."""
    processed = []
    while True:
        try:
            # wait for at least 2 records, take at most 4;
            # after shutdown, explicitly accept the smaller tail batch
            batch = await q.async_q.get_many(2, 4, allow_partial=True)
        except janus.AsyncQueueShutDown:
            break
        for record in batch:
            processed.append(record)
            q.async_q.task_done()  # one task_done() per record
    return processed


async def async_producer(q, batches):
    for batch in batches:
        await q.async_q.put_many(batch)
    await q.async_q.join()  # waits until every record is task_done()'d
    q.async_q.shutdown()


def thread_consumer(q):
    processed = []
    while True:
        try:
            batch = q.sync_q.get_many(2, 4, allow_partial=True)
        except janus.SyncQueueShutDown:
            break
        for record in batch:
            processed.append(record)
            q.sync_q.task_done()
    return processed


async def main():
    # thread -> coroutine
    q1 = janus.Queue(maxsize=4)
    batches = [[0, 1, 2], [3, 4, 5, 6], [7, 8, 9]]
    producer = threading.Thread(target=thread_producer, args=(q1, batches))
    producer.start()
    got = await async_consumer(q1)
    producer.join()
    print("thread -> coroutine:", got)

    # coroutine -> thread
    q2 = janus.Queue(maxsize=4)
    got2 = []
    consumer = threading.Thread(
        target=lambda: got2.extend(thread_consumer(q2))
    )
    consumer.start()
    await async_producer(q2, batches)
    consumer.join()
    print("coroutine -> thread:", got2)


asyncio.run(main())
