=======
 janus
=======
.. image:: https://github.com/aio-libs/janus/actions/workflows/ci.yml/badge.svg
    :target: https://github.com/aio-libs/janus/actions/workflows/ci.yml
.. image:: https://codecov.io/gh/aio-libs/janus/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/aio-libs/janus
.. image:: https://img.shields.io/pypi/v/janus.svg
    :target: https://pypi.python.org/pypi/janus
.. image:: https://badges.gitter.im/Join%20Chat.svg
    :target: https://gitter.im/aio-libs/Lobby
    :alt: Chat on Gitter



Mixed sync-async queue, supposed to be used for communicating between
classic synchronous (threaded) code and asynchronous (in terms of
`asyncio <https://docs.python.org/3/library/asyncio.html>`_) one.

Like `Janus god <https://en.wikipedia.org/wiki/Janus>`_ the queue
object from the library has two faces: synchronous and asynchronous
interface.

Synchronous is fully compatible with `standard queue
<https://docs.python.org/3/library/queue.html>`_, asynchronous one
follows `asyncio queue design
<https://docs.python.org/3/library/asyncio-queue.html>`_.

Usage
=====

Three queues are available:

* ``Queue``
* ``LifoQueue``
* ``PriorityQueue``

Each has two properties: ``sync_q`` and ``async_q``.

Use the first to get synchronous interface and the second to get asynchronous
one.


Example
-------

.. code:: python

    import asyncio
    import janus


    def threaded(sync_q: janus.SyncQueue[int]) -> None:
        for i in range(100):
            sync_q.put(i)
        sync_q.join()


    async def async_coro(async_q: janus.AsyncQueue[int]) -> None:
        for i in range(100):
            val = await async_q.get()
            assert val == i
            async_q.task_done()


    async def main() -> None:
        queue: janus.Queue[int] = janus.Queue()
        loop = asyncio.get_running_loop()
        fut = loop.run_in_executor(None, threaded, queue.sync_q)
        await async_coro(queue.async_q)
        await fut
        await queue.aclose()


    asyncio.run(main())


Batches
-------

Use ``put_many()`` and ``get_many()`` to hand over a contiguous group as one
queue operation. ``put_many()`` accepts bounded sequences only; a full queue
either accepts the whole batch or, in non-blocking calls, rejects it without
inserting any item. ``get_many(min_items, max_items)`` returns as soon as at
least ``min_items`` records are available.

Thread producer, coroutine consumer:

.. code:: python

    import asyncio
    import threading
    import janus


    def producer(sync_q: janus.SyncQueue[int]) -> None:
        for start in range(0, 12, 4):
            sync_q.put_many(list(range(start, start + 4)))
        sync_q.join()


    async def consumer(async_q: janus.AsyncQueue[int]) -> None:
        for _ in range(3):
            batch = await async_q.get_many(3, 4)
            for item in batch:
                async_q.task_done()


    async def main() -> None:
        queue: janus.Queue[int] = janus.Queue()
        thread = threading.Thread(target=producer, args=(queue.sync_q,))
        thread.start()
        await consumer(queue.async_q)
        thread.join()
        await queue.aclose()


    asyncio.run(main())


Coroutine producer, thread consumer:

.. code:: python

    import asyncio
    import threading
    import janus


    def consumer(sync_q: janus.SyncQueue[int]) -> None:
        for _ in range(3):
            batch = sync_q.get_many(3, 4)
            for item in batch:
                sync_q.task_done()
        sync_q.join()


    async def producer(async_q: janus.AsyncQueue[int]) -> None:
        for start in range(0, 12, 4):
            await async_q.put_many(list(range(start, start + 4)))


    async def main() -> None:
        queue: janus.Queue[int] = janus.Queue()
        thread = threading.Thread(target=consumer, args=(queue.sync_q,))
        thread.start()
        await producer(queue.async_q)
        thread.join()
        await queue.aclose()


    asyncio.run(main())


Every returned record still needs exactly one ``task_done()`` call. After
shutdown, pass ``tail_batch=True`` to consume fewer than ``min_items`` records;
without it, waiting callers receive the regular shutdown exception.

Controlled cancellation does not commit a batch. A waiting put cannot reserve
capacity, and a waiting get cannot remove records:

.. code:: python

    import asyncio
    import janus


    async def main() -> None:
        puts = janus.Queue(maxsize=2)
        puts.async_q.put_many_nowait([1])
        put_task = asyncio.create_task(puts.async_q.put_many([2, 3]))
        await asyncio.sleep(0)
        put_task.cancel()
        try:
            await put_task
        except asyncio.CancelledError:
            pass
        puts.async_q.put_nowait(2)
        assert puts.async_q.get_many_nowait(2) == [1, 2]

        gets = janus.Queue()
        await gets.async_q.put_many([4, 5])
        get_task = asyncio.create_task(gets.async_q.get_many(3))
        await asyncio.sleep(0)
        get_task.cancel()
        try:
            await get_task
        except asyncio.CancelledError:
            pass
        assert gets.async_q.get_many_nowait(2) == [4, 5]
        for _ in range(2):
            gets.async_q.task_done()
        await gets.async_q.join()

        await puts.aclose()
        await gets.aclose()


    asyncio.run(main())


Limitations
===========

This library is built using a classic thread-safe design. The design is
time-tested, but has some limitations.

* Once you are done working with a queue, you must properly close it using
  ``aclose()``. This is because this library creates new tasks to notify other
  threads. If you do not properly close the queue,
  `asyncio may generate error messages
  <https://github.com/aio-libs/janus/issues/574>`_.
* The library has quite good performance only when used as intended, that is,
  for communication between synchronous code and asynchronous one.
  For sync-only and async-only cases, use queues from
  `queue <https://docs.python.org/3/library/queue.html>`_ and
  `asyncio queue <https://docs.python.org/3/library/asyncio-queue.html>`_ modules,
  otherwise `the slowdown can be significant
  <https://github.com/aio-libs/janus/issues/419>`_.
* You cannot use queues for communicating between two different event loops
  because, like all asyncio primitives, they bind to the current one.

Development status is production/stable. The ``janus`` library is maintained to
support the latest versions of Python and fixes, but no major changes will be
made. If your application is performance-sensitive, or if you need any new
features such as ``anyio`` support, try the experimental
`culsans <https://github.com/x42005e1f/culsans>`_ library as an alternative.


Communication channels
======================

GitHub Discussions: https://github.com/aio-libs/janus/discussions

Feel free to post your questions and ideas here.

*gitter chat* https://gitter.im/aio-libs/Lobby


License
=======

``janus`` library is offered under Apache 2 license.

Thanks
======

The library development is sponsored by DataRobot (https://datarobot.com)
