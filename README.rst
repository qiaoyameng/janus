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

Weight budget
-------------

In addition to the ``maxsize`` item count limit, a queue can be created
with an optional total *weight* budget to bound the queued data volume
(e.g. bytes of pending payloads) instead of just the number of entries:

.. code:: python

    queue = janus.Queue(maxsize=100,
                        max_weight=16 * 1024 * 1024,
                        weight=lambda item: item.nbytes)

* ``weight`` must be a callable returning a non-negative finite number;
  it is required if and only if ``max_weight`` is set.
* The weight of an item is computed exactly once, before the item is
  enqueued, and is stored along with it.  Mutating the item afterwards
  does not change the accounting.
* A weight function that raises, returns a non-number, or returns a
  negative/NaN/infinite value makes the ``put`` call fail immediately;
  an item heavier than the whole budget is rejected immediately as well.
  Nothing is enqueued and no budget is consumed in these cases.
* Both limits apply at the same time and are shared by the sync and async
  APIs: a put proceeds only when both the item count and the total weight
  fit.  The current usage is exposed as ``current_weight`` on the queue
  and on both queue faces, the configured budget as ``max_weight``.
* Budget is consumed only by a successful enqueue.  A blocked put does
  not reserve budget, so cancelling or timing out a waiting put never
  consumes budget, and a later smaller item that fits may overtake a
  blocked bigger one.
* Budget is released when an item leaves the queue: by a ``get`` (both
  APIs, blocking or not), by draining the remaining items after a
  graceful ``shutdown()``, or all at once by ``shutdown(immediate=True)``
  / ``close()``.  ``task_done()`` does not affect the budget.

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
