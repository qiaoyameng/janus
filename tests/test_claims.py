import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

import janus


def test_sync_claim_acks_success_and_requeues_failure():
    q = janus.Queue()
    sync_q = q.sync_q
    sync_q.put("record")

    for _ in range(2):
        with pytest.raises(RuntimeError, match="boom"):
            with sync_q.claim(timeout=0) as item:
                assert item == "record"
                raise RuntimeError("boom")
        assert sync_q.qsize() == 1
        assert sync_q.unfinished_tasks == 1

    with sync_q.claim(timeout=0):
        pass

    assert sync_q.empty()
    assert sync_q.unfinished_tasks == 0


def test_sync_claim_keeps_queue_slot():
    q = janus.Queue(maxsize=1)
    sync_q = q.sync_q
    sync_q.put("record")

    with sync_q.claim(timeout=0):
        assert sync_q.full()
        with pytest.raises(janus.SyncQueueFull):
            sync_q.put_nowait("other")

    assert not sync_q.full()


def test_sync_claim_repeated_exit_is_idempotent():
    q = janus.Queue()
    sync_q = q.sync_q
    sync_q.put("record")

    claim = sync_q.claim_nowait()
    with claim as item:
        assert item == "record"
    claim.__exit__(None, None, None)

    assert sync_q.unfinished_tasks == 0


def test_sync_claim_supports_none_item():
    q = janus.Queue()
    sync_q = q.sync_q
    sync_q.put(None)

    with sync_q.claim_nowait() as item:
        assert item is None

    assert sync_q.empty()


@pytest.mark.asyncio
async def test_sync_claim_immediate_shutdown_success_marks_done():
    q = janus.Queue()
    sync_q = q.sync_q
    sync_q.put("record")
    join_returned = threading.Event()

    def join():
        sync_q.join()
        join_returned.set()

    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(1) as executor:
        join_future = loop.run_in_executor(executor, join)
        while (
            q._sync_tasks_done_waiting != 1
            or not q._sync_tasks_done._waiters
        ):
            await asyncio.sleep(0.001)

        with sync_q.claim_nowait():
            q.shutdown(immediate=True)

        assert join_returned.wait(1)
        await join_future

    assert sync_q.unfinished_tasks == 0
    await q.wait_closed()


@pytest.mark.asyncio
async def test_sync_claim_graceful_shutdown_requeues_failure():
    loop = asyncio.get_running_loop()
    q = janus.Queue()
    sync_q = q.sync_q
    sync_q.put("record")

    def process():
        try:
            with sync_q.claim(timeout=0):
                q.shutdown(immediate=False)
                raise RuntimeError("boom")
        except RuntimeError:
            pass

    with ThreadPoolExecutor(1) as executor:
        await loop.run_in_executor(executor, process)

    assert sync_q.qsize() == 1
    assert sync_q.unfinished_tasks == 1
    with pytest.raises(janus.SyncQueueShutDown):
        sync_q.put_nowait("other")
    assert sync_q.get_nowait() == "record"
    await q.aclose()


@pytest.mark.asyncio
async def test_sync_claim_immediate_shutdown_discards_failure():
    loop = asyncio.get_running_loop()
    q = janus.Queue()
    sync_q = q.sync_q
    sync_q.put("record")

    def process():
        try:
            with sync_q.claim(timeout=0):
                q.shutdown(immediate=True)
                raise RuntimeError("boom")
        except RuntimeError:
            pass

    with ThreadPoolExecutor(1) as executor:
        await loop.run_in_executor(executor, process)

    assert sync_q.empty()
    assert sync_q.unfinished_tasks == 0
    await q.aclose()


class TestAsyncClaims:
    @pytest.mark.asyncio
    async def test_two_failures_then_success_unblocks_both_joins(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue()
        async_q = q.async_q
        async_q.put_nowait("record")
        join_started = threading.Event()

        async_join = asyncio.create_task(async_q.join())

        def sync_join():
            join_started.set()
            q.sync_q.join()

        with ThreadPoolExecutor(1) as executor:
            sync_join = loop.run_in_executor(executor, sync_join)
            while not join_started.wait(0):
                await asyncio.sleep(0.001)
            while (
                q._sync_tasks_done_waiting != 1
                or not q._sync_tasks_done._waiters
            ):
                await asyncio.sleep(0.001)
            assert not async_join.done()
            assert not sync_join.done()

            for _ in range(2):
                with pytest.raises(RuntimeError, match="boom"):
                    async with async_q.claim() as item:
                        assert item == "record"
                        assert async_q.qsize() == 0
                        assert async_q.full() is False
                        assert async_q.unfinished_tasks == 1
                        raise RuntimeError("boom")
                assert async_q.qsize() == 1
                assert async_q.unfinished_tasks == 1
                assert not async_join.done()
                assert not sync_join.done()

            async with async_q.claim():
                assert async_q.unfinished_tasks == 1

            await asyncio.sleep(0.01)
            await asyncio.wait_for(async_join, timeout=1)
            await asyncio.wait_for(sync_join, timeout=1)

        await q.aclose()

    @pytest.mark.asyncio
    async def test_async_claim_keeps_queue_slot(self):
        q = janus.Queue(maxsize=1)
        async_q = q.async_q
        async_q.put_nowait("record")

        async with async_q.claim():
            assert async_q.full()
            with pytest.raises(janus.AsyncQueueFull):
                async_q.put_nowait("other")

        assert not async_q.full()
        await q.aclose()

    @pytest.mark.asyncio
    async def test_async_claim_supports_none_item(self):
        q = janus.Queue()
        async_q = q.async_q
        async_q.put_nowait(None)

        async with async_q.claim() as item:
            assert item is None

        assert async_q.empty()
        await q.aclose()

    @pytest.mark.asyncio
    async def test_async_cancellation_requeues_item(self):
        q = janus.Queue()
        async_q = q.async_q
        async_q.put_nowait("record")

        async def process():
            async with async_q.claim():
                await asyncio.Event().wait()

        task = asyncio.create_task(process())
        await asyncio.sleep(0.01)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert async_q.qsize() == 1
        assert async_q.unfinished_tasks == 1
        assert async_q.get_nowait() == "record"
        await q.aclose()

    @pytest.mark.asyncio
    async def test_async_graceful_shutdown_requeues_failure(self):
        q = janus.Queue()
        async_q = q.async_q
        async_q.put_nowait("record")

        with pytest.raises(RuntimeError, match="boom"):
            async with async_q.claim():
                q.shutdown(immediate=False)
                raise RuntimeError("boom")

        assert async_q.qsize() == 1
        assert async_q.unfinished_tasks == 1
        with pytest.raises(janus.AsyncQueueShutDown):
            await async_q.put("other")
        assert async_q.get_nowait() == "record"
        await q.aclose()

    @pytest.mark.asyncio
    async def test_async_immediate_shutdown_discards_failure(self):
        q = janus.Queue()
        async_q = q.async_q
        async_q.put_nowait("record")

        with pytest.raises(RuntimeError, match="boom"):
            async with async_q.claim():
                q.shutdown(immediate=True)
                raise RuntimeError("boom")

        assert async_q.empty()
        assert async_q.unfinished_tasks == 0
        await asyncio.wait_for(async_q.join(), timeout=1)
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_async_immediate_shutdown_success_marks_done(self):
        q = janus.Queue()
        async_q = q.async_q
        async_q.put_nowait("record")
        join_returned = threading.Event()

        def join():
            q.sync_q.join()
            join_returned.set()

        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor(1) as executor:
            join_future = loop.run_in_executor(executor, join)
            while (
                q._sync_tasks_done_waiting != 1
                or not q._sync_tasks_done._waiters
            ):
                await asyncio.sleep(0.001)

            async with async_q.claim_nowait():
                q.shutdown(immediate=True)

            assert join_returned.wait(1)
            await join_future

        assert async_q.unfinished_tasks == 0
        await asyncio.wait_for(async_q.join(), timeout=1)
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_repeated_exit_and_close_race_keep_one_record(self):
        q = janus.Queue()
        async_q = q.async_q
        async_q.put_nowait("record")

        claim = await async_q.claim()
        q.shutdown(immediate=False)
        await claim.__aexit__(RuntimeError, RuntimeError("boom"), None)
        await claim.__aexit__(RuntimeError, RuntimeError("boom"), None)

        assert async_q.qsize() == 1
        assert async_q.get_nowait() == "record"
        await q.aclose()
