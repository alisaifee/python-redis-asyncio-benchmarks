import argparse
from asyncio import gather
import anyio
import time
import functools

import redis

import coredis
import os


KEY = "test"
HASH_KEY = b"test-hash"
LIST_KEY = b"test-list"
VALUE = b"value"


async def get(client, task: int, batch_size: int) -> None:
    if isinstance(client, (coredis.client.Client,)):
        await client.get(KEY)
    else:
        await client.get(KEY)


async def set_get_verify(client, task: int, batch_size: int) -> None:
    await client.set(f"{KEY}:{task}", VALUE)
    val = await client.get(f"{KEY}:{task}")
    assert val == VALUE


async def hgetall(client, task: int, batch_size: int) -> None:
    await client.hgetall(HASH_KEY)


async def blpop(client, task: int, batch_size: int) -> None:
    if task % 4 == 0:
        if isinstance(client, (coredis.client.Client,)):
            await client.lpush(LIST_KEY, [task])
        else:
            await client.lpush(LIST_KEY, task)
    else:
        value = await client.blpop([LIST_KEY], timeout=5)

        if value:
            key, item = value
            assert key == LIST_KEY


async def pubsub_consumer(client, task: int, batch_size: int) -> None:
    if isinstance(client, (coredis.client.Client,)):
        async with client.pubsub(
            channels=["test"], ignore_subscribe_messages=True, subscription_timeout=5
        ) as pubsub:
            messages = []
            async for message in pubsub:
                messages.append(message)

                if len(messages) == batch_size:
                    break
    else:
        async with client.pubsub() as pubsub:
            await pubsub.subscribe("test")
            messages = []

            while len(messages) < batch_size:
                if message := await pubsub.get_message(ignore_subscribe_messages=True):
                    messages.append(message)


async def pipeline(client, transaction: bool, task: int, batch_size: int) -> None:
    if transaction:
        key = "{{pipe}}"
    else:
        key = "pipe"

    if isinstance(client, (coredis.client.Client,)):
        async with client.pipeline(transaction=transaction) as pipeline:
            start = time.time()
            set_results = [
                pipeline.set(f"{key}:{task}:{i}", task) for i in range(batch_size // 2)
            ]
            get_results = [
                pipeline.incr(f"{key}:{task}:{i}") for i in range(batch_size // 2)
            ]
        assert list(pipeline.results[: batch_size // 2]) == [True] * (
            batch_size // 2
        ), (len(pipeline.results), batch_size, pipeline.results[: batch_size // 2])

    else:
        async with client.pipeline(transaction=transaction) as pipeline:
            for i in range(batch_size // 2):
                pipeline = pipeline.set(f"{key}:{task}:{i}", task)
            for i in range(batch_size // 2):
                pipeline = pipeline.incr(f"{key}:{task}:{i}")
            results = await pipeline.execute()
        assert results[: batch_size // 2] == [True] * (batch_size // 2)


async def nosetup(
    client, tasks: int, batch_size: int, consumer_complete: anyio.Event
) -> None:
    pass


async def pubsub_producer(
    client, tasks: int, batch_size: int, consumer_complete: anyio.Event
) -> None:
    await client.ping()
    i = 0

    while not consumer_complete.is_set():
        await client.publish("test", i)
        i += 1


# Global setup step for all workloads
async def global_setup(client, workload: str, tasks: int) -> None:
    await client.flushall()

    await client.set(KEY, "x" * 1024)
    if workload == "hgetall":
        if isinstance(client, (coredis.client.Client,)):
            await client.hset(HASH_KEY, {k: k for k in range(1024)})
        else:
            await client.hset(HASH_KEY, mapping={k: k for k in range(1024)})


async def run_concurrent(
    client: coredis.Redis, tasks: int, batch_size: int, fn, complete: anyio.Event
) -> None:
    await gather(
        *(fn(client=client, task=i, batch_size=batch_size) for i in range(tasks))
    )
    complete.set()


async def run_iterative(
    client: coredis.Redis, tasks: int, batch_size: int, fn, complete: anyio.Event
) -> None:
    for i in range(tasks):
        await fn(client=client, task=i, batch_size=batch_size)
    complete.set()


def build_client_v6(args: argparse.Namespace) -> coredis.Redis | coredis.RedisCluster:
    if args.client == "cluster":
        return coredis.RedisCluster.from_url(
            args.redis_url,
            max_connections=args.max_connections,
            max_connections_per_node=True,
        )

    return coredis.Redis.from_url(
        args.redis_url,
        max_connections=args.max_connections,
    )


def build_client_v5(args: argparse.Namespace) -> coredis.Redis | coredis.RedisCluster:
    if args.client == "cluster":
        pool = coredis.pool.BlockingClusterConnectionPool.from_url(
            args.redis_url,
            max_connections=args.max_connections,
            max_connections_per_node=True,
        )

        return coredis.RedisCluster(connection_pool=pool)

    pool = coredis.pool.BlockingConnectionPool.from_url(
        args.redis_url,
        max_connections=args.max_connections,
    )

    return coredis.Redis(connection_pool=pool)


def build_client_redis(
    args: argparse.Namespace,
) -> redis.asyncio.Redis | redis.asyncio.RedisCluster:
    if args.client == "redispy-cluster":
        return redis.asyncio.RedisCluster.from_url(
            args.redis_url,
            max_connections=args.max_connections,
        )

    else:
        pool = redis.asyncio.BlockingConnectionPool.from_url(
            args.redis_url,
            max_connections=args.max_connections,
        )

    return redis.asyncio.Redis(connection_pool=pool)


async def run(args: argparse.Namespace) -> None:
    map = {
        "get": (get, nosetup),
        "set-get-verify": (set_get_verify, nosetup),
        "hgetall": (hgetall, nosetup),
        "blpop": (blpop, nosetup),
        "pipeline": (functools.partial(pipeline, transaction=False), nosetup),
        "transaction": (functools.partial(pipeline, transaction=True), nosetup),
        "pubsub": (pubsub_consumer, pubsub_producer),
        "pubsub_consumer": (pubsub_consumer, nosetup),
    }

    fn, setupfn = map[args.workload]
    runner = run_concurrent if args.mode == "concurrent" else run_iterative

    timings = []

    for _ in range(args.iterations):
        start = time.perf_counter()
        event = anyio.Event()

        if args.client in {"basic", "cluster"}:
            client = build_client_v6(args)
            producer = build_client_v6(args)
            async with client, producer:
                await global_setup(client, args.workload, args.tasks)
                await gather(
                    setupfn(producer, args.tasks, args.batch_size, event),
                    runner(client, args.tasks, args.batch_size, fn, event),
                )
        elif args.client in {"redispy-basic", "redispy-cluster"}:
            client = build_client_redis(args)
            producer = build_client_redis(args)
            await global_setup(client, args.workload, args.tasks)
            await gather(
                setupfn(producer, args.tasks, args.batch_size, event),
                runner(client, args.tasks, args.batch_size, fn, event),
            )

        timings.append(time.perf_counter() - start)

    average = sum(timings) / len(timings)

    if args.client in {"basic", "cluster"}:
        print(f"coredis version : {coredis.__version__}")
    else:
        print(f"redispy version : {redis.__version__}")
    print(f"Client type     : {args.client}")
    print(f"Mode            : {args.mode}")
    print(f"Workload        : {args.workload}")
    print(f"Tasks           : {args.tasks}")
    print(f"Iterations      : {args.iterations}")
    print(f"Max connections : {args.max_connections}")
    print(f"Avg time (s)    : {average:.4f}")
    print(f"Ops/sec         : {args.tasks / average:,.0f}")


# ------------------------
# CLI
# ------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="coredis simple benchmark")

    parser.add_argument("--redis-url", default="redis://localhost:6379")
    parser.add_argument(
        "--client",
        choices=("basic", "cluster", "redispy-basic", "redispy-cluster"),
        default="basic",
    )
    parser.add_argument(
        "--mode", choices=("concurrent", "iterative"), default="concurrent"
    )
    parser.add_argument("--tasks", type=int, default=1024)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--max-connections", type=int, default=4)
    parser.add_argument(
        "--workload",
        choices=(
            "get",
            "set-get-verify",
            "hgetall",
            "blpop",
            "pipeline",
            "pubsub",
            "pubsub_consumer",
            "transaction",
        ),
        default="get",
    )

    return parser.parse_args()


def main() -> None:
    anyio.run(run, parse_args())


if __name__ == "__main__":
    main()
