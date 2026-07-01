"""
subscriber_async.py  --  ASYNC / NON-BLOCKING server  (the fix)
===============================================================

One thread, one connection. Each message is handed to a task and the
loop IMMEDIATELY grabs the next message. All 100 "DB writes" overlap
instead of queueing.

  >>> THE KEY LINE:  asyncio.create_task(...) -- fire and DON'T wait. <<<

Expected:  ~0.1 s  (all 100 writes happen at the same time).
Because MQTT/DB work is I/O-bound, async even beats parallel here.
"""
import asyncio, aiomqtt, json, time, sys, logging

BROKER, PORT, TOPIC = "localhost", 1883, "Testdata"
WORK = 0.05  # 50 ms = simulated DB write / API call
LOG_FILE = "subscriber_async.log"
LOG_EVERY = 100   # log only every Nth message (keeps high-load logs small)

# --- logging: write to BOTH the console and subscriber_async.log -------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ASYNC] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("async")
# -----------------------------------------------------------------------------

# --- Windows fix -------------------------------------------------------------
# On Windows asyncio defaults to the Proactor loop, which has NO add_reader/
# add_writer -> aiomqtt (paho sockets) crashes with NotImplementedError.
# Force the Selector loop, which supports socket callbacks.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
# -----------------------------------------------------------------------------


async def main():
    done = asyncio.Event()
    state = {"count": 0, "t0": None, "total": None}

    async def process_message(data):
        """Pretend to write this message to a database -- non-blocking."""
        await asyncio.sleep(WORK)           # <-- NON-blocking work
        state["count"] += 1
        # publisher detail -> log file (and console), only every Nth message
        if state["count"] % LOG_EVERY == 0:
            log.info("processed seq=%s/%s  (%s done)",
                     data.get("seq"), data.get("total"), state["count"])
        if state["count"] == state["total"]:
            elapsed = time.time() - state["t0"]
            log.info("================ ASYNC RESULT =================")
            log.info("  Messages processed : %s", state["count"])
            log.info("  Total time         : %.2f s", elapsed)
            log.info("  Throughput         : %.0f msg/s", state["count"] / elapsed)
            log.info("===============================================")
            done.set()

    async def consume(client):
        async for message in client.messages:
            data = json.loads(message.payload.decode())
            if state["t0"] is None:         # first message -> start the clock
                state["t0"] = time.time()
                state["total"] = data["total"]
                log.info("draining %s messages CONCURRENTLY...", state["total"])
            asyncio.create_task(process_message(data))   # <-- KEY: don't await

    async with aiomqtt.Client(BROKER, port=PORT) as client:
        await client.subscribe(TOPIC, qos=1)
        log.info("ready on '%s'. Waiting for messages...", TOPIC)
        consumer = asyncio.create_task(consume(client))
        await done.wait()                   # wait until the last message is done
        consumer.cancel()


if __name__ == "__main__":
    asyncio.run(main())
