"""
subscriber_parallel.py  --  PARALLEL server  (one subscriber + a worker POOL)
=============================================================================

The message-per-second problem lives on the SERVER. This version scales past
one CPU core by fanning the work out to NUM_WORKERS separate OS processes.

  ARCHITECTURE (reliable on any broker):
      ONE MQTT client subscribes to 'Testdata' (a normal, reliable
      subscription) and does NO work itself -- on_message just drops each
      payload onto a multiprocessing queue and returns instantly. A POOL of
      NUM_WORKERS processes pulls from that queue and runs the blocking 50 ms
      "DB write" in parallel, one message at a time per worker.

  >>> THE KEY IDEA: the SERVER, not the broker, distributes the work. <<<

Expected (4 workers): 100 / 4 x 50 ms ~= 1.3 s -- about 4x faster than serial.
Faster than serial, but SLOWER than async here, because this work is
I/O-bound. Parallel only wins when the per-message work is CPU-heavy.

  Why not MQTT "$share/..." shared subscriptions?
      A shared subscription ($share/group/topic) asks the BROKER to round-robin
      messages across many connected subscribers. That is the right tool for
      scaling across MACHINES (several backend instances behind one broker),
      but it is broker-dependent: some brokers drop QoS-1 messages under load,
      others send a burst to a single subscriber. For a reliable, on-ONE-machine
      "use all my cores" demo, a local worker pool is the robust pattern.
"""
import paho.mqtt.client as mqtt
import multiprocessing as mp
import json, time, logging, os

BROKER, PORT, TOPIC = "localhost", 1883, "Testdata"
NUM_WORKERS = 4
WORK = 0.05  # 50 ms = simulated DB write / API call
LOG_FILE = "subscriber_parallel.log"
LOG_EVERY = 100   # log only every Nth message (keeps high-load logs small)


def make_logger(mode):
    """Configure logging to BOTH the console and subscriber_parallel.log.

    The main process clears the file once (mode='w'); each worker process
    appends (mode='a') so the subscriber and all 4 workers share one log file.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [PARALLEL] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, mode=mode, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    return logging.getLogger("parallel")


def worker(worker_id, work_q, counter, first_ts, lock):
    """One process: pull a message off the shared queue and 'write it to the DB'.
    NUM_WORKERS of these run at the same time on different cores."""
    log = make_logger("a")   # append -- shared by the subscriber + all workers

    while True:
        payload = work_q.get()              # blocks until a message is available
        data = json.loads(payload.decode())
        total = data["total"]

        with lock:                          # start the shared clock once
            if first_ts.value == 0.0:
                first_ts.value = time.time()

        time.sleep(WORK)                    # blocking work -- PARALLEL across processes

        with lock:
            counter.value += 1
            done = counter.value

        # publisher detail -> log file (and console), only every Nth message
        if done % LOG_EVERY == 0:
            log.info("worker %s processed seq=%s/%s  (%s done)",
                     worker_id, data.get("seq"), data.get("total"), done)

        if done == total:                   # the worker that finishes last reports
            elapsed = time.time() - first_ts.value
            log.info("=========== PARALLEL RESULT (%s workers) ===========", NUM_WORKERS)
            log.info("  Messages processed : %s", done)
            log.info("  Total time         : %.2f s", elapsed)
            log.info("  Throughput         : %.0f msg/s", done / elapsed)
            log.info("=====================================================")
            log.info("(Ready for the next batch -- or press Ctrl+C to stop.)")
            with lock:                       # reset shared state for the next batch
                counter.value = 0
                first_ts.value = 0.0


def main():
    log = make_logger("w")          # main process clears the log file once

    work_q   = mp.Queue()           # subscriber -> worker pool hand-off
    counter  = mp.Value('i', 0)     # shared message counter
    first_ts = mp.Value('d', 0.0)   # shared "first message" timestamp
    lock     = mp.Lock()

    log.info("starting %s worker processes (local pool)...", NUM_WORKERS)
    procs = []
    for w in range(1, NUM_WORKERS + 1):
        p = mp.Process(target=worker, args=(w, work_q, counter, first_ts, lock),
                       daemon=True)
        p.start()
        procs.append(p)

    # ONE reliable subscriber; it only enqueues -- it never does the slow work
    def on_connect(client, userdata, flags, reason_code, properties):
        client.subscribe(TOPIC, qos=1)
        log.info("ready on '%s'. Waiting for messages...", TOPIC)

    def on_message(client, userdata, msg):
        work_q.put(msg.payload)     # non-blocking hand-off to the pool

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id=f"parallel_sub_{os.getpid()}")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, PORT, 60)
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        pass
    finally:
        for p in procs:
            p.terminate()


if __name__ == "__main__":
    main()
