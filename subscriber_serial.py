"""
subscriber_serial.py  --  SERIAL / BLOCKING server  (the bottleneck)
====================================================================

on_message() handles ONE message at a time. Each "DB write" (50 ms)
BLOCKS the whole client, so message #100 has to wait while 1..99 are
processed one by one.

  >>> THE KEY LINE:  process_message(data) runs INLINE and BLOCKS. <<<

Expected:  100 messages x 50 ms  ~=  5 seconds.
"""
import paho.mqtt.client as mqtt
import json, time, logging, os

BROKER, PORT, TOPIC = "localhost", 1883, "Testdata"
WORK = 0.05  # 50 ms = simulated DB write / API call
LOG_FILE = "subscriber_serial.log"
LOG_EVERY = 100   # log only every Nth message (keeps high-load logs small)

# --- logging: write to BOTH the console and subscriber_serial.log ------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SERIAL] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("serial")
# -----------------------------------------------------------------------------

state = {"count": 0, "t0": None, "total": None}


def process_message(data):
    """Pretend to write this message to a database."""
    time.sleep(WORK)          # <-- BLOCKING work


def on_connect(client, userdata, flags, reason_code, properties):
    client.subscribe(TOPIC, qos=1)
    log.info("ready on '%s'. Waiting for messages...", TOPIC)


def on_message(client, userdata, msg):
    data = json.loads(msg.payload.decode())

    if state["t0"] is None:                 # first message -> start the clock
        state["t0"] = time.time()
        state["total"] = data["total"]
        log.info("draining %s messages ONE BY ONE...", state["total"])

    process_message(data)                   # <-- BLOCKS here, one at a time

    state["count"] += 1
    # publisher detail -> log file (and console), only every Nth message
    if state["count"] % LOG_EVERY == 0:
        log.info("processed seq=%s/%s  (%s done)",
                 data.get("seq"), data.get("total"), state["count"])

    if state["count"] == state["total"]:    # last message -> stop the clock
        elapsed = time.time() - state["t0"]
        log.info("================ SERIAL RESULT ================")
        log.info("  Messages processed : %s", state["count"])
        log.info("  Total time         : %.2f s", elapsed)
        log.info("  Throughput         : %.0f msg/s", state["count"] / elapsed)
        log.info("===============================================")
        log.info("(Ready for the next batch -- or press Ctrl+C to stop.)")
        # reset so a SECOND publisher run starts a fresh clock & count
        state["count"], state["t0"], state["total"] = 0, None, None


# unique client_id (PID suffix) so it never collides with a leftover/duplicate
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                     client_id=f"serial_sub_{os.getpid()}")
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER, PORT, 60)
client.loop_forever()
