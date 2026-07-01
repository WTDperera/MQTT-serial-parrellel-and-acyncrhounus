"""
thundering_herd.py  --  RECONNECT STORM vs MITIGATION
=====================================================

Every "device" opens its OWN TCP connection to the broker (unlike
publisher.py, which used one connection). In a real thundering herd,
thousands of devices wake up after an outage and reconnect in the SAME
instant -> CPU spike, connection queue overflows, connections refused.

A production broker won't literally crash on a laptop, so to make the
overload VISIBLE we run the broker with a low limit (max_connections).
What you then see is the broker REFUSING connections -- the real-world
symptom of an overwhelmed server (MQTT's version of HTTP 429).

Modes:
  storm  : all N devices connect at t=0, no retry  -> the herd, many refused
  smart  : each device waits a random JITTER, and retries with EXPONENTIAL
           BACKOFF on failure -> connections spread out, almost all succeed

Usage:
  python thundering_herd.py storm 200
  python thundering_herd.py smart 200
"""
import paho.mqtt.client as mqtt
import threading, time, random, sys

BROKER, PORT = "localhost", 1883
TOPIC = "Testdata"

JITTER_WINDOW = 5.0     # smart mode spreads connections over this many seconds
MAX_ATTEMPTS  = 5       # smart mode retries up to this many times
CONNECT_TIMEOUT = 5.0   # how long to wait for the broker's CONNACK


def try_connect_once(device_id, attempt):
    """Return True if we got a successful CONNACK, else False."""
    connected = threading.Event()
    ok = {"v": False}

    def on_connect(client, userdata, flags, reason_code, properties):
        ok["v"] = not reason_code.is_failure
        connected.set()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id=f"dev_{device_id}_{attempt}")
    client.on_connect = on_connect
    try:
        client.connect(BROKER, PORT, keepalive=30)
        client.loop_start()
        got = connected.wait(timeout=CONNECT_TIMEOUT)
        if got and ok["v"]:
            client.publish(TOPIC, f'{{"device":{device_id}}}', qos=0)
            time.sleep(0.1)
            client.loop_stop(); client.disconnect()
            return True
        client.loop_stop()
        try: client.disconnect()
        except Exception: pass
        return False
    except Exception:
        return False


def device(device_id, mode, res, lock):
    if mode == "smart":
        time.sleep(random.uniform(0, JITTER_WINDOW))      # <-- JITTER

    attempts = MAX_ATTEMPTS if mode == "smart" else 1
    for attempt in range(1, attempts + 1):
        if try_connect_once(device_id, attempt):
            with lock:
                res["ok"] += 1
                res["attempts"] += attempt
            return
        if mode == "smart" and attempt < attempts:
            backoff = (2 ** attempt) * 0.1 + random.uniform(0, 0.3)  # <-- EXP BACKOFF + jitter
            time.sleep(backoff)
    with lock:
        res["fail"] += 1
        res["attempts"] += attempts


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "storm"
    n    = int(sys.argv[2]) if len(sys.argv) > 2 else 200
    assert mode in ("storm", "smart"), "mode must be 'storm' or 'smart'"

    res = {"ok": 0, "fail": 0, "attempts": 0}
    lock = threading.Lock()

    print(f">>> {mode.upper()} : {n} devices connecting "
          f"{'ALL AT ONCE' if mode == 'storm' else 'with jitter + backoff'}...")
    t0 = time.time()

    threads = [threading.Thread(target=device, args=(i, mode, res, lock))
               for i in range(1, n + 1)]
    for t in threads: t.start()
    for t in threads: t.join()

    elapsed = time.time() - t0
    print(f"\n=============== {mode.upper()} RESULT ===============")
    print(f"  Devices              : {n}")
    print(f"  Connected OK         : {res['ok']}")
    print(f"  Refused / failed     : {res['fail']}")
    print(f"  Total connect attempts: {res['attempts']}")
    print(f"  Wall time            : {elapsed:.2f} s")
    print("=" * (24 + len(mode)))
    if res["fail"]:
        print(f"  -> {res['fail']} devices were turned away. Server overwhelmed.")
    else:
        print("  -> Every device got in. Spreading the load saved the server.")


if __name__ == "__main__":
    main()
