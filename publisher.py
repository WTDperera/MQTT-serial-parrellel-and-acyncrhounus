"""
publisher.py  --  THE LOAD GENERATOR  (the "gun")
=================================================

This script PRETENDS to be N IoT devices all publishing at the same
instant. In real life one ESP32 only ever sends its own reading, so a
publisher never needs async. This file exists for ONE reason: to flood
your SERVER so you can measure how the subscriber copes.

  >> It is a TEST TOOL, not production. <<

Every message carries {"seq": i, "total": N} so the subscriber knows
how many to expect and can stop the clock at the last one.

Usage:
    python publisher.py            # 100 messages  (normal test)
    python publisher.py 1000       # 1000 messages (HIGH-LOAD test)
"""
import paho.mqtt.client as mqtt
import json, time, sys

BROKER = "localhost"
PORT   = 1883
TOPIC  = "Testdata"


def main():
    total = int(sys.argv[1]) if len(sys.argv) > 1 else 100

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="load_tester")
    client.connect(BROKER, PORT, 60)
    client.loop_start()

    print(f">>> Firing {total} messages at topic '{TOPIC}' all at once...")
    t0 = time.time()

    infos = []
    for i in range(1, total + 1):
        payload = json.dumps({"seq": i, "total": total})
        infos.append(client.publish(TOPIC, payload, qos=1))

    # wait until every QoS-1 message is confirmed delivered to the broker
    for info in infos:
        info.wait_for_publish()

    elapsed = time.time() - t0
    print(f">>> All {total} messages handed to the broker in {elapsed:.2f} s.")
    print(">>> Now look at the SUBSCRIBER terminal for the processing time.")

    client.loop_stop()
    client.disconnect()


if __name__ == "__main__":
    main()
