# MQTT: Serial vs Parallel vs Async — on the SERVER side

A practical demo for your sir. It proves **where** the concurrency problem
actually lives and **how** each approach handles a flood of messages.

---

## The one idea this demo proves

A **publisher (ESP32)** sends only its own reading. One connection, one tiny
stream — serial is fine, it never has a load problem.

The load problem lives on the **server**: the subscriber that 100s–1000s of
devices hit at once. So `serial / parallel / async` is a question about **how
the SERVER processes incoming messages**, not how a device sends.

`publisher.py` here is NOT the architecture — it is a **load-testing tool**
(a "gun") that pretends to be N devices so we can flood the server and measure it.

---

## Files

| File | Role | What it shows |
|---|---|---|
| `publisher.py` | The load generator (the gun) | Fires N messages at the broker at once |
| `subscriber_serial.py` | Server, **SERIAL** | Processes one message at a time — the bottleneck |
| `subscriber_parallel.py` | Server, **PARALLEL** | 4 worker processes + MQTT 5 shared subscription |
| `subscriber_async.py` | Server, **ASYNC** | One thread, all messages overlap — the fix |
| `thundering_herd.py` | Reconnect **storm** + mitigation | Many devices connect at once → broker refuses; jitter+backoff fixes it |
| `mosquitto_limited.conf` | Broker with a low limit | Makes the overload visible (`max_connections 50`) |
| `requirements.txt` | Python deps | `paho-mqtt`, `aiomqtt` |

Every subscriber simulates the **same** work per message: a 50 ms "DB write".
The ONLY thing that changes between the three files is **how messages are
dispatched to that work**. That single difference is the whole lesson.

---

## Tested results (real runs, mosquitto broker)

| Mode | 100 messages | 1000 messages (high load) |
|---|---|---|
| **Serial** | **5.03 s** (20 msg/s) | **~50 s** (grows linearly) |
| **Parallel** (4 workers) | **1.26 s** (79 msg/s) | **12.61 s** |
| **Async** | **0.06 s** (1559 msg/s) | **0.22 s** (4639 msg/s) |

Read the story in the numbers:
- Serial grows in a straight line — 100 → 5 s, 1000 → 50 s. Dies under load.
- Parallel divides the work across 4 cores — about 4× faster than serial.
- Async barely moves from 100 → 1000, because all the I/O waits overlap.

**Why async beats parallel here:** a DB write is I/O-bound (you're *waiting*,
not computing). Async overlaps all the waiting on one thread. Parallel only runs
4 at a time. If the per-message work were CPU-heavy (e.g. image processing),
parallel would win, because async can't use more than one core. Say exactly this
to your sir — it shows you understand *why*, not just *which is fastest*.

---

## How to run — every single click

You need **two terminals** open in this folder for each test:
one for the server (subscriber), one for the gun (publisher).

### Step 0 — install the Python libraries (once)

```bash
pip install -r requirements.txt
```

### Step 1 — start an MQTT broker on `localhost:1883`

Easiest (Docker, no config needed — supports MQTT 5 shared subscriptions):

```bash
docker run --name mqtt -p 1883:1883 hivemq/hivemq-ce
```

Leave that running. (If you already have your HiveMQ container up on 1883, skip this.)

<details>
<summary>Alternative: Mosquitto instead of HiveMQ</summary>

Create `mosquitto.conf`:

```
listener 1883
allow_anonymous true
```

Run it:

```bash
docker run --name mqtt -p 1883:1883 -v "$PWD/mosquitto.conf:/mosquitto/config/mosquitto.conf" eclipse-mosquitto
```
</details>

---

### TEST A — SERIAL

**Terminal 1 (server):**
```bash
python subscriber_serial.py
```
Wait until it prints `ready ... Waiting for messages...`

**Terminal 2 (gun):**
```bash
python publisher.py 100
```

Look at **Terminal 1**. It prints the SERIAL RESULT (~5 s). Press `Ctrl+C` to stop it.

---

### TEST B — PARALLEL

**Terminal 1 (server):**
```bash
python subscriber_parallel.py
```
Wait for `ready ... Waiting for messages...` (4 workers connect).

**Terminal 2 (gun):**
```bash
python publisher.py 100
```

Look at **Terminal 1** → PARALLEL RESULT (~1.3 s). `Ctrl+C` to stop.

---

### TEST C — ASYNC

**Terminal 1 (server):**
```bash
python subscriber_async.py
```
Wait for `ready ... Waiting for messages...`

**Terminal 2 (gun):**
```bash
python publisher.py 100
```

Look at **Terminal 1** → ASYNC RESULT (~0.06 s). It exits on its own.

---

### The two tests your sir asked for

**1. Time test** — keep N at 100. Run all three (A, B, C) and compare the
"Total time" line. You'll see 5 s vs 1.3 s vs 0.06 s.

**2. High-load test** — change the number to 1000:

```bash
python publisher.py 1000
```

Run it against each server. Watch serial crawl (~50 s), parallel scale (~12 s),
and async stay almost flat (~0.2 s). That flat line under 10× the load is the
proof that async is the right pattern for the server.

> Run the **subscriber first**, then the publisher. The subscriber starts its
> clock on the first message it receives, so idle waiting time is never counted.

---

## TEST D — Thundering herd & server overload

This is a different question from A/B/C. Those asked "how fast does the server
*process* messages?" This asks "what happens when too many devices hit the
server *at the same instant*?" — the thundering herd.

**Honest note:** a real broker won't crash on a laptop. To make the overload
visible we run the broker with a low connection limit, and what you see is the
broker **refusing connections** — the real symptom of an overwhelmed server.
This part is easiest with **Mosquitto** (a one-line limit); HiveMQ doesn't cap
connections so simply.

### Step 1 — start a *limited* broker

```bash
docker run --name mqtt-limited -p 1883:1883 \
  -v "$PWD/mosquitto_limited.conf:/mosquitto/config/mosquitto.conf" eclipse-mosquitto
```

Windows native Mosquitto instead:
```powershell
& "C:\Program Files\mosquitto\mosquitto.exe" -c "$PWD\mosquitto_limited.conf" -v
```

`mosquitto_limited.conf` caps the broker at **50 connections** so the herd has
something to overflow.

### Step 2 — fire the STORM (no jitter)

```bash
python thundering_herd.py storm 500
```
500 devices all connect at the same instant. The broker can only hold 50, so it
**turns most of them away**.

### Step 3 — fire the SMART version (jitter + backoff)

```bash
python thundering_herd.py smart 500
```
Same 500 devices, but each waits a random delay and retries with exponential
backoff. The connections spread out, so **everyone eventually gets in**.

### Tested results (500 devices vs a 50-connection broker)

| | Connected | Refused | Connect attempts | Wall time |
|---|---|---|---|---|
| **storm** (herd) | 150 | **350** | 500 | ~6.5 s |
| **smart** (jitter+backoff) | **500** | **0** | 647 | ~13 s |

The takeaway to give your sir: jitter + backoff turned **350 refused devices
into 0**, at the cost of 147 extra retries and some extra time. You **trade a
little latency for not melting the server.** That trade is exactly why every
real IoT fleet uses jitter and backoff on reconnect.

> Switch the broker back to the normal (unlimited) config when you go back to
> tests A/B/C, or just stop `mqtt-limited` and start the normal `mqtt` container.



| Mode | The one line/idea that changes |
|---|---|
| Serial | `process_message(data)` runs **inline** → blocks until done |
| Parallel | topic = `$share/workers/Testdata` + `multiprocessing.Process` → broker round-robins across worker processes |
| Async | `asyncio.create_task(process_message(data))` → fire it and **don't await**; grab the next message immediately |
| Thundering herd | `storm`: all connect at `t=0` → `smart`: `random jitter` + `(2**attempt)` exponential backoff on retry |

Everything else — the broker, the topic name, the 50 ms work — is identical.

---

## Where each concept lives in a real system

```
ESP32 device      ->  serial is fine (sends only its own data)
MQTT Broker       ->  already concurrent internally (you CONFIGURE it, not code it)
Backend subscriber->  YOU choose here. Async wins for I/O work.
Scale past 1 core ->  multiple backend instances + $share/ shared subscriptions
```

Enterprise systems use **async backend instances** (asyncio / FastAPI), run
**several of them** behind **shared subscriptions** for horizontal scale, in
front of a **broker cluster** with a load balancer. Async is the core; clustering
and shared subscriptions scale it out.

---

## Troubleshooting

- **`ConnectionRefusedError`** → broker isn't running. Do Step 1.
- **Parallel never prints a result** → your broker doesn't support shared
  subscriptions. HiveMQ CE and Mosquitto 2.x both do; very old brokers don't.
- **Subscriber hangs after the result** (serial/parallel) → that's expected,
  it keeps listening. Press `Ctrl+C`.
- **Numbers differ from the table** → totally fine; they depend on your CPU and
  broker. The *ratios* (serial ≫ parallel ≫ async, and async staying flat under
  load) are what matter.
