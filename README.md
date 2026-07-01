# MQTT on the SERVER side — Serial vs Parallel vs Async

![MQTT Architecture Conceptual Illustration](C:/Users/Tharindu/.gemini/antigravity-ide/brain/9faf27f0-5358-48f3-b54c-2701eb9dabbc/mqtt_architecture_1782931487750.png)

A hands-on demo that proves **where** the concurrency problem in an IoT system
actually lives, and **how** three different server designs cope with a flood of
messages. Built to be run live and explained to your sir.

---

## The one idea this demo proves

A **publisher (an ESP32 device)** only ever sends *its own* reading. One
connection, one tiny stream. Serial is completely fine for a device — it never
has a load problem.

The load problem lives on the **server**: the subscriber that **hundreds or
thousands of devices hit at the same instant**. So the question
"serial vs parallel vs async" is really a question about **how the SERVER
processes incoming messages** — not about how a device sends them.

> `publisher.py` in this project is **not** the architecture. It is a
> **load-testing tool** (a "gun") that pretends to be N devices so we can flood
> the server and measure how it copes.

Every subscriber simulates the **same** work per message: a **50 ms "DB write"**.
The ONLY thing that changes between the three servers is **how each message is
dispatched to that work**. That single difference is the whole lesson.

---

## Files

| File | Role | What it shows |
|---|---|---|
| `publisher.py` | The load generator (the "gun") | Fires N messages at the broker at once |
| `subscriber_serial.py` | Server — **SERIAL** | One message at a time — the bottleneck |
| `subscriber_parallel.py` | Server — **PARALLEL** | One subscriber + a pool of 4 worker processes |
| `subscriber_async.py` | Server — **ASYNC** | One thread, all messages overlap — the fix for I/O work |
| `run_all.py` | **Runs all three at once** | Start serial + parallel + async together and compare |
| `thundering_herd.py` | Reconnect **storm** + mitigation | Devices reconnecting all at once → jitter + backoff fixes it |
| `mosquitto.conf` | Normal broker config | `listener 1883`, `allow_anonymous true` |
| `mosquitto_limited.conf` | Broker with a low limit | `max_connections 50` — makes overload visible |
| `requirements.txt` | Python deps | `paho-mqtt`, `aiomqtt` |

Each subscriber writes its own log file (`subscriber_serial.log`,
`subscriber_parallel.log`, `subscriber_async.log`) **and** prints to the console.

---

## Tested results (real runs on this machine)

| Mode | 100 messages | Speed-up vs serial |
|---|---|---|
| **Serial** | **~5.1 s** (20 msg/s) | baseline |
| **Parallel** (4-worker pool) | **~1.3 s** (79 msg/s) | ~4× faster |
| **Async** | **~0.1 s** (600–1600 msg/s) | ~30× faster |

Under 10× the load (1000 messages) serial grows to ~50 s, parallel to ~12 s,
and async barely moves (~0.2 s). That flat async line under load is the proof.

**Read the story in the numbers:**
- **Serial** grows in a straight line — 100 → 5 s, 1000 → 50 s. It dies under load.
- **Parallel** splits the work across 4 CPU cores — about 4× faster than serial.
- **Async** overlaps all the I/O waiting on a single thread — stays almost flat.

**Why async beats parallel here:** a DB write is **I/O-bound** — you are
*waiting*, not computing. Async overlaps all that waiting on one thread. Parallel
only runs 4 at a time and pays process overhead. **If the per-message work were
CPU-heavy** (image processing, ML inference), **parallel would win**, because
async cannot use more than one core. Say exactly this to your sir — it shows you
understand *why*, not just *which is fastest*.

---

## How each server is built (the key difference)

| Mode | The one idea that changes |
|---|---|
| **Serial** | `process_message(data)` runs **inline** in `on_message` → blocks until the 50 ms work is done, so message #100 waits behind 1–99. |
| **Parallel** | One subscriber drops each message on a **`multiprocessing.Queue`**; a **pool of 4 worker processes** pulls from it and does the blocking work on 4 cores at once. |
| **Async** | `asyncio.create_task(process_message(data))` → fire the work and **don't await** it; grab the next message immediately, so all the waits overlap. |

Everything else — the broker, the topic (`Testdata`), the 50 ms work — is identical.

### A note on the parallel design (important)

An earlier version used an MQTT **shared subscription** (`$share/group/topic`),
which asks the *broker* to round-robin messages across many subscribers. That is
the correct pattern for scaling across **machines**, but it is **broker-dependent**
and was unreliable here:

- **Mosquitto** dropped ~97% of QoS-1 shared-subscription messages under load.
- **HiveMQ CE** delivered reliably but sent a whole burst to a **single**
  subscriber, giving no real parallelism.

So this demo scales across **cores on one machine** the robust way: **one
reliable subscriber + a local worker pool**. It gives a true ~4× speed-up on
**any** broker. `$share` is kept as a documented comment in the file as the
"scale across machines" pattern.

---

## Setup

### Step 0 — install the Python libraries (once)

```powershell
pip install -r requirements.txt
```

> **Use one Python.** Install and run with the **same** interpreter. If you use
> [uv](https://github.com/astral-sh/uv) or a venv, install there too
> (`uv pip install -r requirements.txt`). A `ModuleNotFoundError: aiomqtt`
> almost always means you installed with one Python and ran with another.

### Step 1 — start an MQTT broker on `localhost:1883`

**Mosquitto (recommended — small image, works for every test in this project):**

```powershell
docker run -d --name mqtt -p 1883:1883 -v "${PWD}/mosquitto.conf:/mosquitto/config/mosquitto.conf" eclipse-mosquitto
```

<details>
<summary>Alternative: HiveMQ CE (larger image, no config file needed)</summary>

```powershell
docker run -d --name mqtt -p 1883:1883 hivemq/hivemq-ce
```
</details>

Check it's running: `docker ps`  ·  Watch its logs: `docker logs -f mqtt`
Stop / restart later: `docker stop mqtt` / `docker start mqtt`

> **Why the `-v` needs `mosquitto.conf` to already exist:** Docker bind-mounts a
> file into the container. If the host file is missing, Docker Desktop creates an
> **empty directory** with that name and the container fails with
> *"not a directory"*. The `mosquitto.conf` file is included in this folder, so
> you're fine — just don't delete it.

---

## Running the demo

You always need a **broker running** (Step 1). Then pick a style.

### Style A — ALL THREE AT ONCE (easiest)

One command starts serial + parallel + async together, fires the publisher, and
shows all three results side by side:

```powershell
python run_all.py 100        # start all three AND auto-fire 100 messages
python run_all.py 1000       # high-load run
python run_all.py            # start all three, then fire the publisher yourself
```

`run_all.py` waits until every subscriber is connected **before** publishing (so
no messages are missed), streams the combined output live, and saves each
`.log` file. Press **Ctrl+C** to stop everything — its cleanup kills the worker
processes too.

### Style B — ONE BY ONE (two terminals)

Run the **subscriber first**, wait for `ready ... Waiting for messages...`, then
publish from a second terminal.

**Terminal 1 (server):**
```powershell
python subscriber_serial.py       # or subscriber_parallel.py / subscriber_async.py
```

**Terminal 2 (gun):**
```powershell
python publisher.py 100           # try 1000 for the high-load test
```

Watch Terminal 1 for the RESULT block. Serial and parallel keep listening (fire
more batches, or `Ctrl+C` to stop). Async prints its result and exits on its own.

> **Order matters.** Start the subscriber *before* the publisher. A subscriber
> only receives messages published **while it is connected**; if you publish
> first, those messages are gone and you'll see nothing. `run_all.py` handles
> this ordering for you.

---

## Logging

Every subscriber logs to the console **and** to its own file, overwritten fresh
on each start:

```
subscriber_serial.log
subscriber_parallel.log
subscriber_async.log
```

Each log records: the **connection / ready** event, the **publisher's per-message
detail** (`seq=N/total`) every 100th message, and the final **RESULT** block. To
log *every* message instead of every 100th, set `LOG_EVERY = 1` at the top of the
subscriber file. (At `publisher.py 10000` that means a 10,000-line log.)

---

## TEST D — Thundering herd & server overload

A **different** question from A/B/C. Those asked *"how fast does the server
process messages?"* This asks *"what happens when too many devices hit the server
at the same instant?"* — the **thundering herd**.

`thundering_herd.py` opens a **separate TCP connection per device** (unlike
`publisher.py`, which uses one). In a real herd, thousands of devices wake up
after an outage and reconnect in the same instant → CPU spike, connection queue
overflow, refused connections.

**Honest note:** a real broker won't crash on a laptop. To make the overload
*visible*, run the broker with a low connection limit and watch it **refuse
connections** — the real-world symptom of an overwhelmed server (MQTT's version
of HTTP 429). This test is easiest on **Mosquitto** (a one-line limit).

### Step 1 — start a *limited* broker

```powershell
docker rm -f mqtt
docker run -d --name mqtt-limited -p 1883:1883 -v "${PWD}/mosquitto_limited.conf:/mosquitto/config/mosquitto.conf" eclipse-mosquitto
```

`mosquitto_limited.conf` caps the broker at **50 connections** so the herd has
something to overflow.

### Step 2 — fire the STORM (no jitter)

```powershell
python thundering_herd.py storm 500
```

500 devices all connect at the same instant. The broker holds only 50, so it
**turns most of them away**.

### Step 3 — fire the SMART version (jitter + backoff)

```powershell
python thundering_herd.py smart 500
```

Same 500 devices, but each waits a random delay (**jitter**) and retries with
**exponential backoff** on failure. The connections spread out, so **everyone
eventually gets in**.

### Typical results (500 devices vs a 50-connection broker)

| | Connected | Refused | Connect attempts | Wall time |
|---|---|---|---|---|
| **storm** (herd) | ~150 | **~350** | 500 | ~6–7 s |
| **smart** (jitter+backoff) | **500** | **0** | ~650 | ~13 s |

The takeaway for your sir: jitter + backoff turned **~350 refused devices into 0**,
at the cost of some extra retries and time. You **trade a little latency for not
melting the server.** That trade is exactly why every real IoT fleet uses jitter
and backoff on reconnect.

> Switch the broker back to the normal config (Step 1 at the top) when you return
> to tests A/B/C — or just stop `mqtt-limited` and start the normal `mqtt` container.

---

## Where each concept lives in a real system

```
ESP32 device        ->  serial is fine (it only sends its own data)
MQTT Broker         ->  already concurrent internally (you CONFIGURE it, not code it)
Backend subscriber  ->  YOU choose here. Async wins for I/O-bound work.
Scale past 1 core   ->  a worker pool (this demo) OR multiple backend
                        instances behind $share shared subscriptions (many machines)
Reconnect storms    ->  jitter + exponential backoff on every device
```

Enterprise systems use **async backend instances** (asyncio / FastAPI), run
**several of them** for horizontal scale, in front of a **broker cluster** with a
load balancer. Async is the core; a worker pool uses all cores on one box, and
`$share` shared subscriptions spread load across many boxes.

---

## Troubleshooting

- **`ConnectionRefusedError`** → the broker isn't running. Do Step 1, and check
  `docker ps`. If Docker Desktop itself is stopped, start it first.
- **`ModuleNotFoundError: No module named 'aiomqtt'`** → you installed with one
  Python and ran with another. Install into the interpreter you run with (see Setup).
- **Async crashes on Windows with `NotImplementedError` (`add_reader`)** →
  Windows defaults to the Proactor event loop, which aiomqtt can't use.
  `subscriber_async.py` already forces the Selector loop
  (`WindowsSelectorEventLoopPolicy`), so just re-run it.
- **A subscriber keeps re-printing `ready ...` / reconnecting** → a **duplicate
  client** with the same ID is connected (usually a leftover run in another
  terminal), and the broker keeps kicking one off. The subscribers use unique,
  PID-suffixed client IDs to avoid this; if it still happens, close old runs.
  Nuke everything: `Stop-Process -Name python -Force`.
- **`docker: ... not a directory` when starting Mosquitto** → the
  `mosquitto.conf` host file was missing and Docker made an empty directory with
  that name. Delete that directory and restore the `mosquitto.conf` **file**.
- **The subscriber "hangs" after the result** (serial/parallel) → expected — it
  keeps listening for the next batch. Press `Ctrl+C`.
- **Numbers differ from the tables** → totally fine; they depend on your CPU and
  broker. The **ratios** (serial ≫ parallel ≫ async, and async staying flat under
  load) are what matter.
