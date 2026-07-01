"""
run_all.py  --  RUN ALL THREE SERVERS AT ONCE  (or keep using them one by one)
==============================================================================

This project can be used TWO ways -- both still work:

  (1) ONE BY ONE  (unchanged -- nothing here breaks that)
        Terminal 1:  python subscriber_serial.py     (or _parallel / _async)
        Terminal 2:  python publisher.py 100

  (2) ALL IN ONE  (this file)
        python run_all.py            # start serial + parallel + async together,
                                     # then fire the publisher yourself
        python run_all.py 100        # start all three AND auto-fire 100 messages
        python run_all.py 1000       # high-load run, all three at once

All three subscribers listen at the SAME time, so a SINGLE publisher batch
feeds every one of them. You see all three results side by side and can
compare SERIAL vs PARALLEL vs ASYNC on the exact same load.

Each subscriber still writes its OWN log file you can open afterwards:
        subscriber_serial.log
        subscriber_parallel.log
        subscriber_async.log

Press Ctrl+C to stop everything.
"""
import subprocess, sys, threading, time, os

PYTHON = sys.executable
HERE   = os.path.dirname(os.path.abspath(__file__))

# (tag, script, log file) for each server
SUBSCRIBERS = [
    ("SERIAL",   "subscriber_serial.py",   "subscriber_serial.log"),
    ("PARALLEL", "subscriber_parallel.py", "subscriber_parallel.log"),
    ("ASYNC",    "subscriber_async.py",    "subscriber_async.log"),
]

# children inherit an unbuffered env so their output streams live
CHILD_ENV = {**os.environ, "PYTHONUNBUFFERED": "1"}

ready_seen  = 0
result_seen = 0
state_lock  = threading.Lock()
ready_event  = threading.Event()
result_event = threading.Event()


def stream(proc):
    """Forward one child's output to our console and watch for milestones."""
    global ready_seen, result_seen
    for raw in proc.stdout:
        line = raw.rstrip("\n")
        print(line, flush=True)
        if "ready on" in line:
            with state_lock:
                ready_seen += 1
                if ready_seen >= len(SUBSCRIBERS):
                    ready_event.set()
        if "RESULT" in line:
            with state_lock:
                result_seen += 1
                if result_seen >= len(SUBSCRIBERS):
                    result_event.set()


def kill_tree(proc):
    """Stop a child and ALL its grandchildren (parallel spawns 4 workers)."""
    if proc.poll() is not None:
        return
    try:
        if os.name == "nt":
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            proc.terminate()
    except Exception:
        pass


def main():
    total = int(sys.argv[1]) if len(sys.argv) > 1 else None

    print("=" * 70)
    print(" RUN ALL : starting SERIAL + PARALLEL + ASYNC subscribers together")
    print("=" * 70)

    procs = []
    for tag, script, _ in SUBSCRIBERS:
        p = subprocess.Popen(
            [PYTHON, script],
            cwd=HERE, env=CHILD_ENV,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )
        procs.append(p)
        threading.Thread(target=stream, args=(p,), daemon=True).start()

    try:
        # wait until all three have connected (or give up after 15 s)
        if not ready_event.wait(timeout=15):
            print("\n[run_all] WARNING: not all subscribers reported 'ready'. "
                  "Is the broker running on localhost:1883?")

        if total is not None:
            time.sleep(1.0)  # small cushion so subscriptions are fully live
            print(f"\n[run_all] firing publisher.py {total} -> all three at once...\n")
            subprocess.run([PYTHON, "publisher.py", str(total)],
                           cwd=HERE, env=CHILD_ENV)

            # wait for all three results (serial is slowest: ~total * 50 ms)
            budget = max(20.0, total * 0.06 + 15.0)
            if result_event.wait(timeout=budget):
                print("\n[run_all] all three reported their results above.")
            else:
                print("\n[run_all] timed out waiting for all results "
                      "(serial under heavy load can be slow).")

            print("\n" + "-" * 70)
            print(" Logs saved (open these any time):")
            for _, _, logf in SUBSCRIBERS:
                print(f"   - {os.path.join(HERE, logf)}")
            print("-" * 70)
            print(" Subscribers are still listening. Fire more batches with")
            print(f"   python publisher.py {total}")
            print(" or press Ctrl+C here to stop everything.")
        else:
            print("\n[run_all] all three are listening. In another terminal run:")
            print("   python publisher.py 100")
            print(" Press Ctrl+C here to stop everything.")

        # stay alive until the user stops us (or every child has exited)
        while any(p.poll() is None for p in procs):
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\n[run_all] stopping all subscribers...")
    finally:
        for p in procs:
            kill_tree(p)
        print("[run_all] done. Log files are saved in this folder.")


if __name__ == "__main__":
    main()
