#!/usr/bin/env python3
"""Run the adapter against three disposable loopback Kafka 4.4 KRaft brokers.

Usage: python3 scripts/kafka-failover.py PATH_TO_KAFKA_DISTRIBUTION
Run from the workspace with protoc on PATH. This script creates its own topic,
broker processes, temporary storage and ports.
"""
import base64
import contextlib
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import tempfile
import time
import uuid


def kafka_id():
    return base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("=")


def stop(child):
    if child.poll() is None:
        child.terminate()
        try:
            child.wait(timeout=10)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()


def drill(kafka, work, cleanup):
    hosts = []
    with contextlib.ExitStack() as sockets:
        for _ in range(6):
            sock = sockets.enter_context(socket.socket())
            sock.bind(("127.0.0.1", 0))
            hosts.append("127.0.0.1:" + str(sock.getsockname()[1]))
    brokers, controllers = hosts[:3], hosts[3:]
    seeds = ",".join(brokers)
    initial = ",".join(f"{i}@{host}:{kafka_id()}" for i, host in enumerate(controllers, 1))
    cluster = kafka_id()
    topic = "ledger-decisions-drill"
    env = dict(os.environ, KAFKA_HEAP_OPTS="-Xms128m -Xmx256m", LOG_DIR=str(work))
    nodes = {}
    for i, (broker, ctl) in enumerate(zip(brokers, controllers), 1):
        cfg = work / f"{i}.properties"
        cfg.write_text(f"""process.roles=broker,controller
node.id={i}
controller.quorum.bootstrap.servers={','.join(controllers)}
listeners=PLAINTEXT://{broker},CONTROLLER://{ctl}
advertised.listeners=PLAINTEXT://{broker}
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs={work}/data-{i}
auto.create.topics.enable=false
""")
        subprocess.run([kafka / "kafka-storage.sh", "format", "-c", cfg, "-t", cluster,
                        "--initial-controllers", initial], env=env, check=True, timeout=60)
        log = cleanup.enter_context((work / f"broker-{i}.log").open("w"))
        nodes[i] = subprocess.Popen([kafka / "kafka-server-start.sh", cfg], env=env,
                                    stdout=log, stderr=subprocess.STDOUT)
        cleanup.callback(stop, nodes[i])

    def wait_for(read, description, timeout=90):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            result = read()
            if result:
                return result
            time.sleep(.25)
        raise RuntimeError(f"timed out waiting for {description}")

    def started():
        if any(proc.poll() is not None for proc in nodes.values()):
            raise RuntimeError("broker exited during setup")
        return all("Kafka Server started" in (work / f"broker-{i}.log").read_text() for i in nodes)

    wait_for(started, "three Kafka brokers")
    topics = [kafka / "kafka-topics.sh", "--bootstrap-server", seeds]
    subprocess.run([*topics, "--create", "--topic", topic, "--partitions", "2",
                    "--replication-factor", "3", "--config", "min.insync.replicas=2"],
                   check=True, env=env, timeout=60)

    def leader(replicas):
        desc = subprocess.run([*topics, "--describe", "--topic", topic], check=True, env=env,
                              timeout=30, capture_output=True, text=True).stdout
        for row in desc.splitlines():
            fields = re.search(r"Partition:\s+1\s+Leader:\s+(\d+).*Isr:\s+([\d,]+)", row)
            if fields and len(fields[2].split(",")) == replicas:
                print(row.strip(), flush=True)
                return int(fields[1])
        return None

    current = wait_for(lambda: leader(3), "three in-sync replicas")
    test_env = dict(os.environ, LEDGER_TEST_KAFKA_TOPIC=topic, LEDGER_TEST_KAFKA_BROKERS=seeds,
                    LEDGER_TEST_FAILOVER_DIR=str(work))
    test = subprocess.Popen(["cargo", "test", "--locked", "-p", "ledger-export-adapters",
                             "--test", "kafka", "real_broker_acknowledges", "--",
                             "--ignored", "--nocapture"], env=test_env)
    cleanup.callback(stop, test)

    def acknowledged():
        if test.poll() is not None:
            raise RuntimeError(f"adapter test exited before acknowledging: {test.returncode}")
        return (work / "acknowledged").is_file()

    wait_for(acknowledged, "acknowledged decisions", timeout=240)
    # Abruptly stop only the partition leader spawned by this drill.
    nodes[current].kill()
    nodes[current].wait(timeout=10)
    new = wait_for(lambda: leader(2), "two in-sync replicas with a new leader")
    assert new != current
    (work / "leader-replaced").touch()
    if test.wait(timeout=90):
        raise RuntimeError("adapter delivery or verification failed")
    print(f"partition leader {current} -> {new}; verified two original records and a replay", flush=True)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="ledger-kafka-drill-") as temp, contextlib.ExitStack() as stack:
        drill(Path(sys.argv[1]).resolve() / "bin", Path(temp), stack)
