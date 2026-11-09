"""Compare prebuilt Rust test binaries on one machine, one load test at a time.

Pass the original and candidate release network test executables, followed by a
new evidence directory. Both binaries must include the same capacity harness
and the batch-diagnostics feature. Run with an otherwise idle machine.
"""
import argparse
import os
from pathlib import Path
import subprocess


def run(binary, name, test, **options):
    # Start each trial from defaults, even if the caller uses load overrides.
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("LEDGER_LOAD_", "LEDGER_QUAL_", "LEDGER_SNAPSHOT_", "LEDGER_JOURNAL_LOCK_TRACE"))}
    env.update((k, str(v)) for k, v in options.items())
    with (evidence / (name + ".log")).open("x") as out:
        print(f"Starting {name}", flush=True)
        subprocess.run([binary, test, "--exact", "--ignored", "--nocapture"],
                       stdout=out, stderr=subprocess.STDOUT, env=env, check=True, timeout=900)
    if "1 passed; 0 failed" not in (evidence / (name + ".log")).read_text():
        raise RuntimeError(f"{name} did not execute the requested test")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("before")
    parser.add_argument("after")
    parser.add_argument("output", type=Path)
    suites = parser.add_mutually_exclusive_group()
    suites.add_argument("--supplement", action="store_true",
                        help="run matched passive SDK and snapshot trials only")
    suites.add_argument("--protected-active", action="store_true",
                        help="repeat the three protected SDK pairs with the active observer")
    args = parser.parse_args()
    before, after, evidence = args.before, args.after, args.output
    evidence.mkdir(parents=True, exist_ok=True)
    sdk = "api::capacity::sdk_capacity_and_storage"
    mixed = "api::load::public_protected_load_suite"
    snapshot = "recovery::snapshots::snapshot_load::snapshot_catchup_under_payment_load"
    failures = []
    for trial in range(1, 4):
        builds = [("before", before), ("after", after)]
        if trial == 2:
            builds.reverse()
        for mode in (["protected"] if args.protected_active else ["protected", "learner"]):
            for label, binary in builds:
                run(binary, f"{label}-{mode}-{trial}", sdk, LEDGER_QUAL_MODE=mode,
                    LEDGER_QUAL_OBSERVER="passive" if args.supplement else "active")
        if args.protected_active:
            continue
        for label, binary in builds:
            if args.supplement:
                name = f"{label}-snapshot-{trial}"
                try:
                    run(binary, name, snapshot)
                except (subprocess.SubprocessError, RuntimeError) as error:
                    failures.append(name)
                    print(f"{name} failed: {error}", flush=True)
            else:
                run(binary, f"{label}-mixed-{trial}", mixed)
    if failures:
        raise SystemExit("Failed trials: " + ", ".join(failures))
    if args.supplement or args.protected_active:
        raise SystemExit(0)
    for mode in ["protected", "learner"]:
        run(after, "pressure-" + mode, sdk, LEDGER_QUAL_MODE=mode,
            LEDGER_QUAL_RATE=3000, LEDGER_QUAL_SECONDS=4)
        run(after, "growth-" + mode, sdk, LEDGER_QUAL_MODE=mode,
            LEDGER_QUAL_RATE=150, LEDGER_QUAL_SECONDS=180)
    run(after, "group-commit", "group_commit::protected_group_commit_comparison")
    run(after, "snapshot", snapshot)
