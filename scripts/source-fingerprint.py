"""Hash the locked build inputs, tests, fixtures, and qualification scripts."""
import hashlib
from pathlib import Path
import sys


def manifest(root):
    paths = [root / "Cargo.toml", root / "Cargo.lock"]
    for directory in ("crates", "scripts"):
        paths.extend(p for p in (root / directory).rglob("*")
                     if p.is_file() and "__pycache__" not in p.parts and not p.name.startswith("."))
    return "".join(f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.relative_to(root)}\n"
                   for p in sorted(paths)).encode()


if __name__ == "__main__":
    content = manifest(Path(sys.argv[1] if len(sys.argv) > 1 else "."))
    if len(sys.argv) > 2:
        Path(sys.argv[2]).write_bytes(content)
    print(hashlib.sha256(content).hexdigest())
