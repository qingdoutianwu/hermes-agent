"""Core entry: resolve/install startup policy BEFORE sealed scheduler imports.

Invocation: managed Python startup_entry.py <sealed-bootstrap.py> gateway run ...
The dependency-generation launcher remains unchanged.
"""
import runpy
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from gateway import startup_isolation


def main():
    if len(sys.argv) < 2:
        raise startup_isolation.StartupIsolationError("sealed scheduler bootstrap required")
    target = Path(sys.argv[1]).absolute()
    startup_isolation.resolve_config()
    # Denials are active as soon as requested, even while admission owners import.
    if startup_isolation.restricted():
        startup_isolation.write_receipt()  # effective=false until every fence registers
        startup_isolation.install()
        startup_isolation.write_receipt()
    sys.argv = [str(target), *sys.argv[2:]]
    runpy.run_path(str(target), run_name="__main__")


if __name__ == "__main__":
    main()
