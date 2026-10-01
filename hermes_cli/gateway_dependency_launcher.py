"""Recovery candidate: bind committed PM dependencies before scheduler imports.

This launcher selects no new generation, installs no packages and changes no
Gateway arguments. It must be separately deployed; creating it is not deployment.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys


def pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate key: " + key)
        result[key] = value
    return result


def resolve_binding(project: Path, platform_home: Path):
    project = project.resolve(strict=True)
    key = hashlib.sha256(str(project).encode()).hexdigest()[:16]
    install = platform_home.resolve(strict=True) / "installs" / key
    facts_path = install / "facts.json"
    facts_bytes = facts_path.read_bytes()
    facts = json.loads(facts_bytes, object_pairs_hook=pairs)
    record = facts["packages"]["venv"]
    generation = Path(record["environment"]).resolve(strict=True)
    if not generation.is_relative_to((install / "environments").resolve(strict=True)):
        raise ValueError("dependency generation outside managed install")
    config = {}
    for line in (generation / "pyvenv.cfg").read_text().splitlines():
        if "=" in line:
            k, v = line.split("=", 1); config[k.strip()] = v.strip()
    executable = (generation / "bin/python").absolute()
    physical = executable.resolve(strict=True)
    tools = (platform_home / "tools").resolve(strict=True)
    if not physical.is_relative_to(tools) or not os.access(executable, os.X_OK):
        raise ValueError("interpreter outside managed tools")
    version = config.get("version_info", config.get("version", "")).split(".")
    if len(version) < 2 or not all(x.isdigit() for x in version[:2]):
        raise ValueError("generation Python version unknown")
    site = generation / "lib" / ("python" + ".".join(version[:2])) / "site-packages"
    if not site.is_dir():
        raise ValueError("selected site-packages missing")
    lock = Path(record["resolved_lock"]).resolve(strict=True)
    if not lock.is_relative_to(generation.parent):
        raise ValueError("selected lock outside generation")
    return {"project": str(project), "generation": str(generation),
            "python": str(executable), "physical_python": str(physical), "site": str(site),
            "facts": str(facts_path), "facts_sha256": hashlib.sha256(facts_bytes).hexdigest(),
            "lock": str(lock), "lock_sha256": hashlib.sha256(lock.read_bytes()).hexdigest()}


def launch_command(binding, bootstrap, arguments, receipt):
    # Exact argv preserved; exec avoids an extra owner/signal forwarding layer.
    body = (
        "import sys,site,json,hashlib,os,runpy,time;"
        "b=json.loads(" + repr(json.dumps(binding, sort_keys=True)) + ");"
        "assert hashlib.sha256(open(b['facts'],'rb').read()).hexdigest()==b['facts_sha256'];"
        "assert str(__import__('pathlib').Path(sys.executable).resolve())==b['physical_python'];"
        "site.addsitedir(b['site']);sys.path.insert(0,b['project']);"
        "from ruamel.yaml import YAML;import ruamel.yaml;"
        "r={'pid':os.getpid(),'python':sys.executable,'sys_prefix':sys.prefix,'sys_path':sys.path,"
        "'dependency_generation':b['generation'],'facts_sha256':b['facts_sha256'],"
        "'ruamel_file':ruamel.yaml.__file__,'ruamel_import':'PASS',"
        "'captured_at_epoch':time.time()};"
        "p=" + repr(str(receipt)) + ";"
        "fd=os.open(p,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o444);"
        "os.write(fd,json.dumps(r,sort_keys=True).encode()+bytes([10]));os.fsync(fd);os.close(fd);"
        "sys.argv=" + repr([str(bootstrap), *arguments]) + ";"
        "runpy.run_path(" + repr(str(bootstrap)) + ",run_name='__main__')"
    )
    return [binding["python"], "-I", "-B", "-c", body]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--platform-home", type=Path, required=True)
    parser.add_argument("--bootstrap", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    arguments = args.arguments[1:] if args.arguments[:1] == ["--"] else args.arguments
    try:
        binding = resolve_binding(args.project_root, args.platform_home)
        command = launch_command(binding, args.bootstrap.absolute(), arguments, args.receipt.absolute())
        os.execv(command[0], command)
    except (OSError, ValueError, KeyError) as exc:
        print("GATEWAY_DEPENDENCY_BINDING_FAILED: " + str(exc), file=sys.stderr)
        return 78


if __name__ == "__main__":
    raise SystemExit(main())
