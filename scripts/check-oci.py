#!/usr/bin/env python3
"""Verify OCI metadata and the embedded Agent ELF architecture for both targets."""
import io
import json
import sys
import tarfile

MACHINES = {"amd64": 62, "arm64": 183}

with tarfile.open(sys.argv[1]) as archive:
    def blob(descriptor):
        algorithm, digest = descriptor["digest"].split(":", 1)
        return archive.extractfile(f"blobs/{algorithm}/{digest}").read()

    def platforms(document):
        found = set()
        for descriptor in document.get("manifests", []):
            found |= platforms(json.loads(blob(descriptor)))
        if "config" in document:
            config = json.loads(blob(document["config"]))
            architecture = config["architecture"]
            machine = None
            for layer in document["layers"]:
                with tarfile.open(fileobj=io.BytesIO(blob(layer))) as filesystem:
                    for member in filesystem.getmembers():
                        if member.name.lstrip("./") == "cnld-agent" and member.isfile():
                            header = filesystem.extractfile(member).read(20)
                            assert header[:4] == b"\x7fELF", "Agent is not ELF"
                            endian = "little" if header[5] == 1 else "big"
                            machine = int.from_bytes(header[18:20], endian)
            assert machine == MACHINES[architecture], (architecture, machine)
            found.add((config["os"], architecture))
        return found

    found = platforms(json.load(archive.extractfile("index.json")))
    assert {("linux", "amd64"), ("linux", "arm64")} <= found, found
    print("OCI platforms and Agent ELF:", sorted(found))
