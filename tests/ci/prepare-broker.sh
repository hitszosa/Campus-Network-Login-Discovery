#!/bin/sh
# Only for disposable Ubuntu CI runners: permit this job's Broker fixture directory.
set -eu
mkdir -p /tmp/cnld-mqtt-ci
for profile in /etc/apparmor.d/mosquitto /etc/apparmor.d/usr.sbin.mosquitto; do
  if [ ! -f "$profile" ]; then continue; fi
  sudo python3 - "$profile" <<'PY'
from pathlib import Path
import sys
profile = Path(sys.argv[1])
text = profile.read_text()
rule = '\n  # CNLD CI fixtures only\n  /tmp/cnld-mqtt-ci/ r,\n  /tmp/cnld-mqtt-ci/** rwk,\n'
if '# CNLD CI fixtures only' not in text:
    position = text.rfind('}')
    if position < 0:
        raise SystemExit('cannot locate AppArmor profile closing brace')
    profile.write_text(text[:position] + rule + text[position:])
PY
  sudo apparmor_parser -r "$profile"
done
