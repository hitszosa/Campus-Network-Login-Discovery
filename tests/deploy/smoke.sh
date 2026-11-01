#!/bin/sh
# Deployment smoke test for a disposable Linux CI runner with Docker/systemd.
set -eu
cd "$(dirname "$0")/../.."
mode=${1:?usage: smoke.sh docker|systemd}
case "$mode" in docker|systemd) ;; *) exit 2;; esac
work=$(mktemp -d /tmp/cnld-mqtt-ci/deploy-XXXXXX)
broker_pid=
http_pid=
container="cnld-smoke-$$"
cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    cat "$work"/*.log 2>/dev/null || true
    if [ "$mode" = systemd ]; then
      sudo journalctl -u cnld-smoke --no-pager -n 100 || true
    else
      docker logs "$container" || true
    fi
  fi
  if [ "$mode" = systemd ]; then
    sudo systemctl stop cnld-smoke 2>/dev/null || true
    sudo rm -f /etc/systemd/system/cnld-smoke.service /usr/local/bin/cnld-smoke-agent
    sudo rm -rf /etc/cnld-smoke
    sudo systemctl daemon-reload
  else
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi
  if [ -n "$http_pid" ]; then kill "$http_pid" 2>/dev/null || true; wait "$http_pid" 2>/dev/null || true; fi
  if [ -n "$broker_pid" ]; then kill "$broker_pid" 2>/dev/null || true; wait "$broker_pid" 2>/dev/null || true; fi
  rm -rf "$work"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
printf 'listener 28883 127.0.0.1\nallow_anonymous true\npersistence false\n' > "$work/broker.conf"
mosquitto -c "$work/broker.conf" > "$work/broker.log" 2>&1 &
broker_pid=$!
go build -o "$work/fake-campus" ./cmd/fake-campus
printf yes > "$work/mode"
"$work/fake-campus" -listen 127.0.0.1:28080 -mode-file "$work/mode" > "$work/http.log" 2>&1 &
http_pid=$!
# Wait for the exact expected body rather than a fixed startup sleep.
python3 - <<'PY'
import time, urllib.request
for attempt in range(100):
    try:
        if urllib.request.urlopen('http://127.0.0.1:28080/check', timeout=.2).read() == b'authenticated':
            break
    except OSError:
        pass
    time.sleep(.05)
else:
    raise SystemExit('HTTP fixture did not start')
PY
sed -e 's/:18080/:28080/' -e 's/:1883/:28883/' configs/lab.yaml > "$work/config.yaml"
chmod 644 "$work/config.yaml"
if [ "$mode" = docker ]; then
  docker run -d --name "$container" --network host \
    -v "$work/config.yaml:/config.yaml:ro" cnld:ci
  test "$(docker inspect -f '{{.Config.User}}' "$container")" = '65532:65532'
else
  go build -o "$work/cnld-agent" ./cmd/cnld-agent
  id cnld-smoke >/dev/null 2>&1 || sudo useradd --system --user-group --no-create-home cnld-smoke
  sudo install -m 755 "$work/cnld-agent" /usr/local/bin/cnld-smoke-agent
  sudo install -d -m 755 /etc/cnld-smoke
  sudo install -m 644 "$work/config.yaml" /etc/cnld-smoke/config.yaml
  sed -e 's@/usr/local/bin/cnld-agent@/usr/local/bin/cnld-smoke-agent@' \
    -e 's@/etc/cnld/@/etc/cnld-smoke/@g' -e 's/^User=cnld$/User=cnld-smoke/' \
    -e 's/^Group=cnld$/Group=cnld-smoke/' deploy/cnld-agent.service > "$work/service"
  sudo install -m 644 "$work/service" /etc/systemd/system/cnld-smoke.service
  sudo systemctl daemon-reload
  sudo systemctl start cnld-smoke
fi
mosquitto_sub -h 127.0.0.1 -p 28883 -q 1 -t cnld/nodes/lab-01/report -C 1 -W 15 > "$work/report.json"
python3 - "$work/report.json" <<'PY'
import json,sys
report=json.load(open(sys.argv[1]))
assert report['labels']['network.campus/authenticated']=='true', report
assert report['connection_id'] and report['sequence']>0, report
PY
if [ "$mode" = docker ]; then
  docker stop --time 10 "$container"
  test "$(docker inspect -f '{{.State.ExitCode}}' "$container")" = '0'
else
  sudo systemctl stop cnld-smoke
  test "$(sudo systemctl show cnld-smoke -p ExecMainStatus --value)" = '0'
fi
mosquitto_sub -h 127.0.0.1 -p 28883 -q 1 -t cnld/nodes/lab-01/availability -C 1 -W 5 > "$work/offline.json"
python3 - "$work/offline.json" <<'PY'
import json,sys
state=json.load(open(sys.argv[1]))
assert state['status']=='offline' and state['reason']=='graceful_shutdown',state
PY
printf '%s deployment smoke: PASS\n' "$mode"
