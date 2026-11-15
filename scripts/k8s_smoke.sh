#!/usr/bin/env bash
# Enqueue jobs through the API on a running cluster, force-delete a worker pod
# while they run, and require every job to succeed.
set -euo pipefail

JOBS="${JOBS:-200}"
NS="${NS:-taskq}"

kubectl -n "$NS" port-forward svc/api 8000:8000 >/dev/null 2>&1 &
PF=$!
trap 'kill $PF 2>/dev/null || true' EXIT
for _ in $(seq 1 30); do
  curl -fsS localhost:8000/healthz >/dev/null 2>&1 && break
  sleep 1
done

for i in $(seq 1 "$JOBS"); do
  curl -fsS -X POST localhost:8000/jobs -H 'content-type: application/json' \
    -d '{"task":"sleep","payload":{"seconds":0.5},"queue":"smoke"}' >/dev/null
done
echo "enqueued $JOBS jobs"

sleep 3
VICTIM=$(kubectl -n "$NS" get pods -l app=worker -o jsonpath='{.items[0].metadata.name}')
kubectl -n "$NS" delete pod "$VICTIM" --grace-period=0 --force
echo "force-deleted $VICTIM"

for _ in $(seq 1 120); do
  STATS=$(curl -fsS localhost:8000/stats)
  DONE=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('smoke',{}).get('succeeded',0))" "$STATS")
  DEAD=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('smoke',{}).get('dead',0))" "$STATS")
  echo "succeeded=$DONE dead=$DEAD"
  if [ "$DEAD" != "0" ]; then echo "FAIL: jobs dead-lettered"; exit 1; fi
  if [ "$DONE" = "$JOBS" ]; then echo "PASS: all $JOBS jobs succeeded"; exit 0; fi
  sleep 2
done
echo "FAIL: timed out"; exit 1
