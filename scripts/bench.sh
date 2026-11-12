#!/usr/bin/env bash
# Runs ONE load test from a clean state and stops the server afterwards.
#
#   scripts/bench.sh <label> [loadtest flags...]
#   e.g. scripts/bench.sh burst-1 -jobs 10000 -clients 100
#        scripts/bench.sh rate1000-1 -jobs 10000 -clients 100 -rate 1000
#
# Steps: empty the jobs table and EventMesh's Redis keys, start a fresh server
# in -bench mode, record machine conditions, run cmd/loadtest, stop the server.
# Never run two of these at once: they share the database and the CPU.
# Requires: docker compose up -d, and bin/server.exe + bin/loadtest.exe built:
#   go build -o bin/server.exe ./cmd/server && go build -o bin/loadtest.exe ./cmd/loadtest
set -euo pipefail
cd "$(dirname "$0")/.."

label="$1"; shift
workers="${WORKERS:-16}"
port="${PORT:-8080}"

docker compose exec -T postgres psql -U eventmesh -qc 'TRUNCATE jobs' >/dev/null
docker compose exec -T redis redis-cli --scan --pattern 'eventmesh:*' |
	xargs -r docker compose exec -T redis redis-cli DEL >/dev/null || true

./bin/server.exe -bench -workers "$workers" -addr ":$port" >"bin/server-$label.log" 2>&1 &
for _ in $(seq 1 50); do
	curl -sf "http://localhost:$port/healthz" >/dev/null && break
	sleep 0.2
done

free_mb=$(powershell -NoProfile -Command '[int]((Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory/1024)' | tr -d '\r')
cpu=$(powershell -NoProfile -Command '(Get-CimInstance Win32_Processor).Name' | tr -d '\r' | xargs)
notes="server_workers=$workers; pg_pool_max_conns=20; cpu=$cpu; logical_cpus=$(nproc); free_mem_mb_at_start=$free_mb; client+server+postgres+redis on one machine; postgres fsync on"

status=0
./bin/loadtest.exe -url "http://localhost:$port" -label "$label" -notes "$notes" "$@" || status=$?

taskkill //F //IM server.exe >/dev/null 2>&1 || true
exit $status
