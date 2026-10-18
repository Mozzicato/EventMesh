# EVENTMESH — MASTER BUILD ROADMAP

### Project vision

> **EventMesh is a production-grade job processing platform in Go that accepts work over HTTP, executes it reliably at scale, and provably survives chaos — with at-least-once delivery, idempotent consumers, and measured performance.**

The long-term experience is:

> **"Submit a job. Get an idempotency key back. It runs exactly-once-from-your-point-of-view, retries itself through failures, and lands in a dead-letter queue with full forensics if it can't. Then watch it all on a dashboard."**

The key architectural principle:

> **At-least-once delivery with idempotent consumers — because exactly-once is a lie at the platform level, and designing honestly around that lie is the entire point of the project.**

---

## 0. SCOPE DECISION — READ THIS FIRST

EventMesh is a **job/task queue platform** (think: a personal IronMQ / Cloud Tasks / Sidekiq-with-an-API), **not** a distributed message broker (Kafka/RabbitMQ) and **not** a workflow engine.

**Why this is deliberate:**
- A broker drags you into replication protocols, consumer groups, partition semantics — months of scope that dilutes your core stories: concurrency, idempotency, retries, backpressure.
- A single-node job platform lets you prove the *fundamentals* that generalize everywhere, with every claim load-tested.
- The honest limits (single node, at-least-once, in-order-per-queue at best) are interview gold: knowing what you did NOT build is senior signal.

**What we explicitly do NOT build (v1):** multi-node distribution, consumer groups, partition ordering guarantees, scheduled/delayed jobs beyond retry backoff (stretch), authentication beyond an API-key middleware (stretch).

---

## 1. THE EVENTMESH DEVELOPMENT MODEL

Same loop as Orphic and ForgeDB:

```text
Problem
   ↓
Smallest correct implementation
   ↓
Experiment
   ↓
Measure (load test, always)
   ↓
Test (incl. failure injection)
   ↓
Document (ADR / experiment log)
   ↓
Ship (tagged release)
   ↓
Next problem
```

Every phase has: **Objective → Deliverables → Experiments → Exit criteria.**

Four standing rules:
1. **Every number on the resume = a committed load-test artifact** in `benchmarks/results/`. No run, no claim.
2. **Every commit is real and dated.** The GitHub history is authenticity evidence.
3. **Every phase ends with a tagged release.**
4. **The failure-injection harness is a first-class citizen from Phase 4 onward** — it runs in CI, not just on your laptop.

---

## 2. PHASE 0 — THE FOUNDATION (Week 1)

### Objective
Engineering environment where every later experiment is reproducible.

### Deliverables
```text
eventmesh/
├── cmd/server/            # main
├── internal/
│   ├── api/               # HTTP handlers
│   ├── store/             # persistence layer (postgres first)
│   ├── queue/             # queue abstraction + implementations
│   ├── worker/            # worker pool
│   ├── retry/             # backoff logic
│   ├── idempotency/       # idempotency middleware/service
│   └── observability/     # logging, metrics
├── migrations/            # SQL migrations
├── tests/                 # integration tests
├── loadtests/             # k6 / vegeta scripts
├── chaos/                 # failure-injection harness
├── scripts/
├── docs/{decisions,experiments,research,architecture}/
├── .github/workflows/     # CI: vet, lint, tests -race, goleak, chaos suite
├── go.mod
├── README.md
└── .golangci.yml
```

Toolchain decisions (make them ADRs later): Go ≥ 1.22, stdlib `net/http` + Go 1.22 routing, `pgx/v5`, `go-redis/v9`, golangci-lint, `go.uber.org/goleak` in every test, test DB via **testcontainers-go** (real Postgres in CI, not mocks — mocks lie about transaction behavior).

### Research before/while doing it
- Go modules, project layout, `context` semantics
- `pgx` basics, database migrations (golang-migrate or goose)
- Docker + testcontainers
- Structured logging (`log/slog`)

### Exit criteria
Fresh clone → `docker compose up -d db && go test ./...` green in CI. Server boots, health endpoint responds, migrations run.

---

## 3. PHASE 1 — THE SPINE: API + Persistence + Job State Machine (Weeks 2–3)

### Objective
EventMesh accepts jobs over HTTP and tracks them through a strict lifecycle. Correctness first; no workers yet (jobs sit in `pending`).

### Deliverables
**Job lifecycle (design this on paper first — it is the project's constitution):**
```text
pending → claimed → running → succeeded
   ↓          ↓         ↓
   └──── retrying ─────┘ (backoff until attempts exhausted)
                            ↓
                         dead  (dead-letter queue)
```
- `POST /jobs` → `{id, idempotency_key, payload, queue, max_attempts}` → `201 {id, status}`
- `GET /jobs/{id}`, `GET /jobs?status=pending&limit=...`, `POST /jobs/{id}/cancel` (stretch)
- Postgres schema: `jobs` (id, idempotency_key **UNIQUE**, payload JSONB, status, attempts, max_attempts, run_at, created_at, updated_at, last_error), `job_events` (append-only audit: every state transition with timestamp + reason).
- **State transitions go through one function** that validates legality (`pending→running` legal; `succeeded→running` is a bug) and writes a `job_events` row in the same transaction. This transactional event log becomes your audit story and your debugging superpower.

### Experiments
- `pgx` batch insert: 1 vs 100 rows per round-trip.
- Transaction isolation: two concurrent `POST /jobs` with the same idempotency key — prove exactly one `201`, one `200`-with-existing (this belongs here, not Phase 3 — build it when the schema makes it trivial).

### Exit criteria
- API integration tests green against real Postgres.
- Illegal transitions rejected; every transition produces an event row; `updated_at` monotonic.

### Interview story
> "The job lifecycle is enforced by a single transition function inside the DB transaction, with an append-only event table — the state machine and the audit log can't diverge."

---

## 4. PHASE 2 — THE MUSCLE: Bounded Worker Pool (Weeks 4–5)

### Objective
Jobs actually execute. Bounded concurrency, clean shutdown.

### Deliverables
- **Bounded pool of N workers** (configurable): goroutines consuming from a `chan Job`. Learn precisely why the channel must be bounded — unbounded = unbounded memory = an outage shaped like a queue.
- Claim protocol (DB as the work source for now): workers run `UPDATE jobs SET status='running', ... WHERE id = (SELECT id FROM jobs WHERE status='pending' AND run_at <= now() ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING *`. **`FOR UPDATE SKIP LOCKED`** is the crown jewel here — learn it deeply, it is how real queues (Sidekiq Pro, pg-boss) do it.
- Execution: call the registered handler for the job's `queue` name; handlers are just `func(ctx context.Context, payload []byte) error`.
- **Graceful shutdown**: on SIGTERM — stop accepting new jobs, `context` with timeout cancels in-flight work, workers drain the channel, in-flight jobs return to `pending` (or `retrying`) before exit. Test by sending SIGTERM mid-batch and counting outcomes.
- `goleak` verifies no goroutine leaks in every test.

### Experiments
- Pool sizes 1/4/16/64 × sleep-10ms dummy jobs → throughput curve. Watch it flatten: find where DB claim contention, not handler time, becomes the ceiling. That inflection point is a lesson, measure it.
- Kill -TERM mid-run: verify every in-flight job is accounted for (succeeded, retried, or returned — none stuck in `running` forever).

### Exit criteria
- Throughput curve published; graceful-shutdown test green; goleak clean.

### Interview story
> "Workers claim with SELECT ... FOR UPDATE SKIP LOCKED so N workers never grab the same job; shutdown is a drain protocol where in-flight jobs are returned to the queue, verified by a test that SIGTERMs mid-batch."

---

## 5. PHASE 3 — RELIABILITY MACHINERY: Retries, Idempotency, Dead Letters (Weeks 6–8)

**This is the heart of EventMesh.** Everything before was scaffolding.

### Deliverables
**1. Retries with exponential backoff + jitter.**
- `attempts < max_attempts` on failure → status `retrying`, `run_at = now + backoff(attempts) + jitter`.
- Backoff: full jitter (`random(0, min(cap, base * 2^n))`) — read the AWS architecture blog on jitter; thundering-herd avoidance is the point.
- **Clock interface** (`now func() time.Time`) everywhere — this is what makes backoff testable without sleeping in tests.

**2. Idempotency keys (end-to-end).**
- Client sends `Idempotency-Key` header (or server mints one).
- `UNIQUE` constraint on `jobs.idempotency_key` → duplicate submit returns the original job with `200` (not `201`).
- **Idempotent execution semantics**: handlers declare whether they're safe to run twice; for `at-least-once` guarantees the platform may redeliver (worker crashed after claim, before ack — design for it explicitly: a job claimed but not acked within a lease timeout is reclaimed — a **visibility timeout**, like SQS).
- Test: fire 1,000 duplicate submissions concurrently → exactly one job created, all responses identical.

**3. Dead-letter queue.**
- Attempts exhausted → status `dead`, full error history preserved (from `job_events` + `last_error`).
- `POST /jobs/{id}/requeue` → back to `pending` with attempts reset (manual redrive, like SQS DLQ redrive).
- `GET /dlq` endpoint; per-queue dead counts.

### Experiments
- Backoff correctness table: attempt 1..6 → verify `run_at` deltas match the policy with the fake clock.
- Lease/visibility timeout: claim a job, never ack, watch it reclaimed after timeout; prove no double-*visible* execution.
- Jitter effectiveness: 1,000 failing jobs → histogram of retry times with and without jitter (show the herd).

### Exit criteria
- Retry matrix green; duplicate-submission test green at concurrency; DLQ redrive verified; every mechanism has a *deterministic* test via the clock interface.

### Interview story (the big one)
> "Exactly-once isn't available, so the platform is at-least-once: a claimed-but-unacked job is reclaimed after a visibility lease. Client-side idempotency keys dedupe submissions via a unique constraint under the database's concurrency control. Duplicate *execution* is the consumer's problem — so the platform makes it visible, bounded, and recoverable."

That paragraph is a distributed-systems education. Deliver it calmly and you've won the loop.

---

## 6. PHASE 4 — THE PROVING GROUND: Failure Injection (Weeks 9–10)

### Objective
A chaos harness that tries to kill EventMesh professionally, in CI, forever.

### Deliverables — `chaos/` harness, seeded & randomized
```text
Fault classes:
  F1 handler failures      — error, panic, timeout, ctx cancel
  F2 worker death          — crash mid-job (process exit), goroutine leak
  F3 database faults       — connection kill mid-claim, mid-ack (pg_terminate_backend),
                             Postgres restart mid-batch, disk-full simulation (stretch)
  F4 duplicate delivery    — same job delivered twice (lease expiry during long handler)
  F5 API abuse             — 10k duplicate submits, oversized payloads, malformed JSON
  F6 shutdown stress       — SIGTERM at random instants during load
```
- **Oracle model**: every chaos run maintains an expected-outcome model (jobs submitted, handlers that *must* have run ≥1 time, jobs that must end dead, jobs that must be recovered-not-lost). After the run, reconcile DB state against the oracle. **Zero lost jobs, zero permanently stuck jobs, zero silently swallowed errors.**
- Deterministic mode (seeded RNG) for CI + random mode for overnight runs.

### Experiments
- 100+ failure scenarios as a parameterized suite (this is your resume's "100+ failure scenarios" — now real, seeded, committed).
- Kill Postgres at 10 random instants during a 5k-job run → recovery behavior, reconnect behavior, job accounting.

### Exit criteria
Chaos suite green in CI on every PR. The oracle has caught at least one real bug (it will — celebrate it, document it in an experiment log).

---

## 7. PHASE 5 — THE FAST PATH: Redis-Backed Queue (Weeks 11–13)

### Objective
Move the hot queue from Postgres polls to Redis lists/streams — and *prove* when and why it's worth it.

### Deliverables
- Queue abstraction interface (`Enqueue`, `Dequeue`, `Ack`, `Nack`, `Requeue`) with two implementations: Postgres (existing) and Redis (lists + Lua scripts for atomic dequeue, or streams with consumer groups — pick one, write an ADR).
- Redis path: `LPUSH` enqueue, `BRPOPLPUSH` (or Lua equivalent) to a processing list for reliable dequeue, `LREM` ack, re-enqueue on Nack.
- Lease/visibility timeout in Redis: enqueued to processing list with timestamp; reaper moves expired items back.
- Router: queues configured per `queue` name → backend (some queues Postgres, some Redis).

### Experiments (the honest comparison)
- Postgres SKIP LOCKED vs Redis dequeue at 100/1k/5k jobs/sec → latency p50/p95, ops/sec, CPU. **Publish the table.** The result might surprise you (pgx is fast; network hops aren't free). Either way, you now have a *measured* answer to "why Redis?" — which is exactly the question a senior engineer would ask.
- Crash after `BRPOPLPUSH`, before ack → job recovered from processing list (durability of the fast path).

### Exit criteria
Redis backend passes the same integration suite + chaos oracle as Postgres. Comparison benchmark committed.

### Interview story
> "I abstracted the queue behind one interface and ran Postgres vs Redis head-to-head under load. Postgres SKIP LOCKED held up to X jobs/sec; Redis won above that but costs a network hop and a second durability story — here's the table."

---

## 8. PHASE 6 — PERFORMANCE ENGINEERING (Weeks 14–15)

### Objective
Publish the headline numbers — measured, reproducible, honest.

### Deliverables
- Load harness: `k6` (or `vegeta`) scenarios — submit burst, steady-state, mixed read/write. Seedable; committed scripts + results.
- Metrics first (needed to tune honestly): Prometheus `/metrics` — enqueue rate, dequeue rate, job latency histogram (enqueue→complete), attempt distribution, DLQ rate, pool saturation, queue depth. Grafana dashboard (docker-compose) for the demo GIF.
- Tuning experiments: pool size vs claim batch size (`LIMIT n` claims amortize round-trips), pgx pool config, handler concurrency per queue, Redis pipelining.

### Experiments
- Target matrix: **1,000+ jobs/sec sustained, ~10ms median, <50ms p95** (submit → completion for a trivial handler). If you fall short, tune and report the real numbers — the tuned story beats the fantasy.
- **Soak test**: 10,000+ jobs, 100 concurrent clients, 30+ minutes → goroutine count flat (goleak-style monitoring), memory flat, p99 stable, zero lost jobs (oracle).
- Latency breakdown: enqueue, queue wait, execute, ack — where do the 10ms live? (Often: DB round-trips. Show it.)

### Exit criteria
Benchmarks + soak results committed; resume numbers updated to reality; Grafana dashboard recorded as a GIF for the README.

---

## 9. PHASE 7 — OBSERVABILITY & OPERABILITY (Week 16)

### Objective
Make EventMesh debuggable at 3 a.m. — the difference between a toy and a platform.

### Deliverables
- Structured logs (`slog`) with job-id/attempt/trace correlation on every line.
- **Distributed tracing (stretch)**: OpenTelemetry spans across API → store → worker → handler.
- Operational endpoints: `/healthz`, `/readyz` (DB + Redis ping), `/metrics`.
- Runbook doc: "A queue is growing — what do you check?" (queue depth, pool saturation, DLQ rate, handler latency.) Writing this doc forces the operational mindset interviewers love.

### Exit criteria
One `docker compose up` gives you: server + Postgres + Redis + Prometheus + Grafana + a load-test script → full demo environment.

---

## 10. PHASE 8 — HARDENING (Week 17)

### Deliverables
- `go test -race` in CI on every package (concurrency correctness as a gate).
- Payload limits (413), request timeouts, graceful API degradation when DB is down (fail submit fast with `503`, don't hang).
- API key middleware (simple, single-key env var — scope discipline).
- Fuzzing (stretch): `go test -fuzz` on payload decoding.

### Exit criteria
Race-clean, timeout-clean, failure-behavior tests green.

---

## 11. PHASE 9 — PRODUCTIZATION & PORTFOLIO PACKAGE (Week 18)

### Deliverables
- README arc: **Problem → Architecture diagram → Job lifecycle diagram → Reliability model (the at-least-once honesty section) → Chaos demo GIF → Load-test tables → Observability screenshots → Limitations → Future work.** Limitations mandatory: single node, no ordering guarantee across workers, leases not fencing tokens (look up "fencing tokens" — mention them in Limitations and know what they mean; that's a senior wink).
- ADRs: why not a broker; why SKIP LOCKED; why full jitter; why visibility-timeout leases; why Redis lists vs streams; why testcontainers over mocks.
- **Blog post**: "Exactly-once is a lie: what building a job queue in Go taught me about delivery semantics." Your distribution asset.
- Demo: 3-minute recording — submit 10k jobs, kill a worker, watch retries + DLQ + Grafana, redrive, done.

---

## 12. RELEASE MILESTONES & STOPPING POINTS

| Release | Contains | Portfolio value |
|---|---|---|
| **EventMesh 0.1** | Phase 1: API + Postgres + state machine + event log | Solid backend project |
| **EventMesh 0.2** | + worker pool, SKIP LOCKED, graceful shutdown | Real concurrency story |
| **EventMesh 0.3** | + retries/jitter, idempotency keys, DLQ, leases | **Interview-grade. Core talking points live here.** |
| **EventMesh 0.4** | + chaos harness with oracle, in CI | "Wait, you built WHAT?" |
| **EventMesh 0.5** | + Redis fast path + measured comparison | Systems tradeoff literacy |
| **EventMesh 0.6** | + 1k jobs/sec benchmarks, soak test, Grafana | Defensible performance claims |
| **EventMesh 0.7** | + observability, hardening, polish | Exceptional, rare portfolio piece |

**Stopping-point principle:** recruiting season at 0.3? Apply — you own the retry/idempotency/DLQ conversation. At 0.4? You're carrying a chaos-testing story most senior engineers don't have.

**Interview-mapping cheat sheet (rehearse as you build):**
| Phase | Interview topic it arms you for |
|---|---|
| 1 | API design, state machines, transactional consistency |
| 2 | Goroutines, channels, bounded queues, context, graceful shutdown |
| 3 | **Delivery semantics, idempotency, backoff, leases — the distributed systems core** |
| 4 | Failure injection, oracles, testing philosophy |
| 5 | Caching/queue tradeoffs, measured architecture decisions |
| 6 | Load testing, latency analysis, p95/p99 literacy |
| 7 | Observability, operations mindset |
| 0–8 | "Tell me about a system you built" + "how do you test concurrent systems" — both now have real answers |

---

## 13. RESEARCH / EXPERIMENT / DECISION SYSTEMS

```text
docs/research/    at-least-once vs exactly-once; SKIP LOCKED semantics; backoff strategies;
                  visibility timeouts vs acknowledgements; Redis lists vs streams; worker pool patterns
docs/experiments/ numbered logs: Question, Hypothesis, Setup, Variables, Results, Decision
docs/decisions/   ADRs for every architecture-level choice
benchmarks/       every claim reproducible from a committed k6/vegeta script
chaos/results/    seeded oracle runs committed
```

---

## 14. READING LIST — PHASED, NOT ALL AT ONCE

- **Phase 0–1:** Go docs on `net/http` (1.22 routing), `pgx` getting-started, testcontainers-go docs.
- **Phase 2–3:** Go blog: "Share Memory By Communicating," "Pipelines and Cancellation," "Context"; AWS architecture blog "Exponential Backoff And Jitter"; SQS visibility-timeout docs; Sidekiq's "at-least-once" and reliability wiki pages; pg-boss / river queue READMEs (learn from prior art — and say you did).
- **Phase 4–6:** k6 docs; `go test -race` docs; Dave Cheney's "High Performance Go" workshop notes; USE method (Brendan Gregg) for the observability phase.
- **Phase 7–8:** OpenTelemetry Go getting-started (stretch); Prometheus histogram best practices.

Do **not** read Kafka's replication protocol or Raft yet. Temptation to overscope is the enemy.

---

## 15. YOUR FIRST CHECKPOINT

Not "build EventMesh." This:

```text
Go module + CI (vet, lint, tests, goleak)          (Phase 0)
      ↓
Design job lifecycle on paper — every legal transition (Phase 1 prep)
      ↓
POST /jobs → Postgres row → GET /jobs/{id}          (Phase 1)
      ↓
Illegal-transition rejection + job_events audit rows (Phase 1)
      ↓
Two concurrent duplicate submits → exactly one job  (Phase 1)
      ↓
Benchmark: 1 vs 100-row batch inserts               (Phase 1)
      ↓
TAG: v0.1.0 → stop, inspect, write the experiment log
```

**Time budget:** ~18 weeks at 10–15 hrs/week, fitting the Orphic (6w) → EventMesh (18w) → ForgeDB (28w) sequence.

The end goal is not "a background job runner."
It's:

> **"Ask me what happens when a worker dies mid-job, a client double-submits, Postgres restarts at the worst moment, and ten thousand jobs are in flight — I have a seeded test suite that answers all four, and an oracle that would catch me if I were wrong."**

That's EventMesh.
