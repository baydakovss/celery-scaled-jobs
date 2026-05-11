# celery-scaled-jobs

Celery workers as ephemeral Kubernetes Jobs, scaled by KEDA.

## Context

We have a Django/FastAPI platform where web API pods accept requests from multiple clients and dispatch Celery tasks to different queues. Most queues handle short-lived work. But some queues carry heavy ML training jobs and others heavy tasks that run 20min-2h each.

These heavy tasks need real resources - 10-24 GB RAM. But they only actually run 2-3 hours per day total. The rest of the time the capacity sits idle.

With a classic Celery Deployment approach you have two bad options:

1. Keep workers running 24/7 with `requests=limits=16-24Gi`. Expensive. Nodes allocated all day for tasks that run a few hours.
   - Use Karpenter with `karpenter.sh/do-not-disrupt: "true"` on all heavy worker pods. No consolidation possible - Karpenter can't move or evict these pods, so you end up with fragmented underutilized nodes.
2. KEDA ScaledObject (scales Deployment replicas). On scale-down, Kubernetes kills pods in arbitrary order - it doesn't know which pod is idle and which is mid-task. Long-running tasks get terminated randomly.

Both are wasteful.

## Approach

Run heavy workers as KEDA ScaledJobs instead of a Deployment:

- No tasks in queue = no pods = no resources consumed
- Task arrives → KEDA spawns a Job → Karpenter provisions a node → worker runs → task completes → pod exits → node drains
- Resources allocated only for actual execution time
- `karpenter.sh/do-not-disrupt: "true"` only lives on pods that are actually doing work, not idle workers

The lifecycle:

```
task in Redis queue
  → KEDA detects (pollingInterval: 20s)
  → creates Job (1 task = 1 pod)
  → Karpenter provisions node if needed
  → worker starts, takes task
  → task runs 20min-2h
  → worker exits (--max-tasks-per-child=1, sidecar SIGTERM triggers warm shutdown)
  → pod completes
  → node becomes empty → Karpenter reclaims
```

Sidecar sends SIGTERM after a safety timeout. Sending a TERM signal to Celery workers for graceful termination overall works well - it is called "warm shutdown". Worker finishes current task, stops consuming, exits cleanly. No data loss.

## Why not Airflow

We have Airflow (Amazon MWAA). It's heavily used by data engineers for scheduled ETL/ELT pipelines — running tasks via MWAA itself, Glue/Spark, and EKS via KubernetesOperator. This is a different use case and even another teams - ML/AI/data developers and other services dispatching ML and heavy compute tasks from application code via Celery. The task definitions, retry logic, monitoring - all already built into the existing Celery infrastructure. Rewriting to another orchestrator means:

- new infrastructure to maintain
- new paradigm for ML/AI/data developers (they know Celery, they don't want to learn Airflow DAGs)
- splitting task routing between two systems

This is not a best practice. If you're starting from scratch, dedicated batch systems (Airflow / KubernetesExecutor) are probably better. But when you already have a mature Celery-based platform with dozens of task types and multiple teams - adapting the execution model is cheaper than migrating.

## Why now (and not two years ago)

I wanted to do this back in 2024 but hit a wall with Celery + broker reliability.

We were on AmazonMQ (RabbitMQ). AmazonMQ has a weekly maintenance restart. After each restart, workers would stop picking up tasks - connections recovered but consumers silently died. This is a known issue: https://github.com/celery/celery/discussions/7276#discussioncomment-10193856

Switching to Redis/ElastiCache as broker fixed the reconnection problem - Celery 5.4 finally made Redis broker reliable enough for production.

But there was still the prefetch problem. We wanted near "1 pod = 1 task" behavior:

- `prefetch-multiplier=1` + `acks_late=False` -> worker grabs TWO messages (one executing, one prefetched)
- `prefetch-multiplier=1` + `acks_late=True` -> worker grabs ONE message, but with Redis broker `acks_late` relies on visibility timeout (unlike RabbitMQ which requeues immediately on disconnect). If task runs longer than visibility timeout - Redis puts it back in queue and another worker picks it up, causing duplicate execution. You have to set visibility timeout higher than your longest task, which defeats the purpose for unpredictable workloads.

Neither was acceptable for our use case.

Celery 5.6 added `--disable-prefetch` flag. Combined with sidecar-driven shutdown, this gives near "1 pod = 1 task" semantics: worker takes one task, executes it, receives SIGTERM from sidecar, finishes cleanly, exits. No prefetch buffer, no ambiguity about how many tasks are in flight.

This is what made the ScaledJob pattern viable.

## Key flags

```
--concurrency=1
--max-tasks-per-child=1
--disable-prefetch
--without-gossip
```

1 pod = 1 task. No prefetching (avoids reserving a second task before shutdown).

## What's here

- `app.py` - minimal Celery app with a test task (sleeps N minutes, simulates heavy work)
- `Dockerfile` - lightweight image, python + celery[redis]
- `docker-compose.yaml` - local testing
- `celery-long-running-scaled-jobs.yaml` - k8s manifest: Redis + KEDA ScaledJob with sidecar shutdown

## Usage

Local (docker-compose for quick verification):
```bash
docker compose up --build
```

Kubernetes:
```bash
# Initial setup: install KEDA ScaledJob, deploy Redis:
kubectl apply -n <ns> -f celery-long-running-scaled-jobs.yaml

# Send 10 tasks with random duration. Each task triggers a new Job, up to maxReplicaCount (5) in parallel:
kubectl run -n test task-sender --rm -it --restart=Never --image=baydakovss/celery-scaled-jobs:0.0.1 -- python -c "
import random
from app import long_term_sleep_task
for i in range(1, 11):
    seconds = random.randint(30, 180)
    res = long_term_sleep_task.apply_async(kwargs={'minutes_to_sleep': seconds / 60}, queue='heavy_jobs')
    print(f'Task {i} sent: {res.id} ({seconds} sec)')
"
Task 1 sent: 9f89036f-b25d-4660-8b4b-159d422ed420
Task 2 sent: cdca5bba-1b57-4843-aa73-38d49669b5cd
...
Task 10 sent: 4ad43f86-17e8-407f-bd42-aaaf9b93fc37
```

Result - 10 tasks sent, KEDA spawned 5 Jobs in parallel, each completed independently:
```
$ kubectl get scaledjob -n test
NAME               MIN   MAX   READY   ACTIVE   PAUSED   TRIGGERS   AUTHENTICATIONS   AGE
celery-long-job          5     True    False    False    redis                        126m

$ kubectl get jobs -n test
NAME                     STATUS     COMPLETIONS   DURATION   AGE
celery-long-job-5wd26    Complete   1/1           2m8s       12h
celery-long-job-64nsf    Complete   1/1           3m9s       12h
celery-long-job-gsr4t    Complete   1/1           3m9s       12h
celery-long-job-j6z4x    Complete   1/1           4m10s      12h
celery-long-job-l8ngf    Complete   1/1           69s        12h
celery-long-job-qs5vz    Complete   1/1           3m9s       12h
celery-long-job-sghdd    Complete   1/1           4m10s      12h
celery-long-job-sglr2    Complete   1/1           2m8s       12h
celery-long-job-tvpx6    Complete   1/1           3m9s       12h
celery-long-job-x6fnk    Complete   1/1           3m9s       12h
```

Worker logs show warm shutdown in action - sidecar sends SIGTERM at counter 53, but worker finishes the full task (180/180) before exiting:
```
[15:51:28] Counter 53 of 180
worker: Warm shutdown (MainProcess)
[15:51:29] Counter 54 of 180
...
[15:53:35] Counter 180 of 180
Task app.long_term_sleep_task[...] succeeded in 180.04s
```

## Cost impact

Before: 20 workers with limits=10Gi running 24/7 on a shared Karpenter pool, hoping for free RAM on the node. Peak usage needs up to 24Gi per task but we set limit at 10Gi and rely on node memory headroom. If a task exceeds available node RAM - OOMKilled, task retries.

Total: 20 x 10Gi x 24h = 200 GB-hours/day allocated, actual useful work ~3 GB-hours.

After (partially implemented): pods run on a dedicated Karpenter pool, exist only during task execution. Requests equal limits (Guaranteed QoS), so Karpenter provisions a node type that exactly fits the workload. If tasks run 3 hours total per day - you pay for 3 hours of compute, not 24.

## Tradeoffs

- Cold start overhead: pod scheduling + image pull + worker boot = 10-30s per task
- Not suitable for short tasks
- Sidecar timeout must be tuned to max expected celery init time
- Tasks are at-least-once delivery. Duplicate execution is possible if pod crashes before ACK - tasks must be idempotent - the same requirements for usual celery deployment

## Status

This pattern is specifically for long-running, resource-heavy workloads. We still use regular Celery workers (Deployments) for short-lived tasks.

The solution is stable and actively being rolled out to production workloads.
