# Redpanda Cross-Region Demo

Three-region, multi-cloud Redpanda deployment demonstrating cross-region replication, protocol bridging, data residency enforcement, HA, and DR.

---

## Architecture

```
eu-west-1 (AWS, primary)            eu-central-1 (AWS, DR)        europe-west4 (GCP, shadow)
┌──────────────────────────┐        ┌──────────────────────────┐  ┌──────────────────────────┐
│  Mosquitto MQTT broker   │        │  Redpanda Connect        │  │                          │
│      │                   │        │  (Kafka → AMQP bridge)   │  │                          │
│  Redpanda Connect        │        │      │                   │  │                          │
│  (MQTT → Kafka bridge)   │        │  RabbitMQ                │  │                          │
│      │                   │─ShadowLink──▶│                   │  │                          │
│  Redpanda [3 nodes]      │        │  Redpanda [3 nodes]      │  │  Redpanda [3 nodes]      │
│  retail-orders           │─ShadowLink────────────────────────────▶(shadow topics,          │
│  iot-events              │        │  (shadow, read-only)     │  │   read-only)             │
│                          │        │  Python AMQP consumer    │  │                          │
│  Python Kafka consumer   │        │  Console · Grafana       │  │  Console · Grafana       │
│  Console · Grafana       │        └──────────────────────────┘  └──────────────────────────┘
└──────────────────────────┘

ShadowLink filter policy (identical on both destination clusters):
  regional-* prefix → EXCLUDED (stays in eu-west-1 only)
  everything else   → INCLUDED (replicates to both DR clusters)
```

**AWS clusters**: `rp-demo-eu-west-1` (primary, EKS m7gd.large Graviton3), `rp-demo-eu-central-1` (DR, EKS)  
**GCP cluster**: `rp-demo-europe-west4` (shadow, GKE n2-standard-4, PD SSD)  
**Redpanda Operator**: v26.1.3

---

## Before You Start

```bash
./demo.sh preflight
```

Checks all pods, ShadowLink state, topics, and prints Console + Grafana URLs. Fix anything red before proceeding.

**Open in browser:**
- Console eu-west-1 (`:8080`)
- Console eu-central-1 (`:8080`)
- Console europe-west4 (`:8080`)
- Grafana eu-west-1 — Redpanda Overview dashboard

---

## Demo Segments

### Segment 0 — Context (5 min)

- This demo runs three clusters across two clouds: AWS EKS (eu-west-1, eu-central-1) and GCP GKE (europe-west4)
- Identical Operator version, identical CR YAML structure, identical ShadowLink filter on all three
- Also runs on Azure AKS, on-prem Kubernetes, AliCloud ACK — same manifests
- Notable GA features since v23: built-in Schema Registry, Tiered Storage, Operator-driven scale-down

---

### Segment 1 — Architecture Overview (5–7 min)

Slide only. Cover:
- Current estate being replaced and target state
- Key requirements: zero message loss on failover, low ops overhead, incremental migration path, no cloud lock-in

---

### Segment 1b — Cloud Portability (3 min)

```bash
./demo.sh preflight
./demo.sh produce 20
./demo.sh consume-all
```

**What to show:**
- `preflight` shows all three contexts green — eu-west-1 (AWS), eu-central-1 (AWS), europe-west4 (GCP)
- `consume-all` shows the same messages arriving on all three clusters within 30s
- GCP cluster runs identical Operator/CR config — only difference is `storageClass: premium-rwo`
- ShadowLink filter YAML is byte-for-byte identical on both destination clusters
- Any cluster can be promoted to primary with a single `rpk shadow failover` command

---

### Segment 2 — Reference Customer (3–4 min)

Select a reference customer matching the prospect's profile: global enterprise, self-hosted, multi-region, legacy messaging migration, or regulated data residency.

---

### Segment 3 — Kubernetes-Native Operations & GitOps (8 min)

```bash
./demo.sh upgrade
```

- Triggers a rolling StatefulSet restart with a live background producer running
- Each broker cycles one at a time: Pending → Running → Ready
- Message counter shows messages produced during restart; lost = 0
- Point to Grafana: under-replicated partitions briefly non-zero per broker, returns to 0 as each rejoins

**What to show:**
- Full environment defined as YAML in Git — operator handles provisioning, upgrades, scaling
- Every infrastructure change is a pull request with a reviewer and audit trail
- No manual broker interaction required

---

### Segment 4 — Cross-Region Replication / ShadowLink (8 min)

```bash
./demo.sh produce 20
./demo.sh consume-all
./demo.sh status
```

**What to show:**
- Messages produced to eu-west-1 appear on eu-central-1 (AWS) and europe-west4 (GCP) within 30s
- Consumer group offsets replicate cross-region — failover resumes from exact last offset, no replay
- Schema registry sync: consumer contracts stay intact across a regional failure

```bash
kubectl --context rp-demo-eu-central-1 -n redpanda get shadowlink eu-west-1-shadow -o yaml | grep -A5 schemaRegistry
```

- Built-in SR is Confluent SR API-compatible — existing tooling works without changes

---

### Segment 5 — Protocol Bridging (12 min)

```bash
# Terminal 1
./demo.sh amqp-consume

# Terminal 2
./demo.sh mqtt-publish 10
```

**What to show:**
- MQTT events → Redpanda Connect → `iot-events` topic → ShadowLink → eu-central-1 → Connect → RabbitMQ AMQP
- RabbitMQ is an AMQP endpoint illustrating the pattern — Redpanda Connect handles protocol translation at the edge; broker is pure Kafka API
- Connect has source connectors for legacy messaging systems — existing producers keep publishing; migration is topic-by-topic

```bash
./demo.sh amqp-status
```

---

### Segment 6 — Policy-Based Routing & Data Residency (6 min)

```bash
./demo.sh routing
```

**What to show:**
- `global-alerts` → replicated to eu-central-1 (matches include-all rule)
- `regional-eu-west-1-ops` → LOCAL ONLY (matched `regional-` exclude filter)
- Filter policy is two lines of YAML committed to Git — every change is a PR with reviewer and audit trail
- Enforcement happens at broker layer before data leaves the network — not at application layer
- Console → Audit Log: every ShadowLink filter change, ACL update, topic creation is logged

---

### Segment 7 — High Availability & Multi-Tenancy (5 min)

```bash
./demo.sh chaos
```

- Deletes redpanda-0; broker self-heals via Raft leader election
- RTO printed at end — typically 20–30s, no manual intervention

```bash
./demo.sh quotas
```

- Creates `demo-tenant-a` user with ACL scoped to `tenant-a-events` only
- Sets producer quota (5 MB/s) and consumer quota (10 MB/s) per client
- One team's traffic spike cannot impact other tenants on the shared cluster

---

### Segment 8 — Disaster Recovery (10 min)

```bash
./demo.sh failover
```

**What to show:**
- Walk through `./demo.sh restore` *before* triggering so the recovery path is visible
- Failover converts shadow topics to writable — point to Grafana (throughput shifts from eu-west-1 to eu-central-1)
- Console: `retail-orders` on eu-central-1 transitions from read-only shadow to writable

**Tiered Storage (retention objection):**
- Tiered Storage offloads 80–95% of data to S3/GCS/AliOSS
- Consumers read historical data transparently — no difference from hot data
- No disk-bound retention limit

---

### Segment 9 — Forward Platform Story (5 min)

- Tiered Storage is queryable directly from Databricks/Snowflake via Iceberg — no pulling data through the broker
- Immutable append-only log satisfies AI Act inference logging requirements architecturally
- Same cluster/data/guarantees serve operational event flows and AI agent infrastructure

---

## All Commands

| Command | Segment | Description |
|---|---|---|
| `preflight` | Before | Health check — all three clusters, ShadowLink, URLs |
| `upgrade` | 3 | Rolling cluster restart with live producer — zero message loss |
| `produce [n]` | 1b/4 | Produce n order events to eu-west-1 (default: 10) |
| `consume-source` | 4 | Consume `retail-orders` from eu-west-1 |
| `consume-shadow` | 4 | Consume `retail-orders` from eu-central-1 |
| `consume-gcp` | 1b | Consume `retail-orders` from europe-west4 (GCP) |
| `consume-both` | 4 | eu-west-1 + eu-central-1 side by side |
| `consume-all` | 1b/4 | All three clusters side by side |
| `status` | 4 | ShadowLink status, topic offsets, consumer group sync (all three) |
| `mqtt-publish [n]` | 5 | Publish n MQTT events via Mosquitto (default: 10) |
| `mqtt-consume [source\|shadow]` | 5 | Consume `iot-events` as a Kafka client |
| `mqtt-status` | 5 | MQTT bridge pipeline status |
| `amqp-consume` | 5 | AMQP 0.9.1 consumer — end of MQTT→Kafka→AMQP chain |
| `amqp-status` | 5 | RabbitMQ + AMQP bridge pipeline status |
| `routing` | 6 | Policy-based topic routing: global vs regional-scoped topics |
| `chaos` | 7 | Kill broker-0, measure RTO |
| `quotas` | 7 | Multi-tenancy: ACLs, per-client quotas, blast radius |
| `failover` | 8 | Promote eu-central-1 to primary (**irreversible**) |
| `restore` | 8 | Scale eu-west-1 back up, recreate ShadowLink |
| `python-consume [source\|shadow]` | — | Python confluent-kafka consumer (retail-orders) |
| `full-demo` | — | Fully scripted end-to-end demo |

---

## Recovery Playbook

| Symptom | Fix |
|---|---|
| ShadowLink not active | `kubectl --context rp-demo-eu-central-1 -n redpanda describe shadowlink eu-west-1-shadow` — check Events |
| AMQP consumer not receiving | `kubectl --context rp-demo-eu-central-1 -n redpanda rollout restart deployment/connect-amqp-bridge` |
| MQTT bridge not consuming | `kubectl --context rp-demo-eu-west-1 -n redpanda rollout restart deployment/connect-mqtt-bridge` — wait 30s for client ID re-registration |
| Shadow topics missing on eu-central-1 | Wait 30s (ShadowLink sync interval). Check `./demo.sh status` |
| `chaos` — cluster doesn't recover | `kubectl --context rp-demo-eu-west-1 -n redpanda describe pod redpanda-0` — look for PVC or node issues |
| `upgrade` — rollout doesn't cycle pods | `kubectl --context rp-demo-eu-west-1 -n redpanda rollout status statefulset/redpanda` to check progress |
| `quotas` — rpk quotas command fails | Syntax varies by version. Show ACLs and explain quota model verbally |
| Demo needs reset after failover | `./demo.sh restore` — safe to run multiple times |

---

## Setup

```bash
cd demo/
./setup.sh
```

Provisions two EKS clusters and one GKE cluster, installs cert-manager + LVM CSI (EKS only) + kube-prometheus-stack, deploys Redpanda Operator, 3-node clusters, ShadowLink on both DR clusters, MQTT bridge, AMQP bridge, and creates topics. ~30–40 minutes.

---

## File Layout

```
demo/
├── README.md
├── setup.sh                         # Full environment provisioning
├── demo.sh                          # Interactive demo script
├── eks/
│   ├── cluster-eu-west-1.yaml
│   └── cluster-eu-central-1.yaml
├── clusters/
│   ├── region-a/                    # eu-west-1 (AWS EKS) manifests
│   │   ├── redpanda.yaml            # 3-node cluster, TLS, external LB
│   │   ├── console.yaml
│   │   ├── python-consumer.yaml
│   │   └── mqtt-bridge/
│   │       ├── mosquitto.yaml
│   │       └── connect.yaml         # MQTT → iot-events
│   ├── region-b/                    # eu-central-1 (AWS EKS) manifests
│   │   ├── redpanda.yaml            # 3-node cluster, TLS
│   │   ├── console.yaml
│   │   ├── shadowlink.yaml          # regional- excluded, * included
│   │   └── amqp-bridge/
│   │       ├── rabbitmq.yaml
│   │       ├── connect.yaml         # iot-events → AMQP
│   │       └── amqp-consumer.yaml
│   └── region-c/                    # europe-west4 (GCP GKE) manifests
│       ├── redpanda.yaml            # 3-node cluster, premium-rwo storage
│       ├── console.yaml
│       └── shadowlink.yaml          # regional- excluded, * included (identical filter to region-b)
└── consumers/
    └── python/
        ├── retail_consumer.py
        ├── amqp_consumer.py
        └── requirements.txt
```

---

## Key Config Notes

**ShadowLink filter policy** (`clusters/region-b/shadowlink.yaml`):
- `regional-` prefix → excluded (stays in origin region only)
- All other topics → included (replicates to eu-central-1)

**TLS**: cert-manager provisions TLS on all listeners. ShadowLink uses the source cluster's external CA cert, copied during setup to the `eu-west-1-ca-cert` Secret in eu-central-1.

**Failover is irreversible**: `rpk shadow failover` converts shadow topics to regular writable topics. Use `./demo.sh restore` to reset.

**MQTT client IDs**: After pod restart, a second `rollout restart` may be needed if the old pod's session is fighting the new pod's connection (Mosquitto "session taken over" loop).
