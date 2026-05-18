# Redpanda Cross-Region Demo
## Enterprise Event Messaging Platform — SE Demo Runsheet

**Format**: ~90 minutes, 9 segments  
**Audience**: Engineering leaders, platform architects, operations teams evaluating Redpanda as an enterprise event streaming platform

---

## Architecture

```
eu-west-1 (primary / "EU cluster")                eu-central-1 (DR / "shadow cluster")
┌──────────────────────────────────┐              ┌──────────────────────────────────┐
│  Mosquitto MQTT broker           │              │                                  │
│      │                           │              │  Redpanda Connect                │
│  Redpanda Connect                │              │  (Kafka → AMQP 0.9.1 bridge)     │
│  (MQTT → Kafka bridge)           │              │      │                           │
│      │                           │              │  RabbitMQ                        │
│  Redpanda 3-node cluster         │──ShadowLink─▶│  Redpanda 3-node cluster         │
│  retail-orders, iot-events       │  (regional-* │  (shadow topics, read-only)      │
│                                  │   excluded,  │      │                           │
│                                  │   * included)│  Python pika AMQP consumer       │
│  Python confluent-kafka consumer │              │  Redpanda Console                │
│  Redpanda Console                │              │  Prometheus + Grafana            │
│  Prometheus + Grafana            │              └──────────────────────────────────┘
└──────────────────────────────────┘

ShadowLink filter policy (clusters/region-b/shadowlink.yaml):
  regional-* prefix → EXCLUDED (stays in origin region only)
  everything else → INCLUDED (replicates to DR cluster)
```

**EKS clusters**: `rp-demo-eu-west-1` (primary), `rp-demo-eu-central-1` (DR)  
**Instance type**: m7gd.large (Graviton3, local NVMe via LVM CSI)  
**Redpanda Operator**: v26.1.3

---

## Before the Meeting (10–15 min)

```bash
./demo.sh preflight
```

Checks every pod, ShadowLink state, both topics, and prints Console + Grafana URLs. Fix anything red.

**Open these browser tabs before you start:**
- Console eu-west-1 (`:8080`) — topic list, message viewer, audit log
- Console eu-central-1 (`:8080`) — shadow topic confirmation
- Grafana eu-west-1 — Redpanda Overview dashboard (keep visible during chaos + DR)

**State cloud-agnosticism proactively in Segment 0:**
> *"Our demo runs on AWS EKS — the operator and broker are cloud-agnostic and run identically on GCP GKE, Azure AKS, on-prem Kubernetes, and AliCloud ACK."*

---

## Demo Flow — 90 Minutes

### Segment 0 — Context Setting (5 min)

Open with two goals:

**1. Acknowledge what has changed since any prior evaluation:**
> *"Here's what has changed."*  
Three things on one slide: Schema Registry built-in (GA 2023), Tiered Storage GA, Operator-driven scale-down automated.

**2. State cloud-agnosticism immediately:**
> *"Our demo runs on AWS EKS. The operator and broker are identical on GCP GKE, Azure AKS, on-prem Kubernetes, and AliCloud ACK — same YAML, same GitOps model, same day-2 behaviour."*

Transition with:
> *"Most vendors will show you a legacy messaging replacement today. We want to show you something different — the streaming backbone for your AI Agent gateways, your data mesh, your inference pipelines, and your compliance logging."*

---

### Segment 1 — Current → Future Architecture Recap (5–7 min)

Single slide showing what Redpanda understands about the current estate and target state.

**Cover explicitly:**
- What's being replaced: existing event mesh + protocol translation layer
- Target state: self-hosted K8s-native, multi-cloud, high-throughput, large consumer footprint
- Three things that matter most: zero message loss on failover for critical platforms, minimal ops overhead, migration path with no day-one refactoring

**Invite correction:** *"Before we open a terminal, does this match your topology? Correct us now."*

---

### Segment 2 — Reference Customer (3–4 min)

Large global enterprise, self-hosted, multi-region, migration from legacy messaging, or regulated data residency. Match the reference customer profile to the prospect's industry and use case.

---

### Segment 3 — Kubernetes-Native Platform & GitOps (8 min)

```bash
# Start a live producer in the background first, then trigger the rolling restart
./demo.sh upgrade
```

Narrate as it runs:
> *"Your platform team writes YAML. The operator provisions, upgrades, and scales the cluster. No manual broker operations. The full environment is reproducible from a Git repo — every change is a PR, every state is auditable."*

Watch the counter — messages produced during the rolling restart, messages lost = 0.

**Browser:** Point to Grafana — under-replicated partitions briefly non-zero per broker, returns to 0 as each one rejoins. No producer interruption.

**Key hits:**
- **Cost:** "This is how you reduce operational costs — the operator handles every upgrade, every patch, every scale event."
- **Governance:** "Every infrastructure change is a pull request — full audit trail for the entire development community."

**ADP connection:** *"This is also how AI agent infrastructure gets deployed and managed at scale — same operator model runs your streaming platform and your AI event infrastructure."*

---

### Segment 4 — Cross-Region Replication / ShadowLink (8 min)

```bash
./demo.sh produce 20       # produce to eu-west-1
./demo.sh consume-both     # watch both clusters receive messages
./demo.sh status           # show consumer group offsets synced cross-region
```

State explicitly:
> *"When the primary region fails, consumers switch to the DR cluster and resume from exactly the last message they processed. No replay. No data loss. No reconfiguration."*

**Explicitly call out schema registry sync:**
```bash
kubectl --context rp-demo-eu-central-1 -n redpanda get shadowlink eu-west-1-shadow -o yaml | grep -A5 schemaRegistry
```

**Browser:** Show Grafana — throughput dropping on primary, coming up on DR.

**Key hits:**
- "Consumer group offsets replicate cross-region — schema registry sync means your consumer contracts stay intact across a regional failure."
- "Redpanda's built-in SR is Confluent SR API-compatible — existing SR tooling works."

**ADP connection:** *"The same replication that protects your operational event flows also replicates agent context, inference logs, and model event streams across regions — without loss, without replay."*

---

### Segment 5 — Protocol Bridging + Migration Architecture (12 min)

```bash
# Terminal 1: watch the end of the chain
./demo.sh amqp-consume

# Terminal 2: publish MQTT events
./demo.sh mqtt-publish 10
```

**Own the RabbitMQ immediately:**
> *"We're using RabbitMQ here as an AMQP endpoint to illustrate the pattern — in your environment this is any AMQP consumer. Redpanda Connect handles protocol translation at the edge; the broker stays pure Kafka API throughout."*

**Walk the migration path explicitly:**
> *"Connect has source connectors for legacy messaging systems. Your existing producers keep publishing where they are. Events bridge into Redpanda. Consumers migrate at their own pace. Migration is topic-by-topic, team-by-team."*

**Tie to IoT reality:**
- Edge devices at distributed locations → MQTT → Redpanda Connect → `iot-events` topic
- This pattern directly mirrors how distributed IoT estates work today

```bash
./demo.sh amqp-status     # show pipeline health, consumer group lag
```

**ADP connection:** *"The same Connect pipeline that ingests MQTT telemetry from edge devices is also how sensor data feeds AI pipelines in real time — computer vision, inventory models, autonomous systems. You're not building two pipelines. You're building one."*

---

### Segment 6 — Policy-Based Routing & Data Residency (6 min)

```bash
./demo.sh routing
```

**Lead with the governance reframe:**
> *"Routing policy is code. It lives in your GitOps repository — every change is a pull request with a reviewer and a complete audit trail. That's a stronger governance model than a UI where a routing rule can be changed with a click and no record."*

Watch the countdown. After the result:
- `global-alerts` → replicated to DR cluster ✓
- `regional-eu-west-1-ops` → LOCAL ONLY ✓ (matched 'regional-' exclude rule)

**Name data residency compliance explicitly.** Regional data didn't leave its origin region. That's not a naming convention — it's enforced by the broker before any data leaves the network.

**Browser:** Show Console → Audit Log. Every ShadowLink filter change, every ACL update, every topic creation is logged. This is the governance trail.

**Key hits:**
- Policy-as-code aligns with modern API gateway governance — same model, consistent mental model across teams
- Data residency is enforced at the infrastructure layer, not the application layer — satisfies regulatory requirements automatically

**ADP connection:** *"Data residency enforcement at the broker layer is also how you satisfy AI Act and similar regulatory requirements that model training data and inference inputs are processed only in permitted jurisdictions."*

---

### Segment 7 — High Availability + Multi-Tenancy (5 min)

```bash
# Kill broker-0, read the RTO out loud
./demo.sh chaos
```

After the RTO prints, add immediately:
> *"The failure of this broker affected only the partitions it was leading — other tenants on the platform kept processing without interruption."*

Then pivot straight to multi-tenancy:
```bash
./demo.sh quotas
```

This shows namespaces, ACLs, per-client throughput quotas, and rate limiting. Walk through:
1. `demo-tenant-a` service account
2. ACL: can only access `tenant-a-events` — no access to other teams' topics
3. Throughput quota: 5 MB/s produce, 10 MB/s consume — one team's surge can't starve others

**Key hit:** Blast radius containment is a day-1 operational requirement for any large multi-team platform. This is not theoretical.

---

### Segment 8 — Disaster Recovery (10 min)

```bash
# Walk through restore flow BEFORE triggering — show the recovery path
./demo.sh failover
```

**Before triggering failover:**
> *"Let me show you what the restore path looks like before I pull the trigger — so you can see the full operational picture."*
Point to `./demo.sh restore` in the README.

**During failover:**
- **Browser (Grafana):** throughput dropping on eu-west-1, coming up on eu-central-1 — show the transition
- **Browser (Console):** point to `retail-orders` on eu-central-1 going from read-only shadow to writable

**Close the disk-bound / infinite retention objection:**
> *"Tiered Storage offloads 80–95% of data to object storage — S3, GCS, AliOSS. Consumers read historical data transparently — no difference between a message from yesterday and a message from six months ago."*

**Show 10MB payload support:**
```bash
./demo.sh produce-large
```
> *"Large payload support is handled natively — up to 10MB messages, no broker configuration changes."*

**Key hits:**
- Ops console view of the failover — show the operational experience
- Seamless historical read from tiered storage = no retention limits

---

### Segment 9 — Forward Platform Story & Close (5 min)

Close with the forward-looking platform investment story:

> *"You came here evaluating a messaging platform replacement. What you're actually selecting is the streaming backbone for the next five years of your data and AI architecture."*

Three points:
- **Iceberg-compatible tiered storage:** 80–95% offloaded to object storage is queryable directly from Databricks, Snowflake — no pulling through the broker.
- **Regulatory audit trail:** Redpanda's immutable append-only log is architecturally aligned with AI Act inference logging requirements.
- **Agentic Data Plane:** The event infrastructure that AI agents need — real-time context delivery, agent-to-agent communication, tool call event streams — is exactly what Redpanda was built for.

Close line: *"The legacy migration funds the AI infrastructure. Same cluster, same data, same guarantees."*

Follow up: *"Can we schedule an architecture session with your full platform team?"*

---

## Key Landmines to Pre-empt

| Landmine | Pre-emption |
|---|---|
| Demo runs on AWS, we're on GCP/Azure/AliCloud | State in Segment 0: operator is cloud-agnostic, identical on GCP GKE, Azure AKS, on-prem K8s, AliCloud ACK |
| Why is there a RabbitMQ? | Own it in Segment 5 before they ask — frame as edge translation pattern, not a dependency |
| Policy routing looks thin vs legacy mesh UI | GitOps reframe: policy is code, lives in Git, every change is a PR. Stronger governance than a UI |
| Migration path for existing endpoints | Connect source connectors = no day-one refactoring. Migration is incremental, topic-by-topic |
| Can you really run on AliCloud? | Immediate, confident: self-hosted on AliCloud ACK, same operator, same GitOps model |
| Demo shows 2 regions, we have more | ShadowLink scales to N clusters. Routing policy is additive YAML. Show the pattern — scale follows |
| Schema registry compatibility | Built-in SR GA since 2023. Full Confluent SR API compatibility. External SR also supported |

---

## Common Objections — Close Them Proactively

| Concern | Answer |
|---|---|
| Record deletion | Compacted topics + tombstone records. Key-based delete via null-value record. GDPR-compliant pattern. |
| Scale-down was painful | Operator: automated decommission with partition rebalance. Tiered storage reduces data volume moved during scale-down. |
| Needed external Confluent SR | Built-in Schema Registry, GA since 2023. Full Confluent SR API compatibility. |
| Disk-bound, no infinite retention | Tiered Storage to S3/GCS/AliOSS. Consumer reads historical data transparently — no difference from hot data. |

---

## All Commands

| Command | Segment | Description |
|---|---|---|
| `preflight` | Before | Health check — verify all pods, ShadowLink, URLs |
| `upgrade` | 3 | Rolling cluster upgrade with live producer — zero message loss |
| `produce [n]` | 4 | Produce n order events to eu-west-1 (default: 10) |
| `produce-large` | 8 | Produce a 10MB message — demonstrate large payload support |
| `consume-source` | 4 | Consume `retail-orders` from eu-west-1 |
| `consume-shadow` | 4 | Consume `retail-orders` from eu-central-1 |
| `consume-both` | 4 | Both clusters side by side |
| `status` | 4 | ShadowLink status, topic offsets, consumer group sync |
| `mqtt-publish [n]` | 5 | Publish n MQTT events via Mosquitto (default: 10) |
| `mqtt-consume [source\|shadow]` | 5 | Consume `iot-events` as a Kafka client |
| `mqtt-status` | 5 | MQTT bridge pipeline status |
| `amqp-consume` | 5 | AMQP 0.9.1 consumer — end of MQTT→Kafka→AMQP chain |
| `amqp-status` | 5 | RabbitMQ + AMQP bridge pipeline status |
| `routing` | 6 | Policy-based topic routing: global vs regional-scoped topics |
| `chaos` | 7 | Kill broker-0, read RTO out loud |
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
| Shadow topics missing on eu-central-1 | Wait 30s — ShadowLink interval is 30s. Check `./demo.sh status` |
| `chaos` — cluster doesn't recover | `kubectl --context rp-demo-eu-west-1 -n redpanda describe pod redpanda-0` — look for PVC or node issues |
| `upgrade` — annotation doesn't trigger restart | `kubectl --context rp-demo-eu-west-1 -n redpanda rollout restart statefulset/redpanda` as fallback |
| `quotas` — rpk quotas command fails | Quotas command syntax varies by version. Show ACLs and explain the quota model verbally |
| Demo needs reset after failover | `./demo.sh restore` — safe to run multiple times |

---

## Setup

```bash
cd demo/
./setup.sh
```

Provisions both EKS clusters, installs cert-manager + LVM CSI + kube-prometheus-stack, deploys Redpanda Operator, 3-node clusters, ShadowLink, MQTT bridge, AMQP bridge, and creates topics. Takes ~25–35 minutes.

---

## File Layout

```
demo/
├── README.md                        # This file (presenter runsheet)
├── setup.sh                         # Full environment provisioning (~30 min)
├── demo.sh                          # Interactive demo script
├── eks/
│   ├── cluster-eu-west-1.yaml       # EKS cluster spec (primary)
│   └── cluster-eu-central-1.yaml    # EKS cluster spec (DR)
├── clusters/
│   ├── region-a/                    # eu-west-1 manifests
│   │   ├── redpanda.yaml            # Redpanda cluster (3-node, TLS, external LB)
│   │   ├── console.yaml             # Redpanda Console
│   │   ├── python-consumer.yaml     # Python confluent-kafka consumer (retail-orders)
│   │   └── mqtt-bridge/
│   │       ├── mosquitto.yaml       # Eclipse Mosquitto MQTT broker
│   │       └── connect.yaml         # Redpanda Connect: MQTT → iot-events
│   └── region-b/                    # eu-central-1 manifests
│       ├── redpanda.yaml            # Redpanda cluster (3-node, TLS)
│       ├── console.yaml             # Redpanda Console
│       ├── shadowlink.yaml          # ShadowLink config — regional- excluded, * included
│       └── amqp-bridge/
│           ├── rabbitmq.yaml        # RabbitMQ 3-management
│           ├── connect.yaml         # Redpanda Connect: iot-events → AMQP
│           └── amqp-consumer.yaml   # Python pika AMQP 0.9.1 consumer
└── consumers/
    └── python/
        ├── retail_consumer.py       # Standalone Kafka/TLS consumer (confluent-kafka)
        ├── amqp_consumer.py         # Standalone AMQP consumer (pika)
        └── requirements.txt
```

---

## Key Config Notes

**ShadowLink filter policy** (`clusters/region-b/shadowlink.yaml`):
- `regional-` prefix → excluded (stays in origin region only)
- All other topics → included (replicates to eu-central-1)

**TLS**: cert-manager provisions TLS on all listeners. ShadowLink uses the source cluster's external CA cert, copied during setup to the `eu-west-1-ca-cert` Secret in eu-central-1.

**Failover is irreversible**: `rpk shadow failover` converts shadow topics to regular writable topics. The `restore` command handles cleanup.

**MQTT client IDs**: After pod restart, a second `rollout restart` may be needed if the old pod's session fights the new pod's connection (Mosquitto "session taken over" loop).
