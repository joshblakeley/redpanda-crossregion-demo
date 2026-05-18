# Redpanda Cross-Region Demo
## IKEA (Ingka Group) — Event Messaging Platform Evaluation

**Demo**: Wed 20 May 2026, 10:00 AM BST  
**Format**: 90 minutes, 9 segments — Jeff owns bookends, Josh owns technical segments  
**Evaluators**: Nihar Shah (Eng Leader, decision-maker), Martin Hilferink (Sr Tech Architect), Subhasish Bhabani (Solace Operator), Björn Ramberg (Exec Sponsor)

> This is not a Solace replacement demo. It's "build the event infrastructure that will run IKEA's AI agent estate for the next five years." Surface this at the close.

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

**⚠ Say this proactively in Segment 0:** *"Our demo runs on AWS EKS — the operator and broker are cloud-agnostic, identical on GCP GKE, Azure AKS, on-prem Kubernetes, and AliCloud ACK. Ingka's environment is all four of those."* Don't wait to be asked.

---

## Demo Flow — 90 Minutes

### Segment 0 — Context Setting (5 min) | Jeff

Jeff opens. Two jobs for this segment:

**1. Acknowledge the 2023 evaluation generically — don't name Bruno:**
> *"We know your team looked at Redpanda before — here's what has changed."*  
Three things on one slide: Schema Registry built-in (GA 2023), Tiered Storage GA, Operator-driven scale-down automated.

**2. State cloud-agnosticism immediately:**
> *"Our demo runs on AWS EKS. The operator and broker are identical on GCP GKE, Azure AKS, on-prem Kubernetes, and AliCloud ACK — same YAML, same GitOps model, same day-2 behaviour."*

**Josh picks up with:**
> *"Most vendors will show you a Solace replacement today. We want to show you something different — the streaming backbone for your AI Agent gateways, your data mesh, your inference pipelines, and your compliance logging. The Solace migration is the entry point. The AI infrastructure is where this platform goes."*

---

### Segment 1 — Current → Future Architecture Recap (5–7 min) | Jeff

Single slide showing what Redpanda understands about the current Solace estate and target state.

**Cover explicitly:**
- What's being replaced: Solace event mesh (Sweden on-prem, GCP, AliCloud, Azure, AWS) + Kong+Solace Partner Portal + SMF protocol translation
- Target state: self-hosted K8s-native, AliCloud included, 10K msg/sec, 500–1,000 consumers across 50+ markets, 80–95% tiered storage offload
- Three things that matter most: zero message loss on failover for SL1 platforms, minimal ops overhead, migration path with no day-one Solace refactoring

**Invite correction:** *"Before Josh opens a terminal, does this match your topology? Correct us now."*

> **Why this matters:** Shows Subhasish exactly what happens to his existing producers (bridge path, no day-one refactoring). Shows Martin that the Kong+Solace translation layer disappears entirely.

---

### Segment 2 — Reference Customer (3–4 min) | Jeff

Large global enterprise, self-hosted BYOC, multi-region, migration from legacy messaging, or regulated data residency. Retail or IoT-heavy reference preferred. Objective: prove Redpanda has done this migration before, not just greenfield deployments.

---

### Segment 3 — Kubernetes-Native Platform & GitOps (8 min) | Josh

**Assessment: STRONG. Frame for Nihar (25% cost reduction) and Björn (governance).**

```bash
# Start a live producer in the background first, then trigger the rolling restart
./demo.sh upgrade
```

Narrate as it runs:
> *"Your platform team writes YAML. The operator provisions, upgrades, and scales the cluster. No manual broker operations. The full environment is reproducible from a Git repo — every change is a PR, every state is auditable."*

Watch the counter — messages produced during the rolling restart, messages lost = 0.

**Browser:** Point to Grafana — under-replicated partitions briefly non-zero per broker, returns to 0 as each one rejoins. No producer interruption.

**Key hits:**
- **Nihar:** "This is how you get from 'reducing operational costs' to 25% — the operator handles every upgrade, every patch, every scale event."
- **Björn:** "Governance and enablement for the entire Ingka developer community — every infrastructure change is a PR."

**ADP connection:** *"This is also how AI agent infrastructure gets deployed and managed at scale — same operator model runs your streaming platform and your AI event infrastructure."*

---

### Segment 4 — Cross-Region Replication / ShadowLink (8 min) | Josh

**Assessment: STRONG. Land offset-sync and schema registry sync explicitly — Nihar's most critical requirement.**

```bash
./demo.sh produce 20       # produce to eu-west-1
./demo.sh consume-both     # watch both clusters receive messages
./demo.sh status           # show consumer group offsets synced cross-region
```

State explicitly:
> *"When the primary region fails, consumers switch to the DR cluster and resume from exactly the last message they processed. No replay. No data loss. No reconfiguration."*

**Explicitly call out schema registry sync** — Ingka has an existing schema registry and a migration path concern. Point to the `schemaRegistrySyncOptions` in shadowlink.yaml:
```bash
kubectl --context rp-demo-eu-central-1 -n redpanda get shadowlink eu-west-1-shadow -o yaml | grep -A5 schemaRegistry
```

**Browser:** Show Grafana — throughput dropping on primary, coming up on DR.

**Key hits:**
- **Nihar:** "SL1 for 50+ markets — schema registry sync means your consumer contracts stay intact across a regional failure."
- **Schema registry:** Redpanda built-in SR is Confluent SR API-compatible — existing SR tooling works.

**ADP connection:** *"The same replication that protects your operational event flows also replicates agent context, inference logs, and model event streams across regions — without loss, without replay."*

---

### Segment 5 — Protocol Bridging + Solace Migration Architecture (12 min) | Josh

**Assessment: NEEDS PROACTIVE FRAMING. Own the RabbitMQ before anyone asks. Add Solace migration path.**

```bash
# Terminal 1: watch the end of the chain
./demo.sh amqp-consume

# Terminal 2: publish MQTT events
./demo.sh mqtt-publish 10
```

**Own the RabbitMQ immediately:**
> *"We're using RabbitMQ here as an AMQP endpoint to illustrate the pattern — in your environment this is any AMQP consumer. Redpanda Connect handles protocol translation at the edge; the broker stays pure Kafka API throughout."*

**For Subhasish — walk the Solace migration path explicitly:**
> *"Connect has a Solace SMF source connector. Your existing producers stay on Solace. Events bridge into Redpanda. Consumers migrate at their own pace. Week one looks like this: nothing changes for your 250+ SMF-connected endpoints. They keep publishing to Solace. Connect reads from Solace and writes to Redpanda. Migration is topic-by-topic, team-by-team."*

**Tie to MQTT/IKEA reality:**
- 250+ warehouse drones at 73 locations → MQTT → Redpanda Connect → `iot-events` topic
- This is directly how IKEA's IoT estate works today

```bash
./demo.sh amqp-status     # show pipeline health, consumer group lag
```

**ADP connection:** *"The same Connect pipeline that ingests MQTT telemetry from your 250 warehouse drones is also how sensor data feeds AI pipelines in real time — computer vision, inventory models, drone autonomy. You're not building two pipelines. You're building one."*

---

### Segment 6 — Policy-Based Routing & Data Residency (6 min) | Josh

**Assessment: WEAKEST SEGMENT. Reframe as GitOps governance and EU AI Act infrastructure. Don't show it as a two-line filter — show it as the architecture of compliance.**

```bash
./demo.sh routing
```

**Lead with the reframe for Martin** (who built the Solace mesh and knows what Solace's routing UI looks like):
> *"Routing policy is code. It lives in your GitOps repository — every change is a pull request with a reviewer and a complete audit trail. That's a stronger governance model than a UI where a routing rule can be changed with a click and no record."*

Watch the countdown. After the result:
- `eu-transaction-events` → replicated to DR cluster ✓
- `regional-eu-west-1-ops` → LOCAL ONLY ✓ (matched 'regional-' exclude rule)

**Name Cyber Law compliance explicitly.** Chinese data didn't leave eu-west-1. That's not a naming convention — it's enforced by the broker before any data leaves the network.

**Browser:** Show Console → Audit Log. Every ShadowLink filter change, every ACL update, every topic creation is logged. This is the governance trail.

**Key hits:**
- **Martin:** Policy-as-code resonates with his MCP/AI gateway direction — same model as his API gateway governance
- **Nihar:** Governance is part of SL1 platform requirements

**ADP connection:** *"Data residency enforcement at the broker layer is also how you satisfy the EU AI Act's requirement that model training data and inference inputs are processed only in permitted jurisdictions — enforced at the infrastructure layer, not the application layer."*

---

### Segment 7 — High Availability + Multi-Tenancy (5 min) | Josh

**Assessment: STRONG. Kill the broker AND show blast radius after — Section 5.3 of demo requirements asks for multi-tenancy explicitly.**

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

**Key hit for Nihar:** Blast radius containment is a day-1 operational requirement for a 50+ market platform. This is not theoretical.

---

### Segment 8 — Disaster Recovery (10 min) | Josh

**Assessment: STRONG. Add Grafana visibility. Close the 2023 'infinite retention' objection.**

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

**Close the 2023 'infinite retention' objection** (without naming Bruno):
> *"One concern from a previous evaluation was that Redpanda was disk-bound. Tiered Storage offloads 80–95% of data to object storage — S3, GCS, AliOSS. Consumers read historical data transparently — no difference between a message from yesterday and a message from six months ago."*

**Show 10MB payload support (if not shown earlier):**
```bash
./demo.sh produce-large
```
> *"Your RFI states some publishers up to 10MB. Handled."*

**Key hits:**
- **Subhasish:** Ops console view of the failover — this is his day job
- **Everyone:** Seamless historical read from tiered storage = infinite retention objection closed

---

### Segment 9 — Iceberg + ADP Close (5 min) | Jeff

Jeff closes with the forward-looking platform investment story:

> *"You came here evaluating a Solace replacement. What you're actually selecting is the streaming backbone for the next five years of your data and AI architecture."*

Three points:
- **Iceberg-compatible tiered storage:** 80–95% offloaded to object storage is queryable directly from Databricks, Snowflake — no pulling through the broker. Martin has Databricks in the stack already.
- **EU AI Act audit trail:** Redpanda's immutable append-only log is architecturally aligned with AI Act inference logging requirements.
- **Agentic Data Plane:** Martin is building MCP servers and AI Agent gateways right now. The event infrastructure those agents need — real-time context delivery, agent-to-agent communication, tool call event streams — is exactly what Redpanda was built for.

Close line: *"The Solace migration funds the AI infrastructure. Same cluster, same data, same guarantees."*

Then Jeff asks: *"Can we schedule a follow-up architecture session with your full platform team before the RFP document drops?"*

---

## Key Landmines to Pre-empt

| Landmine | Who Raises It | Pre-emption |
|---|---|---|
| Demo runs on AWS, we're on GCP/Azure/AliCloud | Martin or Subhasish | State in Segment 0: operator is cloud-agnostic, identical on GCP GKE, Azure AKS, on-prem K8s, AliCloud ACK |
| Why is there a RabbitMQ? | Subhasish | Own it in Segment 5 before they ask — frame as edge translation pattern, not a dependency |
| Policy routing looks thin vs Solace mesh UI | Martin (built the Solace mesh) | GitOps reframe: policy is code, lives in Git, every change is a PR. Stronger governance than a UI |
| Migration path for 250+ Solace endpoints | Subhasish | Connect Solace SMF source connector = no day-one refactoring. Migration is incremental, topic-by-topic |
| Can you really run on AliCloud? | Any evaluator | Immediate, confident: self-hosted on AliCloud ACK, same operator, same GitOps model |
| Vendor stability — 5-year platform bet | Björn (5 months in role) | Funding, ARR trajectory, customer logos, independence from IBM/Confluent |
| Demo shows 2 regions, we have 5+ across 4 clouds | Any evaluator | ShadowLink scales to N clusters. Routing policy is additive YAML. Show the pattern — scale follows |
| Schema registry compatibility | Subhasish | Built-in SR GA since 2023. Full Confluent SR API compatibility. External SR also supported |

**Do NOT name Bruno Gouveia.** He ran the 2023 evaluation and knows Redpanda's historical gaps. Address his objections (scale-down, infinite retention, schema registry) proactively in the demo flow without referencing him.

---

## 2023 Objections — Close Them Proactively

| 2023 Concern | Answer |
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
