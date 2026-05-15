# Redpanda Cross-Region Demo

A self-contained demo environment showing Redpanda's enterprise event streaming capabilities across two AWS EKS clusters. Demonstrates Kubernetes-native deployment, cross-region replication with ShadowLink, MQTT protocol bridging, and live DR failover with measurable RTO/RPO.

## Architecture

```
eu-west-1 (primary)                              eu-central-1 (DR)
┌──────────────────────────────────┐             ┌──────────────────────────────────┐
│  Mosquitto MQTT broker           │             │                                  │
│      │                           │             │  Redpanda Connect                │
│  Redpanda Connect                │             │  (Kafka → AMQP 0.9.1 bridge)     │
│  (MQTT → Kafka bridge)           │             │      │                           │
│      │                           │             │  RabbitMQ                        │
│  Redpanda 3-node cluster         │──ShadowLink▶│  Redpanda 3-node cluster         │
│  retail-orders, iot-events       │             │  (shadow topics, read-only)      │
│      │                           │             │      │                           │
│  Python confluent-kafka consumer │             │  Python pika AMQP consumer       │
│  Redpanda Console                │             │  Redpanda Console                │
│  Prometheus + Grafana            │             │  Prometheus + Grafana            │
└──────────────────────────────────┘             └──────────────────────────────────┘
```

**EKS clusters**: `rp-demo-eu-west-1`, `rp-demo-eu-central-1`  
**Instance type**: m7gd.large (Graviton3, local NVMe via LVM CSI)  
**Redpanda Operator**: v26.1.3  
**Topics**: `retail-orders` (order events), `iot-events` (MQTT bridge output)

---

## Setup

```bash
cd demo/
./setup.sh
```

Provisions both EKS clusters, installs cert-manager + LVM CSI + kube-prometheus-stack, deploys the Redpanda Operator and 3-node clusters, enables ShadowLink, deploys all bridge services, and creates topics. Takes ~25–35 minutes.

### Prerequisites

- `eksctl`, `kubectl`, `helm` installed and configured
- AWS credentials with EKS create permissions in eu-west-1 and eu-central-1

---

## Running the Demo — Presenter Runsheet

### Before the meeting (10–15 min)

Run the pre-flight check:

```bash
./demo.sh preflight
```

This verifies every pod, the ShadowLink state, both topics, and prints Console + Grafana URLs. Fix anything red before starting.

**Open these tabs in your browser ahead of time:**
- Redpanda Console eu-west-1 (port 8080) — topic list + message viewer
- Redpanda Console eu-central-1 (port 8080) — shadow topic confirmation
- Grafana eu-west-1 — Redpanda dashboard, keep it visible during chaos demo

---

### Segment 1 — Cross-Region Replication (~5 min)

**What you're showing:** Write once in eu-west-1, read from anywhere. ShadowLink keeps eu-central-1 in sync with zero application-side effort.

**Terminal:**
```bash
# Show current replication state and topic offsets
./demo.sh status
```
> Point to "ShadowLink is active" and the matching high-watermarks between both clusters.

```bash
# Produce 20 order events
./demo.sh produce 20
```
> Walk through the JSON payload — order_id, store, amount, timestamp. These are retail point-of-sale events.

```bash
# Watch messages arrive on both clusters simultaneously
./demo.sh consume-both
```
> Both consumers start printing. eu-central-1 lags slightly — that's the 30-second sync interval. Ctrl+C after you've made the point.

**Browser:** Switch to Console eu-central-1, navigate to the `retail-orders` topic, show messages are there. Click a message to show the full payload.

**Key point:** Consumer group offsets sync too. If eu-central-1 took over as primary right now, consumers would resume from where they left off — no replay, no gap.

---

### Segment 2 — Protocol Bridging (MQTT → Kafka → AMQP) (~7 min)

**What you're showing:** Redpanda as a universal integration hub. IoT devices speak MQTT; backend systems speak AMQP; everything flows through the same Kafka-protocol cluster.

**Terminal — window 1:**
```bash
# Watch the end of the chain — AMQP consumer in eu-central-1
./demo.sh amqp-consume
```
> Leave this running. It's consuming from RabbitMQ, which is fed by a Redpanda Connect pipeline reading from the shadow `iot-events` topic.

**Terminal — window 2:**
```bash
# Publish MQTT events
./demo.sh mqtt-publish 10
```
> Point to the asset types: store checkouts, warehouse conveyors, fleet telemetry. MQTT topic path is `iot/{region}/{asset_type}/{asset_id}/{event_type}`.

Watch window 1 — messages appear with `routing_key=iot-events` and the full enriched JSON.

**Full chain to narrate:**
```
MQTT pub → Mosquitto → Redpanda Connect → iot-events (eu-west-1)
  → ShadowLink → iot-events (eu-central-1)
    → Redpanda Connect → RabbitMQ → pika consumer
```

```bash
# Show bridge pipeline health
./demo.sh amqp-status
```
> Point to the consumer group lag — it should be 0 or near-0, showing the pipeline is keeping up.

**Browser:** Show `iot-events` in Console eu-west-1. Click a message — show the enriched fields (`mqtt_topic`, `region`, `asset_type`, `asset_id`, `event_type`, `ingested_at`). These were added by the Redpanda Connect pipeline's Bloblang processor, not the original MQTT payload.

**Key point:** The AMQP consumer doesn't know Redpanda exists. It's connecting to RabbitMQ. Any existing system that speaks AMQP can consume events from Redpanda without code changes.

---

### Segment 3 — Selective Routing (~4 min)

**What you're showing:** ShadowLink replicates based on policy, not blindly. You can pin sensitive or region-specific data to its origin cluster while still replicating shared event streams everywhere.

**Terminal:**
```bash
./demo.sh routing
```
> This runs automatically — creates two topics, produces to both, counts down 35s, then shows the outcome. Narrate as it runs.

While the countdown runs, explain the filter policy (from `clusters/region-b/shadowlink.yaml`):

| Topic prefix | Rule | Behaviour |
|---|---|---|
| `regional-` | exclude (first match wins) | Stays in eu-west-1 only |
| `*` | include | Replicates to eu-central-1 |

After the countdown:
- `global-alerts` → Replicated ✓
- `regional-eu-west-1-ops` → Local only ✓

**Key point:** This is how you enforce data residency requirements. EU-only operational data stays in the EU region. Shared business events cross regions. The policy lives in one YAML file.

---

### Segment 4 — High Availability (~5 min)

**What you're showing:** Single broker failure is transparent — Raft re-elects a leader, Kubernetes restarts the pod, the cluster heals itself. No pager, no manual steps.

**Browser:** Open Grafana eu-west-1, navigate to the Redpanda Overview dashboard. Have it visible so you can point to the dip and recovery.

**Terminal:**
```bash
./demo.sh chaos
```
> Narrate: "I'm deleting redpanda-0 with `--grace-period=0` — this is a hard kill, not a graceful shutdown." Watch the recovery polling print elapsed time.

When recovery completes, the script prints the measured RTO (typically 30–90 seconds on m7gd.large EKS).

**Browser:** Point to the Grafana timeline — you'll see a brief dip in the "Under-replicated partitions" panel, then it returns to 0 as the cluster recovers. No data was lost.

**Key point:** This is Raft-based consensus — the cluster can absorb broker loss without operator intervention. In a 3-node cluster, you can lose one node and continue producing and consuming. 

---

### Segment 5 — Disaster Recovery Failover (~8 min)

**What you're showing:** When an entire region goes dark, promotion of the shadow cluster to primary is a single command. RTO is measured in seconds, not hours.

> ⚠️  **This is the one irreversible step.** Run `./demo.sh restore` after to reset. Don't skip it if you need to run the demo again.

**Terminal:**
```bash
./demo.sh failover
```

The script will:
1. Print the pre-failover replication lag (**this is your RPO** — messages synced before the outage)
2. Produce 10 messages to eu-west-1
3. Simulate the outage (scale StatefulSet to 0 replicas)
4. Watch ShadowLink detect the disconnection
5. Run `rpk shadow failover --all --no-confirm`
6. Prove eu-central-1 is writable by producing a test message
7. Print RTO (outage start → first write to new primary)

Narrate the RPO result as the lag prints: "These are the messages that synced before the outage. Everything in this window is available on eu-central-1 right now."

**Browser:** After failover, refresh Console eu-central-1 — `retail-orders` is now a regular writable topic, not a shadow. Show that you can navigate to it and see the failover test message.

**After the demo:**
```bash
./demo.sh restore
```
> Scales eu-west-1 back up, deletes the promoted topics, recreates the ShadowLink. Takes ~2 minutes.

---

### Recovery Playbook

Things that can go wrong and how to fix them:

| Symptom | Likely cause | Fix |
|---|---|---|
| `preflight` shows ShadowLink not active | ShadowLink controller restarted | `kubectl --context rp-demo-eu-central-1 -n redpanda describe shadowlink eu-west-1-shadow` — check Events |
| AMQP consumer not receiving messages | Connect pipeline restarted and lost connection | `kubectl --context rp-demo-eu-central-1 -n redpanda rollout restart deployment/connect-amqp-bridge` |
| MQTT bridge not consuming | Client ID conflict after pod restart | `kubectl --context rp-demo-eu-west-1 -n redpanda rollout restart deployment/connect-mqtt-bridge` — wait 30s |
| Shadow topics missing on eu-central-1 | ShadowLink sync hasn't run yet | Wait 30s and check `./demo.sh status` |
| `chaos` — cluster doesn't recover | Pod stuck in Pending | `kubectl --context rp-demo-eu-west-1 -n redpanda describe pod redpanda-0` — look for PVC or node issues |
| `failover` — `rpk shadow failover` fails | ShadowLink already disconnected or topics not ready | Check `rpk shadow status eu-west-1-shadow` on eu-central-1 |
| Demo needs reset after failover | Forgot to run restore | `./demo.sh restore` — safe to run multiple times |

---

## All Commands

| Command | Description |
|---|---|
| `preflight` | Pre-meeting health check — verify all pods, links, and URLs |
| `produce [n]` | Produce n order events to eu-west-1 (default: 10) |
| `consume-source` | Consume `retail-orders` from eu-west-1 |
| `consume-shadow` | Consume `retail-orders` from eu-central-1 |
| `consume-both` | Both clusters side by side |
| `status` | ShadowLink status and topic offsets |
| `full-demo` | Fully scripted end-to-end demo |
| `mqtt-publish [n]` | Publish n MQTT events via Mosquitto (default: 10) |
| `mqtt-consume [source\|shadow]` | Consume `iot-events` as a Kafka client |
| `mqtt-status` | MQTT bridge pipeline status and topic offsets |
| `python-consume [source\|shadow]` | Python confluent-kafka consumer (retail-orders) |
| `amqp-consume` | AMQP 0.9.1 consumer — end of MQTT→Kafka→AMQP chain |
| `amqp-status` | RabbitMQ + AMQP bridge pipeline status |
| `routing` | Policy-based topic routing: global vs regional-scoped topics |
| `chaos` | Kill broker-0, measure self-healing RTO |
| `failover` | Promote eu-central-1 shadow to primary (**irreversible**) |
| `restore` | Scale eu-west-1 back up, recreate ShadowLink |

---

## Observability

Grafana is deployed on both clusters. Get URLs:

```bash
kubectl --context rp-demo-eu-west-1   -n monitoring get svc kube-prometheus-stack-grafana
kubectl --context rp-demo-eu-central-1 -n monitoring get svc kube-prometheus-stack-grafana
```

Credentials: `admin` / retrieve password:
```bash
kubectl --context rp-demo-eu-west-1 -n monitoring get secret kube-prometheus-stack-grafana \
  -o jsonpath='{.data.admin-password}' | base64 -d
```

Redpanda Console:
```bash
kubectl --context rp-demo-eu-west-1   -n redpanda get svc console
kubectl --context rp-demo-eu-central-1 -n redpanda get svc console
```

---

## File Layout

```
demo/
├── README.md                        # This file (presenter runsheet)
├── setup.sh                         # Full environment provisioning (~30 min)
├── demo.sh                          # Interactive demo script
├── eks/
│   ├── cluster-eu-west-1.yaml       # EKS cluster spec (primary, m7gd.large)
│   └── cluster-eu-central-1.yaml    # EKS cluster spec (DR, m7gd.large)
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
│       ├── shadowlink.yaml          # ShadowLink replication config + filter policy
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

## Key Configuration Notes

**TLS**: cert-manager provisions TLS on all Redpanda listeners. ShadowLink uses the source cluster's external CA cert, copied during setup to the `eu-west-1-ca-cert` Secret in eu-central-1.

**ShadowLink sync interval**: 30 seconds for topic metadata, consumer offsets, and schema registry.

**Selective routing policy** (in `clusters/region-b/shadowlink.yaml`):
- Topics prefixed `regional-` → excluded (stay in origin region)
- All other topics → included (replicated to eu-central-1)
- Rules evaluated in order; first match wins.

**Storage**: Local NVMe via LVM CSI (`csi-driver-lvm-striped-xfs` StorageClass). The `m7gd.large` instance provides one NVMe device at `/dev/nvme1n1`.

**Failover is irreversible**: `rpk shadow failover` converts shadow topics to regular writable topics. The `restore` command handles cleanup by deleting those topics and recreating the ShadowLink resource.

**MQTT client IDs**: The Redpanda Connect MQTT bridge uses client ID `rp-connect-mqtt-bridge`. If a pod restart causes a client ID conflict (old pod and new pod both trying to connect), Mosquitto will see a "session taken over" loop. A second `rollout restart` after the old pod fully terminates resolves it.
