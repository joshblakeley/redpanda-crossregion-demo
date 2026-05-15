# Redpanda Cross-Region Demo

A self-contained demo environment showing Redpanda's enterprise event streaming capabilities across two AWS EKS clusters. Demonstrates Kubernetes-native deployment, cross-region replication with ShadowLink, MQTT protocol bridging, and live DR failover with measurable RTO.

## Architecture

```
eu-west-1 (primary)                          eu-central-1 (DR)
┌─────────────────────────────────┐          ┌─────────────────────────┐
│  Mosquitto MQTT broker          │          │                         │
│      │                          │          │                         │
│  Redpanda Connect               │          │                         │
│  (MQTT → Kafka bridge)          │          │                         │
│      │                          │          │                         │
│  Redpanda 3-node cluster        │─────────▶│  Redpanda 3-node        │
│  (retail-orders, iot-events)    │ ShadowLink│  cluster (read-only     │
│                                 │          │  shadow topics)         │
│  Redpanda Console               │          │  Redpanda Console       │
│  Prometheus + Grafana           │          │  Prometheus + Grafana   │
└─────────────────────────────────┘          └─────────────────────────┘
```

**EKS clusters**: `rp-demo-eu-west-1`, `rp-demo-eu-central-1`  
**Instance type**: m7gd.large (Graviton3, local NVMe via LVM CSI)  
**Redpanda Operator**: v26.1.3  
**Topics**: `retail-orders` (order events), `iot-events` (MQTT bridge output)

## Prerequisites

- `eksctl`, `kubectl`, `helm` installed and configured
- AWS credentials with EKS create permissions in eu-west-1 and eu-central-1
- Redpanda `rpk` CLI

## Setup

Provision everything from scratch:

```bash
cd demo/
./setup.sh
```

This will:
1. Create two EKS clusters (m7gd.large, 3 nodes each)
2. Install cert-manager, LVM CSI driver, kube-prometheus-stack
3. Deploy the Redpanda Operator and a 3-node Redpanda cluster on each
4. Enable ShadowLink feature on both clusters
5. Copy the eu-west-1 CA cert to eu-central-1
6. Deploy the ShadowLink resource and wait for it to become `active`

Setup takes approximately 25–35 minutes. Once done:

```bash
# Verify both clusters
kubectl --context rp-demo-eu-west-1   -n redpanda exec -it redpanda-0 -- rpk cluster info
kubectl --context rp-demo-eu-central-1 -n redpanda exec -it redpanda-0 -- rpk cluster info

# Check ShadowLink is active
kubectl --context rp-demo-eu-central-1 -n redpanda get shadowlink eu-west-1-shadow
```

## Demo Script

All demo commands are in `demo.sh`. Run without arguments to see usage:

```bash
./demo.sh
```

### Recommended Demo Flow

Run these in order for a complete end-to-end walkthrough.

---

#### Step 1 — Cross-Region Replication

Show that messages produced in eu-west-1 are automatically replicated to eu-central-1 via ShadowLink:

```bash
# Check ShadowLink replication status and lag
./demo.sh status

# Produce 20 order events to eu-west-1
./demo.sh produce 20

# Watch messages appear on both clusters simultaneously
./demo.sh consume-both
```

ShadowLink syncs topic data, consumer group offsets, and schema registry entries on a 30-second interval.

---

#### Step 2 — MQTT Protocol Bridging

Show heterogeneous protocol ingestion — IoT devices publishing over MQTT arrive as standard Kafka topics:

```bash
# Publish 10 MQTT events (store checkouts, warehouse sensors, fleet telemetry)
./demo.sh mqtt-publish 10

# Confirm the bridge pipeline is running and events are flowing
./demo.sh mqtt-status

# Consume iot-events as a normal Kafka consumer on either cluster
./demo.sh mqtt-consume source
./demo.sh mqtt-consume shadow
```

Redpanda Connect subscribes to `iot/#` on Mosquitto, enriches each message with parsed topic metadata (`region`, `asset_type`, `asset_id`, `event_type`), and produces to the `iot-events` Kafka topic. ShadowLink then replicates `iot-events` to eu-central-1 automatically.

---

#### Step 3 — Selective Routing

Show that ShadowLink replicates based on policy, not blindly. Some topics stay in their origin region; others replicate globally:

```bash
./demo.sh routing
```

This creates two topics on eu-west-1, produces to both, waits one sync interval (35s), then shows the outcome:

| Topic | Policy | Result |
|---|---|---|
| `global-alerts` | matches include-all rule | Replicated to eu-central-1 ✓ |
| `regional-eu-west-1-ops` | matches `regional-` exclude rule | Local only — not on eu-central-1 ✓ |

The filter policy in `clusters/region-b/shadowlink.yaml`:
```yaml
autoCreateShadowTopicFilters:
  - name: "regional-"
    filterType: exclude
    patternType: prefixed
  - name: "*"
    filterType: include
    patternType: literal
```

Rules are evaluated in order — first match wins. Any topic prefixed `regional-` is excluded before the wildcard include is reached. This maps directly to data residency requirements: EU-only operational topics stay in EU regions, while shared event streams replicate as needed.

---

#### Step 4 — HA: Single Broker Failure and Self-Healing

Show that a broker failure is transparent — Raft re-elects a leader and the cluster heals without manual intervention:

```bash
./demo.sh chaos
```

This kills `redpanda-0` on eu-west-1 with `--grace-period=0`, polls until the pod recovers and `rpk cluster health` reports healthy, then prints the measured RTO. Safe to run repeatedly.

---

#### Step 5 — DR: Regional Failover

Show full regional disaster recovery with ShadowLink promotion:

```bash
# Simulate eu-west-1 going down; promote eu-central-1 to primary
# WARNING: This is irreversible — run restore afterwards
./demo.sh failover
```

This will:
1. Report pre-failover replication lag (your RPO baseline)
2. Scale the eu-west-1 StatefulSet to 0 (simulates regional outage)
3. Wait for ShadowLink to detect disconnection
4. Run `rpk shadow failover --all --no-confirm` to promote all shadow topics to writable
5. Produce a test message to prove eu-central-1 is now writable
6. Print total RTO (outage start → first write to new primary)

After seeing the failover:

```bash
# Restore eu-west-1 and recreate the ShadowLink for another run
./demo.sh restore
```

---

### All Commands

| Command | Description |
|---|---|
| `produce [n]` | Produce n order events to eu-west-1 (default: 10) |
| `consume-source` | Consume `retail-orders` from eu-west-1 |
| `consume-shadow` | Consume `retail-orders` from eu-central-1 |
| `consume-both` | Both clusters side by side |
| `status` | ShadowLink status and topic offsets |
| `full-demo` | Fully scripted end-to-end demo |
| `mqtt-publish [n]` | Publish n MQTT events via Mosquitto (default: 10) |
| `mqtt-consume [source\|shadow]` | Consume `iot-events` as a Kafka client |
| `mqtt-status` | MQTT bridge pipeline status and topic offsets |
| `routing` | Policy-based topic routing: global vs regional-scoped topics |
| `chaos` | Kill broker-0, measure self-healing RTO |
| `failover` | Promote eu-central-1 shadow to primary (**irreversible**) |
| `restore` | Scale eu-west-1 back up, recreate ShadowLink |

## Observability

Grafana is deployed on both clusters with the official Redpanda dashboards pre-imported.

```bash
# Get Grafana URL (eu-west-1)
kubectl --context rp-demo-eu-west-1 -n monitoring get svc kube-prometheus-stack-grafana

# Get Grafana URL (eu-central-1)
kubectl --context rp-demo-eu-central-1 -n monitoring get svc kube-prometheus-stack-grafana
```

Default credentials: `admin` / retrieve password with:

```bash
kubectl --context rp-demo-eu-west-1 -n monitoring get secret kube-prometheus-stack-grafana \
  -o jsonpath='{.data.admin-password}' | base64 -d
```

Redpanda Console is also available:

```bash
kubectl --context rp-demo-eu-west-1   -n redpanda get svc console
kubectl --context rp-demo-eu-central-1 -n redpanda get svc console
```

## File Layout

```
demo/
├── README.md                        # This file
├── setup.sh                         # Full environment provisioning
├── demo.sh                          # Interactive demo script
├── eks/
│   ├── cluster-eu-west-1.yaml       # EKS cluster spec (primary)
│   └── cluster-eu-central-1.yaml    # EKS cluster spec (DR)
└── clusters/
    ├── region-a/                    # eu-west-1 manifests
    │   ├── redpanda.yaml            # Redpanda cluster (3-node, TLS, external LB)
    │   ├── console.yaml             # Redpanda Console
    │   └── mqtt-bridge/
    │       ├── mosquitto.yaml       # Eclipse Mosquitto MQTT broker
    │       └── connect.yaml         # Redpanda Connect pipeline (MQTT → Kafka)
    └── region-b/                    # eu-central-1 manifests
        ├── redpanda.yaml            # Redpanda cluster (3-node, TLS)
        ├── console.yaml             # Redpanda Console
        └── shadowlink.yaml          # ShadowLink cross-region replication config
```

## Key Configuration Notes

**TLS**: cert-manager automatically provisions TLS on all listeners. External connections use the cluster's `redpanda-external-root-certificate` CA. ShadowLink uses the source cluster's external CA cert (copied during setup to the `eu-west-1-ca-cert` Secret in eu-central-1).

**ShadowLink sync intervals**: Topic metadata, consumer offsets, and schema registry all sync every 30 seconds.

**Storage**: Local NVMe via LVM CSI (`csi-driver-lvm-striped-xfs` StorageClass). The `m7gd.large` instance provides one NVMe device at `/dev/nvme1n1`.

**Failover is irreversible**: Once `rpk shadow failover` runs, shadow topics become regular writable topics. The `restore` command handles cleanup by deleting and recreating the ShadowLink resource and the promoted topics.
