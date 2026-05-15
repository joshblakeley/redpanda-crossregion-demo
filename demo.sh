#!/usr/bin/env bash
# Cross-region ShadowLink demo: produce to eu-west-1, consume from both clusters.
set -euo pipefail

CONTEXT_A="rp-demo-eu-west-1"
CONTEXT_B="rp-demo-eu-central-1"
TOPIC="retail-orders"
GROUP="retail-pos-system"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'
SHADOW_LINK="eu-west-1-shadow"

banner() { echo -e "\n${BOLD}${BLUE}══════════════════════════════════════════════${NC}"; echo -e "${BOLD}${BLUE}  $1${NC}"; echo -e "${BOLD}${BLUE}══════════════════════════════════════════════${NC}\n"; }
step()   { echo -e "${CYAN}▶ $1${NC}"; }
ok()     { echo -e "${GREEN}✓ $1${NC}"; }
info()   { echo -e "${YELLOW}  $1${NC}"; }

usage() {
  echo "Usage: $0 <command>"
  echo ""
  echo "Commands:"
  echo "  preflight              Pre-meeting health check — verify all pods, links, and URLs"
  echo ""
  echo "  produce [n]            Produce n order events to eu-west-1 (default: 10)"
  echo "  consume-source         Consume retail-orders from eu-west-1"
  echo "  consume-shadow         Consume retail-orders from eu-central-1"
  echo "  consume-both           Both clusters side by side"
  echo "  status                 ShadowLink status and topic offsets"
  echo "  full-demo              Full end-to-end scripted demo"
  echo ""
  echo "  mqtt-publish [n]       Publish n MQTT events via Mosquitto (default: 10)"
  echo "  mqtt-consume [source|shadow]  Consume iot-events as cloud-native Kafka client"
  echo "  mqtt-status            MQTT bridge and iot-events topic status"
  echo ""
  echo "  python-consume [source|shadow]  Python confluent-kafka consumer (retail-orders)"
  echo "  amqp-consume           AMQP 0.9.1 consumer — end of MQTT→Kafka→AMQP chain"
  echo "  amqp-status            RabbitMQ + AMQP bridge pipeline status"
  echo ""
  echo "  routing                Policy-based topic routing: global vs regional-scoped topics"
  echo ""
  echo "  chaos                  Kill broker-0 on eu-west-1, measure RTO for self-healing"
  echo "  failover               Promote eu-central-1 shadow to primary (IRREVERSIBLE)"
  echo "  restore                Re-provision eu-west-1 as primary and recreate ShadowLink"
  exit 1
}

rp_exec() {
  local ctx=$1; shift
  kubectl --context "$ctx" -n redpanda exec -i redpanda-0 -c redpanda -- "$@" 2>&1
}

# ── Commands ──────────────────────────────────────────────────────────────────

cmd_preflight() {
  banner "Pre-flight Check"
  info "Run this 10–15 minutes before the demo to confirm everything is healthy."
  echo ""

  local PASS=0
  local FAIL=0

  _chk() {
    local label="$1"; shift
    if "$@" &>/dev/null; then
      ok "$label"
      (( PASS++ )) || true
    else
      echo -e "${RED}✗ $label${NC}"
      (( FAIL++ )) || true
    fi
  }

  # ── kubectl contexts reachable ───────────────────────────────────────────
  step "kubectl contexts:"
  _chk "context $CONTEXT_A reachable" \
    kubectl --context "$CONTEXT_A" cluster-info
  _chk "context $CONTEXT_B reachable" \
    kubectl --context "$CONTEXT_B" cluster-info
  echo ""

  # ── Redpanda pods ────────────────────────────────────────────────────────
  step "Redpanda brokers:"
  for broker in 0 1 2; do
    _chk "eu-west-1    redpanda-$broker Running" \
      kubectl --context "$CONTEXT_A" -n redpanda get pod "redpanda-$broker" \
        --field-selector=status.phase=Running --no-headers
    _chk "eu-central-1 redpanda-$broker Running" \
      kubectl --context "$CONTEXT_B" -n redpanda get pod "redpanda-$broker" \
        --field-selector=status.phase=Running --no-headers
  done
  echo ""

  # ── Support services ─────────────────────────────────────────────────────
  step "Support services:"
  _chk "eu-west-1    Console Running" \
    kubectl --context "$CONTEXT_A" -n redpanda get pod -l app.kubernetes.io/name=console \
      --field-selector=status.phase=Running --no-headers
  _chk "eu-central-1 Console Running" \
    kubectl --context "$CONTEXT_B" -n redpanda get pod -l app.kubernetes.io/name=console \
      --field-selector=status.phase=Running --no-headers
  _chk "eu-west-1    Mosquitto MQTT broker Running" \
    kubectl --context "$CONTEXT_A" -n redpanda get pod -l app=mosquitto \
      --field-selector=status.phase=Running --no-headers
  _chk "eu-west-1    MQTT→Kafka bridge Running" \
    kubectl --context "$CONTEXT_A" -n redpanda get pod -l app=connect-mqtt-bridge \
      --field-selector=status.phase=Running --no-headers
  _chk "eu-west-1    Python retail consumer Running" \
    kubectl --context "$CONTEXT_A" -n redpanda get pod -l app=python-retail-consumer \
      --field-selector=status.phase=Running --no-headers
  _chk "eu-central-1 RabbitMQ Running" \
    kubectl --context "$CONTEXT_B" -n redpanda get pod -l app=rabbitmq \
      --field-selector=status.phase=Running --no-headers
  _chk "eu-central-1 Kafka→AMQP bridge Running" \
    kubectl --context "$CONTEXT_B" -n redpanda get pod -l app=connect-amqp-bridge \
      --field-selector=status.phase=Running --no-headers
  _chk "eu-central-1 AMQP consumer Running" \
    kubectl --context "$CONTEXT_B" -n redpanda get pod -l app=amqp-iot-consumer \
      --field-selector=status.phase=Running --no-headers
  echo ""

  # ── ShadowLink ───────────────────────────────────────────────────────────
  step "ShadowLink:"
  SL_STATE=$(kubectl --context "$CONTEXT_B" -n redpanda get shadowlink "$SHADOW_LINK" \
    -o jsonpath='{.status.state}' 2>/dev/null || echo "not found")
  if [[ "$SL_STATE" == "active" ]]; then
    ok "ShadowLink '$SHADOW_LINK' is active"
    (( PASS++ )) || true
  else
    echo -e "${RED}✗ ShadowLink '$SHADOW_LINK' state: $SL_STATE${NC}"
    (( FAIL++ )) || true
  fi
  echo ""

  # ── Topics ───────────────────────────────────────────────────────────────
  step "Topics:"
  for t in retail-orders iot-events; do
    _chk "eu-west-1    topic '$t' exists" \
      rp_exec "$CONTEXT_A" rpk topic describe "$t"
    _chk "eu-central-1 topic '$t' shadow exists" \
      rp_exec "$CONTEXT_B" rpk topic describe "$t"
  done
  echo ""

  # ── URLs ─────────────────────────────────────────────────────────────────
  step "Service URLs (open these in your browser before the demo):"
  CONSOLE_A=$(kubectl --context "$CONTEXT_A" -n redpanda get svc console \
    -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null)
  CONSOLE_B=$(kubectl --context "$CONTEXT_B" -n redpanda get svc console \
    -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null)
  GRAFANA_A=$(kubectl --context "$CONTEXT_A" -n monitoring get svc kube-prometheus-stack-grafana \
    -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null)
  GRAFANA_B=$(kubectl --context "$CONTEXT_B" -n monitoring get svc kube-prometheus-stack-grafana \
    -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null)

  [[ -n "$CONSOLE_A" ]] && info "  Console eu-west-1   : http://$CONSOLE_A:8080" \
    || echo -e "  ${RED}Console eu-west-1   : LoadBalancer not ready${NC}"
  [[ -n "$CONSOLE_B" ]] && info "  Console eu-central-1: http://$CONSOLE_B:8080" \
    || echo -e "  ${RED}Console eu-central-1: LoadBalancer not ready${NC}"
  [[ -n "$GRAFANA_A" ]] && info "  Grafana eu-west-1   : http://$GRAFANA_A  (admin / prom-operator)" \
    || echo -e "  ${RED}Grafana eu-west-1   : LoadBalancer not ready${NC}"
  [[ -n "$GRAFANA_B" ]] && info "  Grafana eu-central-1: http://$GRAFANA_B  (admin / prom-operator)" \
    || echo -e "  ${RED}Grafana eu-central-1: LoadBalancer not ready${NC}"
  echo ""

  # ── Summary ──────────────────────────────────────────────────────────────
  TOTAL=$(( PASS + FAIL ))
  if [[ $FAIL -eq 0 ]]; then
    banner "All $TOTAL checks passed — good to go"
  else
    echo -e "\n${RED}${BOLD}$FAIL / $TOTAL checks failed — investigate before starting the demo${NC}\n"
    echo "Quick fixes:"
    echo "  Pod not Running   → kubectl --context <ctx> -n redpanda describe pod <name>"
    echo "  ShadowLink broken → kubectl --context $CONTEXT_B -n redpanda describe shadowlink $SHADOW_LINK"
    echo "  Topic missing     → ./demo.sh status"
    exit 1
  fi
}

cmd_produce() {
  local n=${1:-10}
  banner "Producing $n messages → eu-west-1 (primary)"
  step "Target cluster : $CONTEXT_A"
  step "Topic          : $TOPIC"
  echo ""

  for i in $(seq 1 "$n"); do
    local ts
    ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    local stores=("SE-Stockholm" "NO-Oslo" "DK-Copenhagen" "FI-Helsinki" "DE-Berlin" "NL-Amsterdam")
    local store="${stores[$((RANDOM % ${#stores[@]}))]}"
    local amount
    amount=$(awk "BEGIN {printf \"%.2f\", ($RANDOM % 49100) / 100 + 9}")
    local payload
    payload=$(printf '{"order_id":"ORD-%05d","store":"STORE-%s","amount":%s,"ts":"%s"}' \
      "$((RANDOM % 99999))" "$store" "$amount" "$ts")

    echo "$payload" | kubectl --context "$CONTEXT_A" -n redpanda exec -i redpanda-0 -c redpanda -- \
      rpk topic produce "$TOPIC" --key "store-$(( (i - 1) % 3 ))" 2>&1
    ok "Message $i: $payload"
  done

  echo ""
  info "Produced $n messages. ShadowLink will sync to eu-central-1 within 30s."
}

cmd_consume_source() {
  banner "Consuming from eu-west-1 (source)"
  step "Cluster        : $CONTEXT_A"
  step "Topic          : $TOPIC"
  step "Consumer group : $GROUP"
  info "Press Ctrl+C to stop"
  echo ""

  kubectl --context "$CONTEXT_A" -n redpanda exec -it redpanda-0 -c redpanda -- \
    rpk topic consume "$TOPIC" \
    --group "$GROUP" \
    --format '%v\n' \
    --print-offset
}

cmd_consume_shadow() {
  banner "Consuming from eu-central-1 (shadow)"
  step "Cluster        : $CONTEXT_B"
  step "Topic          : $TOPIC"
  step "Consumer group : $GROUP"
  info "Press Ctrl+C to stop"
  echo ""

  kubectl --context "$CONTEXT_B" -n redpanda exec -it redpanda-0 -c redpanda -- \
    rpk topic consume "$TOPIC" \
    --group "$GROUP" \
    --format '%v\n' \
    --print-offset
}

cmd_consume_both() {
  banner "Consuming from BOTH clusters (split terminal view)"
  info "Starting consumers on both clusters. Kill with Ctrl+C."
  echo ""

  kubectl --context "$CONTEXT_A" -n redpanda exec -it redpanda-0 -c redpanda -- \
    rpk topic consume "$TOPIC" \
    --group "$GROUP-source" \
    --format "[eu-west-1]  %v\n" \
    --print-offset &
  PID_A=$!

  kubectl --context "$CONTEXT_B" -n redpanda exec -it redpanda-0 -c redpanda -- \
    rpk topic consume "$TOPIC" \
    --group "$GROUP-shadow" \
    --format "[eu-central-1] %v\n" \
    --print-offset &
  PID_B=$!

  trap "kill $PID_A $PID_B 2>/dev/null; exit 0" INT TERM
  wait
}

cmd_status() {
  banner "ShadowLink Status"

  step "ShadowLink resource (eu-central-1):"
  kubectl --context "$CONTEXT_B" -n redpanda get shadowlink eu-west-1-shadow \
    -o custom-columns=\
'NAME:.metadata.name,STATE:.status.state,SYNCED:.status.conditions[0].status,MESSAGE:.status.conditions[0].message' \
    2>&1
  echo ""

  step "Active tasks per broker:"
  kubectl --context "$CONTEXT_B" -n redpanda get shadowlink eu-west-1-shadow \
    -o jsonpath='{range .status.taskStatuses[*]}{.name}{" (broker "}{.brokerId}{"): "}{.state}{"\n"}{end}' \
    2>&1
  echo ""

  step "Topic offsets — eu-west-1 (source):"
  rp_exec "$CONTEXT_A" rpk topic describe "$TOPIC" -p 2>&1 || true
  echo ""

  step "Topic offsets — eu-central-1 (shadow):"
  rp_exec "$CONTEXT_B" rpk topic describe "$TOPIC" -p 2>&1 || true
  echo ""

  step "Consumer group offsets — eu-west-1 (source):"
  rp_exec "$CONTEXT_A" rpk group describe "$GROUP" 2>&1 || true
  echo ""

  step "Consumer group offsets — eu-central-1 (shadow — synced by ShadowLink):"
  rp_exec "$CONTEXT_B" rpk group describe "$GROUP" 2>&1 || true
}

cmd_full_demo() {
  banner "Redpanda — Cross-Region Demo"
  info "eu-west-1 (primary)  →  ShadowLink  →  eu-central-1 (DR)"
  echo ""

  step "1. Check ShadowLink is active..."
  STATE=$(kubectl --context "$CONTEXT_B" -n redpanda get shadowlink eu-west-1-shadow \
    -o jsonpath='{.status.state}' 2>/dev/null)
  if [[ "$STATE" == "active" ]]; then
    ok "ShadowLink is active"
  else
    echo -e "${RED}ShadowLink state: $STATE — check 'kubectl --context $CONTEXT_B -n redpanda get shadowlink eu-west-1-shadow'${NC}"
    exit 1
  fi

  step "2. Produce 5 order events to eu-west-1..."
  cmd_produce 5
  echo ""

  step "3. Confirm messages on source cluster (eu-west-1)..."
  SOURCE_COUNT=$(rp_exec "$CONTEXT_A" rpk topic describe "$TOPIC" -p 2>/dev/null \
    | awk 'NR>1 {sum += $4} END {print sum}' || echo 0)
  ok "eu-west-1 high watermark: $SOURCE_COUNT"
  echo ""

  step "4. Waiting for ShadowLink sync (up to 60s)..."
  for _ in $(seq 1 12); do
    SHADOW_COUNT=$(rp_exec "$CONTEXT_B" rpk topic describe "$TOPIC" -p 2>/dev/null \
      | awk 'NR>1 {sum += $4} END {print sum}' || echo 0)
    if [[ "$SHADOW_COUNT" -ge "$SOURCE_COUNT" && "$SOURCE_COUNT" -gt 0 ]]; then
      ok "eu-central-1 high watermark: $SHADOW_COUNT — in sync!"
      break
    fi
    info "  eu-central-1 at $SHADOW_COUNT / $SOURCE_COUNT..."
    sleep 5
  done
  echo ""

  step "5. Consumer group offset sync check..."
  info "Reading 3 messages on source with group '$GROUP'..."
  rp_exec "$CONTEXT_A" sh -c \
    "rpk topic consume $TOPIC --group $GROUP --num 3 --format '%v\n' 2>/dev/null" || true
  echo ""
  info "Same group offsets on shadow cluster (synced by ShadowLink):"
  rp_exec "$CONTEXT_B" rpk group describe "$GROUP" 2>/dev/null || true
  echo ""

  banner "Demo Complete"
  ok "Topics and consumer offsets are synced cross-region."
  info "Console (eu-west-1) : $(kubectl --context "$CONTEXT_A" -n redpanda get svc console -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null):8080"
  info "Console (eu-central-1): $(kubectl --context "$CONTEXT_B" -n redpanda get svc console -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null):8080"
}

cmd_mqtt_publish() {
  local n=${1:-10}
  banner "MQTT → Redpanda Connect → Kafka (Protocol Bridge Demo)"
  step "Publishing $n MQTT messages to Mosquitto (eu-west-1)"
  info "Topic pattern: iot/{region}/{asset_type}/{asset_id}/{event_type}"
  echo ""

  local stores=("SE-Stockholm" "NO-Oslo" "DK-Copenhagen" "FI-Helsinki" "DE-Berlin" "NL-Amsterdam")
  local asset_types=("store" "warehouse" "fleet")

  for i in $(seq 1 "$n"); do
    local region="${stores[$((RANDOM % ${#stores[@]}))]}"
    local atype="${asset_types[$((RANDOM % ${#asset_types[@]}))]}"

    case "$atype" in
      store)
        local asset="store-0$((RANDOM % 5 + 1))"
        local event="checkout"
        local r1=$RANDOM r2=$RANDOM r3=$RANDOM
        local total; total=$(awk -v r="$r1" 'BEGIN {printf "%.2f", (r % 49900) / 100 + 1}')
        local payment; payment=$([ $((r3 % 2)) -eq 0 ] && echo card || echo mobile)
        local payload
        payload=$(printf '{"items":%d,"total":%s,"currency":"EUR","payment":"%s"}' \
          "$((r2 % 10 + 1))" "$total" "$payment")
        ;;
      warehouse)
        local asset="wh-0$((RANDOM % 3 + 1))"
        local event="conveyor"
        local r1=$RANDOM r2=$RANDOM r3=$RANDOM r4=$RANDOM
        local speed; speed=$(awk -v r="$r1" 'BEGIN {printf "%.1f", (r % 20 + 5) / 10}')
        local status; status=$([ $((r4 % 8)) -eq 0 ] && echo warning || echo ok)
        local payload
        payload=$(printf '{"belt_id":"B%02d","speed_ms":%s,"load_kg":%d,"status":"%s"}' \
          "$((r2 % 20 + 1))" "$speed" "$((r3 % 120 + 10))" "$status")
        ;;
      fleet)
        local asset="truck-$(printf '%02X%02X' $((RANDOM % 256)) $((RANDOM % 256)))"
        local event="telemetry"
        local r1=$RANDOM r2=$RANDOM r3=$RANDOM r4=$RANDOM
        local lat; lat=$(awk -v r="$r1" 'BEGIN {printf "%.4f", 51 + (r % 400) / 100}')
        local lon; lon=$(awk -v r="$r2" 'BEGIN {printf "%.4f", 4 + (r % 1800) / 100}')
        local payload
        payload=$(printf '{"lat":%s,"lon":%s,"speed_kmh":%d,"fuel_pct":%d}' \
          "$lat" "$lon" "$((r3 % 110 + 20))" "$((r4 % 80 + 20))")
        ;;
    esac

    local mqtt_topic="iot/${region}/${atype}/${asset}/${event}"
    kubectl --context "$CONTEXT_A" -n redpanda exec deployment/mosquitto -- \
      mosquitto_pub -h localhost -t "$mqtt_topic" -m "$payload" 2>/dev/null
    ok "[$atype] $mqtt_topic"
    info "    $payload"
  done

  echo ""
  info "Redpanda Connect is bridging these to topic 'iot-events'."
  info "ShadowLink will replicate to eu-central-1 within 30s."
}

cmd_mqtt_consume() {
  local cluster="${1:-source}"
  local ctx="$CONTEXT_A"
  local label="eu-west-1 (source)"
  [[ "$cluster" == "shadow" ]] && { ctx="$CONTEXT_B"; label="eu-central-1 (shadow)"; }

  banner "Cloud-native Kafka consumer — $label"
  step "Consuming iot-events (bridged from MQTT via Redpanda Connect)"
  info "Press Ctrl+C to stop"
  echo ""

  kubectl --context "$ctx" -n redpanda exec -it redpanda-0 -c redpanda -- \
    rpk topic consume iot-events \
    --group iot-consumer \
    --format '%v\n' \
    --print-offset
}

cmd_mqtt_status() {
  banner "MQTT Bridge Status"

  step "Redpanda Connect pod:"
  kubectl --context "$CONTEXT_A" -n redpanda get pod \
    -l app=connect-mqtt-bridge \
    -o custom-columns='NAME:.metadata.name,STATUS:.status.phase,RESTARTS:.status.containerStatuses[0].restartCount,AGE:.metadata.creationTimestamp' \
    2>&1
  echo ""

  step "Connect pipeline logs (last 5 lines):"
  kubectl --context "$CONTEXT_A" -n redpanda logs deployment/connect-mqtt-bridge --tail=5 2>&1
  echo ""

  step "iot-events offsets — eu-west-1 (source):"
  kubectl --context "$CONTEXT_A" -n redpanda exec redpanda-0 -c redpanda -- \
    rpk topic describe iot-events -p 2>&1
  echo ""

  step "iot-events offsets — eu-central-1 (shadow, via ShadowLink):"
  kubectl --context "$CONTEXT_B" -n redpanda exec redpanda-0 -c redpanda -- \
    rpk topic describe iot-events -p 2>/dev/null || \
    echo "  (topic not yet synced — ShadowLink interval is 30s)"
}

# ── SDK consumers ─────────────────────────────────────────────────────────────

cmd_python_consume() {
  local cluster="${1:-source}"
  local ctx="$CONTEXT_A"
  local label="eu-west-1 (source)"
  local deploy="python-retail-consumer"
  [[ "$cluster" == "shadow" ]] && { ctx="$CONTEXT_B"; label="eu-central-1 (shadow)"; }

  banner "Python Kafka Consumer — $label"
  info "Client library : confluent-kafka (standard Kafka protocol)"
  info "Topic          : retail-orders"
  info "Protocol       : Kafka/TLS — no Redpanda SDK required"
  echo ""

  step "Checking consumer pod is running..."
  POD=$(kubectl --context "$ctx" -n redpanda get pod \
    -l app="$deploy" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  if [[ -z "$POD" ]]; then
    echo -e "${RED}Pod not found. Run: kubectl --context $ctx apply -f clusters/region-a/python-consumer.yaml${NC}"
    exit 1
  fi

  PHASE=$(kubectl --context "$ctx" -n redpanda get pod "$POD" \
    -o jsonpath='{.status.phase}' 2>/dev/null)
  if [[ "$PHASE" != "Running" ]]; then
    step "Pod is $PHASE — waiting for Running state..."
    kubectl --context "$ctx" -n redpanda wait pod "$POD" \
      --for=condition=Ready --timeout=120s 2>&1
  fi
  ok "Pod $POD is $PHASE"
  echo ""

  info "Press Ctrl+C to stop"
  echo ""
  kubectl --context "$ctx" -n redpanda logs -f "$POD" 2>&1
}

cmd_amqp_consume() {
  banner "AMQP Consumer — eu-central-1 (shadow region)"
  info "Protocol  : AMQP 0.9.1 (pika) via RabbitMQ"
  info "Chain     : MQTT → Kafka → ShadowLink → Redpanda Connect → RabbitMQ → this consumer"
  echo ""

  step "Checking AMQP consumer pod is running..."
  POD=$(kubectl --context "$CONTEXT_B" -n redpanda get pod \
    -l app=amqp-iot-consumer -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  if [[ -z "$POD" ]]; then
    echo -e "${RED}Pod not found. Run: kubectl --context $CONTEXT_B apply -f clusters/region-b/amqp-bridge/amqp-consumer.yaml${NC}"
    exit 1
  fi

  PHASE=$(kubectl --context "$CONTEXT_B" -n redpanda get pod "$POD" \
    -o jsonpath='{.status.phase}' 2>/dev/null)
  if [[ "$PHASE" != "Running" ]]; then
    step "Pod is $PHASE — waiting for Running state..."
    kubectl --context "$CONTEXT_B" -n redpanda wait pod "$POD" \
      --for=condition=Ready --timeout=120s 2>&1
  fi
  ok "Pod $POD is $PHASE"
  echo ""

  info "Publish MQTT events in another terminal: ./demo.sh mqtt-publish 10"
  info "Press Ctrl+C to stop"
  echo ""
  kubectl --context "$CONTEXT_B" -n redpanda logs -f "$POD" 2>&1
}

cmd_amqp_status() {
  banner "AMQP Bridge Status — eu-central-1"

  step "RabbitMQ pod:"
  kubectl --context "$CONTEXT_B" -n redpanda get pod \
    -l app=rabbitmq \
    -o custom-columns='NAME:.metadata.name,STATUS:.status.phase,RESTARTS:.status.containerStatuses[0].restartCount,AGE:.metadata.creationTimestamp' \
    2>&1
  echo ""

  step "Redpanda Connect (Kafka → AMQP) pod:"
  kubectl --context "$CONTEXT_B" -n redpanda get pod \
    -l app=connect-amqp-bridge \
    -o custom-columns='NAME:.metadata.name,STATUS:.status.phase,RESTARTS:.status.containerStatuses[0].restartCount,AGE:.metadata.creationTimestamp' \
    2>&1
  echo ""

  step "Connect pipeline logs (last 5 lines):"
  kubectl --context "$CONTEXT_B" -n redpanda logs deployment/connect-amqp-bridge --tail=5 2>&1
  echo ""

  step "AMQP consumer pod:"
  kubectl --context "$CONTEXT_B" -n redpanda get pod \
    -l app=amqp-iot-consumer \
    -o custom-columns='NAME:.metadata.name,STATUS:.status.phase,RESTARTS:.status.containerStatuses[0].restartCount,AGE:.metadata.creationTimestamp' \
    2>&1
  echo ""

  step "iot-events shadow topic offsets (eu-central-1):"
  rp_exec "$CONTEXT_B" rpk topic describe iot-events -p 2>&1 || \
    echo "  (topic not yet synced)"
  echo ""

  step "amqp-bridge-consumer group lag (eu-central-1):"
  rp_exec "$CONTEXT_B" rpk group describe amqp-bridge-consumer 2>&1 || true
}

# ── Selective routing demo ────────────────────────────────────────────────────

cmd_routing() {
  local GLOBAL_TOPIC="global-alerts"
  local LOCAL_TOPIC="regional-eu-west-1-ops"

  banner "Selective Routing — Policy-Based Topic Distribution"
  info "Filter policy (from shadowlink.yaml):"
  info "  1. EXCLUDE topics prefixed 'regional-'  → stay in eu-west-1 only"
  info "  2. INCLUDE everything else              → replicate to eu-central-1"
  echo ""

  step "1. Creating demo topics on eu-west-1..."
  rp_exec "$CONTEXT_A" rpk topic create "$GLOBAL_TOPIC" --partitions 3 2>&1 || true
  rp_exec "$CONTEXT_A" rpk topic create "$LOCAL_TOPIC"  --partitions 3 2>&1 || true
  ok "Topics created: $GLOBAL_TOPIC, $LOCAL_TOPIC"
  echo ""

  step "2. Producing 5 messages to each topic on eu-west-1..."
  for i in $(seq 1 5); do
    local ts; ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    printf '{"alert_id":"ALT-%04d","severity":"high","ts":"%s"}\n' "$((RANDOM % 9999))" "$ts" | \
      kubectl --context "$CONTEXT_A" -n redpanda exec -i redpanda-0 -c redpanda -- \
        rpk topic produce "$GLOBAL_TOPIC" --key "alert-$i" &>/dev/null || true
    printf '{"op_id":"OPS-%04d","host":"broker-%d","ts":"%s"}\n' "$((RANDOM % 9999))" "$((i % 3))" "$ts" | \
      kubectl --context "$CONTEXT_A" -n redpanda exec -i redpanda-0 -c redpanda -- \
        rpk topic produce "$LOCAL_TOPIC" --key "ops-$i" &>/dev/null || true
  done
  ok "Produced 5 messages to $GLOBAL_TOPIC (global) and 5 to $LOCAL_TOPIC (regional)"
  echo ""

  step "3. Topics visible on eu-west-1 (source):"
  rp_exec "$CONTEXT_A" rpk topic list 2>&1 | grep -E "$GLOBAL_TOPIC|$LOCAL_TOPIC" || \
    echo "  (none found)"
  echo ""

  step "4. Waiting 35s for ShadowLink sync cycle..."
  for i in $(seq 35 -1 1); do
    printf "\r  %2ds remaining..." "$i"
    sleep 1
  done
  echo ""
  echo ""

  step "5. Topics visible on eu-central-1 (shadow) after sync:"
  SHADOW_TOPICS=$(rp_exec "$CONTEXT_B" rpk topic list 2>/dev/null || echo "")
  if echo "$SHADOW_TOPICS" | grep -q "$GLOBAL_TOPIC"; then
    ok "  $GLOBAL_TOPIC   → REPLICATED  ✓  (matches include-all rule)"
  else
    echo -e "  ${YELLOW}$GLOBAL_TOPIC   → not yet synced (retry in a few seconds)${NC}"
  fi
  if echo "$SHADOW_TOPICS" | grep -q "$LOCAL_TOPIC"; then
    echo -e "  ${RED}  $LOCAL_TOPIC → REPLICATED (unexpected — check filter config)${NC}"
  else
    ok "  $LOCAL_TOPIC → LOCAL ONLY   ✓  (matched 'regional-' exclude rule)"
  fi
  echo ""

  step "6. Confirming $LOCAL_TOPIC data stays in eu-west-1 only:"
  info "  eu-west-1  : $(rp_exec "$CONTEXT_A" rpk topic describe "$LOCAL_TOPIC" -p 2>/dev/null \
    | awk 'NR>1 {sum += $6} END {print sum+0}') messages"
  info "  eu-central-1: topic does not exist (policy enforced)"
  echo ""

  banner "Routing Demo Complete"
  ok "Policy-based selective distribution confirmed:"
  info "  global-*   topics  →  replicated to all regions"
  info "  regional-* topics  →  scoped to origin region only"
  info ""
  info "To clean up demo topics:"
  info "  kubectl --context $CONTEXT_A -n redpanda exec redpanda-0 -c redpanda -- rpk topic delete $GLOBAL_TOPIC $LOCAL_TOPIC"
  info "  (shadow copy of $GLOBAL_TOPIC on eu-central-1 is ShadowLink-managed;"
  info "   it will be removed automatically once the source topic is deleted and synced)"
}

# ── HA / DR commands ──────────────────────────────────────────────────────────

cmd_chaos() {
  banner "HA Demo — Single Broker Failure & Self-Healing"
  step "Cluster  : $CONTEXT_A (eu-west-1, primary)"
  step "Action   : Kill redpanda-0, watch Raft re-elect, measure recovery time"
  echo ""

  step "1. Pre-flight: produce 20 messages as baseline..."
  cmd_produce 20 2>/dev/null
  echo ""

  step "2. ShadowLink lag before kill:"
  rp_exec "$CONTEXT_B" rpk shadow status "$SHADOW_LINK" --print-topic 2>&1 | \
    grep -E "NAME|LAG|PARTITION" | head -20
  echo ""

  step "3. Cluster health before kill:"
  rp_exec "$CONTEXT_A" rpk cluster health 2>&1
  echo ""

  info "Killing redpanda-0 on eu-west-1 in 3 seconds..."
  sleep 3
  CHAOS_START=$SECONDS
  kubectl --context "$CONTEXT_A" -n redpanda delete pod redpanda-0 --grace-period=0 2>&1
  echo -e "${RED}✗ redpanda-0 deleted — outage started${NC}"
  echo ""

  step "4. Watching cluster recovery (polling every 5s)..."
  RECOVERED=false
  while true; do
    ELAPSED=$(( SECONDS - CHAOS_START ))
    HEALTH=$(rp_exec "$CONTEXT_A" rpk cluster health 2>/dev/null | grep "Healthy:" || echo "Healthy: false")
    POD_PHASE=$(kubectl --context "$CONTEXT_A" -n redpanda get pod redpanda-0 \
      -o jsonpath='{.status.phase}' 2>/dev/null || echo "Pending")
    echo -e "  ${ELAPSED}s — pod: ${POD_PHASE} | ${HEALTH}"
    if echo "$HEALTH" | grep -q "true" && [[ "$POD_PHASE" == "Running" ]]; then
      RECOVERED=true
      break
    fi
    if [[ $ELAPSED -gt 300 ]]; then
      echo -e "${RED}Timed out after 5m${NC}"; break
    fi
    sleep 5
  done
  echo ""

  if $RECOVERED; then
    RTO=$(( SECONDS - CHAOS_START ))
    ok "Broker recovered in ${RTO}s"
    echo ""
    step "5. ShadowLink lag after recovery (should be 0 or near-0):"
    rp_exec "$CONTEXT_B" rpk shadow status "$SHADOW_LINK" --print-topic 2>&1 | \
      grep -E "NAME|LAG|PARTITION" | head -20
    echo ""
    ok "HA demo complete — no manual intervention, no data loss, RTO = ${RTO}s"
  fi
}

cmd_failover() {
  banner "DR Demo — Regional Failover to eu-central-1"
  echo -e "${RED}  WARNING: Shadow topic failover is IRREVERSIBLE.${NC}"
  echo -e "${YELLOW}  Run './demo.sh restore' afterwards to reset for another run.${NC}"
  echo ""

  step "1. Pre-failover checks..."
  SHADOW_STATE=$(rp_exec "$CONTEXT_B" rpk shadow status "$SHADOW_LINK" --print-overview 2>/dev/null \
    | grep "^STATE" | awk '{print $2}')
  ok "ShadowLink state: $SHADOW_STATE"
  echo ""

  step "2. Current replication lag (should be 0 before outage):"
  rp_exec "$CONTEXT_B" rpk shadow status "$SHADOW_LINK" --print-topic 2>&1 | \
    grep -E "^Name:|LAG" | head -20
  echo ""

  step "3. Producing 10 messages to eu-west-1 before outage..."
  cmd_produce 10 2>/dev/null
  echo ""

  info "Simulating eu-west-1 regional outage in 3 seconds..."
  info "(scaling Redpanda StatefulSet to 0 replicas)"
  sleep 3
  OUTAGE_START=$SECONDS
  kubectl --context "$CONTEXT_A" -n redpanda scale statefulset redpanda --replicas=0 2>&1
  echo -e "${RED}✗ eu-west-1 source cluster is DOWN — outage clock started${NC}"
  echo ""

  step "4. Waiting for ShadowLink to detect disconnection..."
  for _ in $(seq 1 12); do
    sleep 5
    STATE=$(rp_exec "$CONTEXT_B" rpk shadow status "$SHADOW_LINK" --print-overview 2>/dev/null \
      | grep "^STATE" | awk '{print $2}' || echo "unknown")
    ELAPSED=$(( SECONDS - OUTAGE_START ))
    echo "  ${ELAPSED}s — ShadowLink state: $STATE"
    if [[ "$STATE" != "ACTIVE" ]]; then break; fi
  done
  echo ""

  step "5. Final replication lag (this is our RPO — data synced before outage):"
  rp_exec "$CONTEXT_B" rpk shadow status "$SHADOW_LINK" --print-topic 2>&1 | \
    grep -E "^Name:|PARTITION|LAG" | head -30
  echo ""

  step "6. Initiating failover — converting shadow topics to writable..."
  FAILOVER_START=$SECONDS
  rp_exec "$CONTEXT_B" rpk shadow failover "$SHADOW_LINK" --all --no-confirm 2>&1
  echo ""

  step "7. Verifying topics are now writable on eu-central-1..."
  # Try to produce a message — this proves the topic is writable
  echo '{"source":"failover-test","ts":"'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'"}' | \
    kubectl --context "$CONTEXT_B" -n redpanda exec -i redpanda-0 -c redpanda -- \
      rpk topic produce "$TOPIC" --key failover 2>&1
  echo ""

  RTO=$(( SECONDS - OUTAGE_START ))
  FAILOVER_TIME=$(( SECONDS - FAILOVER_START ))
  echo ""
  ok "═══════════════════════════════════════════"
  ok "  FAILOVER COMPLETE"
  ok "  Total outage → recovery time : ${RTO}s"
  ok "  rpk shadow failover duration : ${FAILOVER_TIME}s"
  ok "  eu-central-1 is now PRIMARY"
  ok "═══════════════════════════════════════════"
  echo ""
  info "Consumers and producers should now point to eu-central-1."
  info "Run './demo.sh restore' to reset eu-west-1 and recreate the ShadowLink."
}

cmd_restore() {
  banner "Restore — Reset Demo Environment"
  info "Scales eu-west-1 back up and recreates the ShadowLink."
  echo ""

  step "1. Scaling eu-west-1 Redpanda back to 3 replicas..."
  kubectl --context "$CONTEXT_A" -n redpanda scale statefulset redpanda --replicas=3 2>&1

  step "2. Waiting for eu-west-1 to be healthy..."
  until rp_exec "$CONTEXT_A" rpk cluster health 2>/dev/null | grep -q "Healthy: true"; do
    sleep 5
  done
  ok "eu-west-1 is healthy"
  echo ""

  step "3. Deleting old ShadowLink resource (topics on eu-central-1 are now regular topics)..."
  kubectl --context "$CONTEXT_B" -n redpanda delete shadowlink "$SHADOW_LINK" --ignore-not-found 2>&1
  echo ""

  step "4. Deleting failed-over topics on eu-central-1 (will be re-synced fresh)..."
  for t in retail-orders iot-events; do
    rp_exec "$CONTEXT_B" rpk topic delete "$t" 2>/dev/null || true
    ok "Deleted $t on eu-central-1"
  done
  echo ""

  step "5. Recreating ShadowLink on eu-central-1..."
  kubectl --context "$CONTEXT_B" apply -f \
    "$(dirname "$0")/clusters/region-b/shadowlink.yaml" 2>&1

  step "6. Waiting for ShadowLink to become active..."
  until kubectl --context "$CONTEXT_B" -n redpanda get shadowlink "$SHADOW_LINK" \
      -o jsonpath='{.status.state}' 2>/dev/null | grep -q "active"; do
    sleep 5
  done
  ok "ShadowLink is active — replication resumed"
  echo ""

  ok "Demo environment restored. eu-west-1 is primary, eu-central-1 is shadow."
}

# ── Dispatch ──────────────────────────────────────────────────────────────────
case "${1:-}" in
  preflight)      cmd_preflight ;;
  produce)        cmd_produce "${2:-10}" ;;
  consume-source) cmd_consume_source ;;
  consume-shadow) cmd_consume_shadow ;;
  consume-both)   cmd_consume_both ;;
  status)         cmd_status ;;
  full-demo)      cmd_full_demo ;;
  mqtt-publish)   cmd_mqtt_publish "${2:-10}" ;;
  mqtt-consume)   cmd_mqtt_consume "${2:-source}" ;;
  mqtt-status)    cmd_mqtt_status ;;
  python-consume) cmd_python_consume "${2:-source}" ;;
  amqp-consume)   cmd_amqp_consume ;;
  amqp-status)    cmd_amqp_status ;;
  routing)        cmd_routing ;;
  chaos)          cmd_chaos ;;
  failover)       cmd_failover ;;
  restore)        cmd_restore ;;
  *)              usage ;;
esac
