#!/usr/bin/env bash
set -euo pipefail

OPERATOR_VERSION="v26.1.3"
CONTEXT_A="rp-demo-eu-west-1"
CONTEXT_B="rp-demo-eu-central-1"

# ---------------------------------------------------------------------------
# 1. Provision EKS clusters
# ---------------------------------------------------------------------------
echo "==> Creating EKS cluster in eu-west-1..."
eksctl create cluster -f eks/cluster-eu-west-1.yaml

echo "==> Creating EKS cluster in eu-central-1..."
eksctl create cluster -f eks/cluster-eu-central-1.yaml

# Rename contexts to predictable names
kubectl config rename-context \
  "$(kubectl config get-contexts -o name | grep eu-west-1)" \
  "$CONTEXT_A"

kubectl config rename-context \
  "$(kubectl config get-contexts -o name | grep eu-central-1)" \
  "$CONTEXT_B"

# ---------------------------------------------------------------------------
# 2. Install cert-manager (required by Redpanda Operator)
# ---------------------------------------------------------------------------
helm repo add jetstack https://charts.jetstack.io
helm repo update

for CTX in "$CONTEXT_A" "$CONTEXT_B"; do
  echo "==> Installing cert-manager on $CTX..."
  helm upgrade --install cert-manager jetstack/cert-manager \
    --kube-context "$CTX" \
    --namespace cert-manager \
    --create-namespace \
    --set crds.enabled=true \
    --wait
done

# ---------------------------------------------------------------------------
# 3. Install LVM CSI driver for local NVMe storage (m7gd.large: /dev/nvme1n1)
# ---------------------------------------------------------------------------
helm repo add metal-stack https://helm.metal-stack.io
helm repo update

for CTX in "$CONTEXT_A" "$CONTEXT_B"; do
  echo "==> Installing LVM CSI driver on $CTX..."
  helm upgrade --install csi-driver-lvm metal-stack/csi-driver-lvm \
    --kube-context "$CTX" \
    --version 0.6.0 \
    --namespace csi-driver-lvm \
    --create-namespace \
    --set lvm.devicePattern='/dev/nvme[1-9]n[0-9]' \
    --wait

  kubectl --context "$CTX" apply -f - <<'EOF'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: csi-driver-lvm-striped-xfs
provisioner: lvm.csi.metal-stack.io
parameters:
  type: striped
  csi.storage.k8s.io/fstype: xfs
  mkfsParams: "-i nrext64=0"
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
EOF
done

# ---------------------------------------------------------------------------
# 4. Install kube-prometheus-stack (Prometheus + Grafana + Alertmanager)
# ---------------------------------------------------------------------------
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

for CTX in "$CONTEXT_A" "$CONTEXT_B"; do
  echo "==> Installing kube-prometheus-stack on $CTX..."
  helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
    --kube-context "$CTX" \
    --namespace monitoring \
    --create-namespace \
    --set grafana.service.type=LoadBalancer \
    --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false \
    --wait
done

# ---------------------------------------------------------------------------
# 5. Install Redpanda Operator on both clusters
# ---------------------------------------------------------------------------
helm repo add redpanda https://charts.redpanda.com
helm repo update

for CTX in "$CONTEXT_A" "$CONTEXT_B"; do
  echo "==> Installing Redpanda Operator on $CTX..."
  helm upgrade --install redpanda-controller redpanda/operator \
    --kube-context "$CTX" \
    --namespace redpanda \
    --create-namespace \
    --version "$OPERATOR_VERSION" \
    --set crds.enabled=true \
    --wait
done

# ---------------------------------------------------------------------------
# 6. Deploy 3-node Redpanda clusters + Console
# ---------------------------------------------------------------------------
echo "==> Deploying Redpanda cluster on $CONTEXT_A (eu-west-1)..."
kubectl --context "$CONTEXT_A" apply -f clusters/region-a/redpanda.yaml
kubectl --context "$CONTEXT_A" apply -f clusters/region-a/console.yaml

echo "==> Deploying Redpanda cluster on $CONTEXT_B (eu-central-1)..."
kubectl --context "$CONTEXT_B" apply -f clusters/region-b/redpanda.yaml
kubectl --context "$CONTEXT_B" apply -f clusters/region-b/console.yaml

# ---------------------------------------------------------------------------
# 7. Wait for clusters to be ready
# ---------------------------------------------------------------------------
for CTX in "$CONTEXT_A" "$CONTEXT_B"; do
  echo "==> Waiting for Redpanda cluster to be ready on $CTX (up to 10m)..."
  SECONDS=0
  until kubectl --context "$CTX" -n redpanda get redpanda redpanda \
      -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null | grep -q "True"; do
    if (( SECONDS > 600 )); then
      echo "ERROR: timed out waiting for Redpanda on $CTX"
      exit 1
    fi
    sleep 10
  done
  echo "  $CTX: ready"
done

# ---------------------------------------------------------------------------
# 8. Enable shadow linking on both clusters
# ---------------------------------------------------------------------------
echo "==> Enabling shadow_linking feature on both clusters..."
kubectl --context "$CONTEXT_A" -n redpanda exec redpanda-0 -- \
  rpk cluster config set enable_shadow_linking true
kubectl --context "$CONTEXT_B" -n redpanda exec redpanda-0 -- \
  rpk cluster config set enable_shadow_linking true

# ---------------------------------------------------------------------------
# 9. Copy eu-west-1 external CA cert to eu-central-1, then deploy ShadowLink
# ---------------------------------------------------------------------------
echo "==> Copying eu-west-1 external CA cert to eu-central-1..."
kubectl --context "$CONTEXT_A" -n redpanda get secret redpanda-external-root-certificate \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > /tmp/eu-west-1-ca.crt

kubectl --context "$CONTEXT_B" -n redpanda create secret generic eu-west-1-ca-cert \
  --from-file=ca.crt=/tmp/eu-west-1-ca.crt \
  --dry-run=client -o yaml | kubectl --context "$CONTEXT_B" apply -f -

rm /tmp/eu-west-1-ca.crt

echo "==> Deploying ShadowLink on $CONTEXT_B (eu-central-1)..."
# Replace broker addresses with live LB hostnames from eu-west-1
BROKER_0=$(kubectl --context "$CONTEXT_A" -n redpanda get svc lb-redpanda-0 \
  -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')
BROKER_1=$(kubectl --context "$CONTEXT_A" -n redpanda get svc lb-redpanda-1 \
  -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')
BROKER_2=$(kubectl --context "$CONTEXT_A" -n redpanda get svc lb-redpanda-2 \
  -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')

cat clusters/region-b/shadowlink.yaml \
  | sed "s|ab14f1341b5a04455951a4c439736dab-638340892.eu-west-1.elb.amazonaws.com|$BROKER_0|g" \
  | sed "s|ac89516824be94ebd9a1cd2b16ed6b92-1451718857.eu-west-1.elb.amazonaws.com|$BROKER_1|g" \
  | sed "s|aa2598d94380b48f6b6c4619e7a40926-1282810100.eu-west-1.elb.amazonaws.com|$BROKER_2|g" \
  | kubectl --context "$CONTEXT_B" apply -f -

echo "==> Waiting for ShadowLink to become active (up to 3m)..."
SECONDS=0
until kubectl --context "$CONTEXT_B" -n redpanda get shadowlink eu-west-1-shadow \
    -o jsonpath='{.status.state}' 2>/dev/null | grep -q "active"; do
  if (( SECONDS > 180 )); then
    echo "WARNING: ShadowLink not yet active - check 'kubectl --context $CONTEXT_B -n redpanda get shadowlink eu-west-1-shadow'"
    break
  fi
  sleep 5
done
echo "  ShadowLink active - eu-west-1 topics will sync to eu-central-1 every 30s"

# ---------------------------------------------------------------------------
# 10. Deploy MQTT bridge (eu-west-1) and AMQP bridge + consumers (eu-central-1)
# ---------------------------------------------------------------------------
echo "==> Deploying MQTT bridge on $CONTEXT_A (eu-west-1)..."
kubectl --context "$CONTEXT_A" apply -f clusters/region-a/mqtt-bridge/mosquitto.yaml
kubectl --context "$CONTEXT_A" apply -f clusters/region-a/mqtt-bridge/connect.yaml

echo "==> Deploying Python Kafka consumer on $CONTEXT_A (eu-west-1)..."
kubectl --context "$CONTEXT_A" apply -f clusters/region-a/python-consumer.yaml

echo "==> Deploying AMQP bridge and consumers on $CONTEXT_B (eu-central-1)..."
kubectl --context "$CONTEXT_B" apply -f clusters/region-b/amqp-bridge/rabbitmq.yaml
kubectl --context "$CONTEXT_B" apply -f clusters/region-b/amqp-bridge/connect.yaml
kubectl --context "$CONTEXT_B" apply -f clusters/region-b/amqp-bridge/amqp-consumer.yaml

echo "==> Creating topics on eu-west-1..."
kubectl --context "$CONTEXT_A" -n redpanda exec redpanda-0 -c redpanda -- \
  rpk topic create retail-orders iot-events --partitions 3 2>/dev/null || true

echo ""
echo "==> Done. Both clusters are up."
echo ""
echo "Contexts:"
echo "  eu-west-1   : kubectl --context $CONTEXT_A ..."
echo "  eu-central-1: kubectl --context $CONTEXT_B ..."
echo ""
echo "Smoke test:"
echo "  kubectl --context $CONTEXT_A -n redpanda exec -it redpanda-0 -- rpk cluster info"
echo "  kubectl --context $CONTEXT_B -n redpanda exec -it redpanda-0 -- rpk cluster info"
echo ""
echo "Console URLs (once LoadBalancer IPs are assigned):"
echo "  kubectl --context $CONTEXT_A -n redpanda get svc console"
echo "  kubectl --context $CONTEXT_B -n redpanda get svc console"
echo ""
echo "Grafana URLs (admin / prom-operator):"
echo "  kubectl --context $CONTEXT_A -n monitoring get svc kube-prometheus-stack-grafana"
echo "  kubectl --context $CONTEXT_B -n monitoring get svc kube-prometheus-stack-grafana"
