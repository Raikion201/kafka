#!/usr/bin/env bash
# demo-strimzi-rest-proxy.sh
# --------------------------------------------------------------------
# End-to-end: build the Kafka REST proxy from this repo, build a
# Strimzi operator image from the sibling ../strimzi-kafka-operator
# (branch rest-api), load both into a local Kubernetes cluster, deploy
# a Kafka CR with a type: http listener, curl the Strimzi-created
# LoadBalancer from outside the cluster.
#
# Expected layout:
#   D:\apache kafka\kafka                 (this repo, branch rest-proxy)
#   D:\apache kafka\strimzi-kafka-operator (sibling repo, branch rest-api)
#
# Requirements:
#   - Docker running
#   - kubectl pointing at a cluster (Docker Desktop k8s, kind, or minikube)
#   - mvn     in PATH
#   - gradle  (via ./gradlew in this repo)
#   - sha512sum
#
# On Docker Desktop's built-in Kubernetes, images built locally are
# automatically available to the cluster — no `kind load` or
# `minikube image load` step needed. This script detects the context
# and skips the load step when possible.
#
# Heavy steps can be skipped on re-runs:
#   --skip-kafka-build     Skip ./gradlew releaseTarGz (uses cached tarball)
#   --skip-image-build     Skip both Docker image builds (uses cached images)
#   --skip-operator        Skip Strimzi operator install (assumes already deployed)
#   --reset                Delete previous Kafka CR + operator before deploying
#
# Usage:
#   bash demo-strimzi-rest-proxy.sh
#   bash demo-strimzi-rest-proxy.sh --skip-kafka-build
#   bash demo-strimzi-rest-proxy.sh --reset
# --------------------------------------------------------------------

set -euo pipefail

# -- Locations ---------------------------------------------------------
KAFKA_ROOT="$(cd "$(dirname "$0")" && pwd)"
STRIMZI_ROOT="$(cd "$KAFKA_ROOT/../strimzi-kafka-operator" && pwd)"

NAMESPACE="kafka"
CLUSTER_NAME="my-cluster"
KAFKA_IMAGE="kafka-rest-proxy:demo"
OPERATOR_IMAGE="strimzi-cluster-operator-rest:demo"
REST_LISTENER_PORT=8080
REST_USER="alice"
REST_PASS="s3cret"
TOPIC="demo-topic"

SKIP_KAFKA_BUILD=0
SKIP_IMAGE_BUILD=0
SKIP_OPERATOR=0
RESET=0

for arg in "$@"; do
  case "$arg" in
    --skip-kafka-build) SKIP_KAFKA_BUILD=1 ;;
    --skip-image-build) SKIP_IMAGE_BUILD=1 ;;
    --skip-operator)    SKIP_OPERATOR=1 ;;
    --reset)            RESET=1 ;;
    -h|--help)
      sed -n '2,35p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown argument: $arg"
      exit 1
      ;;
  esac
done

say() {
  echo ""
  echo "════════════════════════════════════════════════════════════════"
  echo "  $*"
  echo "════════════════════════════════════════════════════════════════"
}

require() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing required tool: $1"; exit 1; }
}

# -- 0. Prereq check ---------------------------------------------------

say "0. checking prerequisites"
require docker
require kubectl
require mvn
require sha512sum
[ -d "$STRIMZI_ROOT" ] || { echo "cannot find sibling repo at $STRIMZI_ROOT"; exit 1; }

kubectl cluster-info >/dev/null 2>&1 \
  || { echo "kubectl is not pointing at a running cluster; start Docker Desktop k8s / kind / minikube first"; exit 1; }

CLUSTER_CTX=$(kubectl config current-context)
echo "current kubectl context: $CLUSTER_CTX"

# Detect cluster type for image-load behaviour
case "$CLUSTER_CTX" in
  docker-desktop|docker-for-desktop) CLUSTER_KIND="docker-desktop" ;;
  kind-*)                            CLUSTER_KIND="kind" ;;
  minikube)                          CLUSTER_KIND="minikube" ;;
  *)                                 CLUSTER_KIND="unknown" ;;
esac
echo "detected cluster kind: $CLUSTER_KIND"

# -- 1. Kafka tarball --------------------------------------------------

TARBALL_NAME="kafka_2.13-4.4.0-rest-proxy.tgz"
TARBALL_PATH="$KAFKA_ROOT/core/build/distributions/$TARBALL_NAME"

if [ "$SKIP_KAFKA_BUILD" -eq 1 ] && [ -f "$TARBALL_PATH" ]; then
  say "1. Kafka tarball: reusing $TARBALL_PATH (--skip-kafka-build)"
else
  say "1. building Kafka tarball with REST proxy"
  cd "$KAFKA_ROOT"
  # Writes to core/build/distributions/kafka_2.13-<version>-SNAPSHOT.tgz
  ./gradlew releaseTarGz -PscalaVersion=2.13 --console=plain
  ORIG_TARBALL=$(ls -1 core/build/distributions/kafka_2.13-*-SNAPSHOT.tgz | head -1)
  [ -f "$ORIG_TARBALL" ] || { echo "gradle did not produce a tarball"; exit 1; }
  cp "$ORIG_TARBALL" "$TARBALL_PATH"
  echo "tarball: $TARBALL_PATH"
fi

SHA512=$(sha512sum "$TARBALL_PATH" | awk '{print $1}')
echo "sha-512: $SHA512"

# -- 2. Kafka container image -----------------------------------------

if [ "$SKIP_IMAGE_BUILD" -eq 1 ] && docker image inspect "$KAFKA_IMAGE" >/dev/null 2>&1; then
  say "2. Kafka image: reusing $KAFKA_IMAGE (--skip-image-build)"
else
  say "2. building custom Kafka image: $KAFKA_IMAGE"
  BUILD_CTX=$(mktemp -d)
  trap 'rm -rf "$BUILD_CTX"' EXIT

  cp "$TARBALL_PATH" "$BUILD_CTX/kafka.tgz"

  cat > "$BUILD_CTX/Dockerfile" <<'DOCKERFILE'
# Overlay the REST-proxy-enabled Kafka libs on top of a stock Strimzi
# Kafka image so we inherit Strimzi's directory layout / user / entrypoint
# but serve our Kafka binaries.
FROM quay.io/strimzi/kafka:latest-kafka-4.2.0

USER root

# Replace /opt/kafka contents with the custom build.
RUN rm -rf /opt/kafka
COPY kafka.tgz /tmp/kafka.tgz
RUN mkdir -p /opt/kafka \
 && tar -xzf /tmp/kafka.tgz -C /opt/kafka --strip-components=1 \
 && rm /tmp/kafka.tgz \
 && chown -R 1001:0 /opt/kafka

USER 1001
DOCKERFILE

  docker build -t "$KAFKA_IMAGE" "$BUILD_CTX"
fi

# -- 3. Strimzi operator image ----------------------------------------

if [ "$SKIP_IMAGE_BUILD" -eq 1 ] && docker image inspect "$OPERATOR_IMAGE" >/dev/null 2>&1; then
  say "3. Strimzi operator image: reusing $OPERATOR_IMAGE (--skip-image-build)"
else
  say "3. building Strimzi operator from $STRIMZI_ROOT (branch rest-api)"
  cd "$STRIMZI_ROOT"

  # Patch kafka-versions.yaml with the real SHA-512 of our tarball so the
  # operator does not refuse to recognise the 4.4.0-rest-proxy entry.
  # awk-based: track whether we're inside the rest-proxy block and rewrite
  # only that block's checksum line.
  echo "patching kafka-versions.yaml checksum"
  awk -v sha="$SHA512" '
    /^- version: 4\.4\.0-rest-proxy$/ { in_block = 1 }
    in_block && /^  checksum: / {
      print "  checksum: " sha
      in_block = 0
      next
    }
    { print }
  ' kafka-versions.yaml > kafka-versions.yaml.new && mv kafka-versions.yaml.new kafka-versions.yaml

  # Build cluster-operator jar. Skip the exec plugin that chokes on Windows
  # (CRD generator); we already regenerated those files at commit time.
  mvn -pl cluster-operator -am package -DskipTests -Dexec.skip=true -q

  # Build the Docker image using Strimzi's existing Dockerfile.
  make -C docker-images/operator build DOCKER_TAG=demo DOCKER_ORG=strimzi-cluster-operator-rest
  docker tag "strimzi-cluster-operator-rest/cluster-operator:demo" "$OPERATOR_IMAGE"
fi

# -- 4. Load images into the cluster -----------------------------------

say "4. making images available to the cluster"
case "$CLUSTER_KIND" in
  docker-desktop)
    echo "Docker Desktop shares its image store with its Kubernetes node — nothing to do."
    ;;
  kind)
    KIND_NAME=${CLUSTER_CTX#kind-}
    kind load docker-image "$KAFKA_IMAGE"    --name "$KIND_NAME"
    kind load docker-image "$OPERATOR_IMAGE" --name "$KIND_NAME"
    ;;
  minikube)
    minikube image load "$KAFKA_IMAGE"
    minikube image load "$OPERATOR_IMAGE"
    ;;
  *)
    echo "unknown cluster kind — make sure the cluster can pull images named:"
    echo "  $KAFKA_IMAGE"
    echo "  $OPERATOR_IMAGE"
    echo "(push them to a registry the cluster can reach, or use Docker Desktop / kind / minikube)"
    ;;
esac

# -- 5. Reset (optional) ----------------------------------------------

if [ "$RESET" -eq 1 ]; then
  say "5. reset: deleting any previous Kafka CR + operator"
  kubectl delete kafka "$CLUSTER_NAME" -n "$NAMESPACE" --ignore-not-found=true
  kubectl delete -f "$STRIMZI_ROOT/install/cluster-operator/" -n "$NAMESPACE" --ignore-not-found=true
  kubectl delete namespace "$NAMESPACE" --ignore-not-found=true
fi

# -- 6. Install Strimzi operator --------------------------------------

if [ "$SKIP_OPERATOR" -eq 1 ]; then
  say "6. operator install: skipped (--skip-operator)"
else
  say "6. installing Strimzi operator into namespace '$NAMESPACE'"
  kubectl get ns "$NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$NAMESPACE"

  # Rewrite the 'myproject' placeholder that Strimzi's install YAMLs use.
  TMP_INSTALL=$(mktemp -d)
  trap 'rm -rf "$TMP_INSTALL"' EXIT
  cp -r "$STRIMZI_ROOT/install/cluster-operator/"*.yaml "$TMP_INSTALL"
  # Substitute placeholder namespace ('myproject') AND point Deployment at our operator image.
  for f in "$TMP_INSTALL"/*.yaml; do
    sed -i "s/namespace: myproject/namespace: $NAMESPACE/g" "$f"
  done
  # Patch the Deployment image. Strimzi's default is quay.io/...
  sed -i "s|image: quay\\.io/strimzi/operator.*|image: $OPERATOR_IMAGE|" "$TMP_INSTALL"/060-Deployment-strimzi-cluster-operator.yaml || true

  kubectl apply -f "$TMP_INSTALL" -n "$NAMESPACE"
  echo "waiting for operator pod..."
  kubectl wait --for=condition=Available --timeout=5m deployment/strimzi-cluster-operator -n "$NAMESPACE"
fi

# -- 7. Apply the Kafka CR --------------------------------------------

say "7. applying Kafka CR with type: http listener"
# Generate a tweaked copy of the example that pins our custom Kafka image.
KAFKA_CR=$(mktemp)
cat > "$KAFKA_CR" <<YAML
apiVersion: kafka.strimzi.io/v1
kind: KafkaNodePool
metadata:
  name: controller
  namespace: $NAMESPACE
  labels:
    strimzi.io/cluster: $CLUSTER_NAME
spec:
  replicas: 1
  roles: [controller]
  storage:
    type: jbod
    volumes:
      - id: 0
        type: persistent-claim
        size: 10Gi
        kraftMetadata: shared
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaNodePool
metadata:
  name: broker
  namespace: $NAMESPACE
  labels:
    strimzi.io/cluster: $CLUSTER_NAME
spec:
  replicas: 1
  roles: [broker]
  storage:
    type: jbod
    volumes:
      - id: 0
        type: persistent-claim
        size: 10Gi
---
apiVersion: kafka.strimzi.io/v1
kind: Kafka
metadata:
  name: $CLUSTER_NAME
  namespace: $NAMESPACE
spec:
  kafka:
    version: 4.4.0-rest-proxy
    metadataVersion: 4.2-IV1
    image: $KAFKA_IMAGE
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
      - name: rest
        port: $REST_LISTENER_PORT
        type: http
        tls: false
    config:
      http.rest.basic.credentials: $REST_USER:$REST_PASS
      http.rest.executor.threads: 8
      offsets.topic.replication.factor: 1
      transaction.state.log.replication.factor: 1
      transaction.state.log.min.isr: 1
      default.replication.factor: 1
      min.insync.replicas: 1
  entityOperator:
    topicOperator: {}
    userOperator: {}
YAML

kubectl apply -f "$KAFKA_CR"

# Topic pre-creation — avoids the 404 on first produce if the broker has
# auto.create.topics.enable=false (Strimzi default).
cat <<YAML | kubectl apply -f -
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: $TOPIC
  namespace: $NAMESPACE
  labels:
    strimzi.io/cluster: $CLUSTER_NAME
spec:
  partitions: 3
  replicas: 1
YAML

say "8. waiting for Kafka cluster to become Ready (can take 2-3 minutes)"
kubectl wait kafka/"$CLUSTER_NAME" --for=condition=Ready --timeout=10m -n "$NAMESPACE"

# -- 9. Resolve the LoadBalancer address ------------------------------

SERVICE_NAME="$CLUSTER_NAME-kafka-rest-bootstrap"
say "9. waiting for LoadBalancer Service '$SERVICE_NAME' to get an external address"

LB_HOST=""
for i in {1..60}; do
  LB_HOST=$(kubectl get svc "$SERVICE_NAME" -n "$NAMESPACE" \
      -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)
  if [ -z "$LB_HOST" ]; then
    LB_HOST=$(kubectl get svc "$SERVICE_NAME" -n "$NAMESPACE" \
        -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true)
  fi
  if [ -n "$LB_HOST" ]; then break; fi
  sleep 5
  echo "  still waiting ($i/60)..."
done

if [ -z "$LB_HOST" ]; then
  echo ""
  echo "LoadBalancer Service never acquired an external address."
  echo "Docker Desktop should assign 'localhost' automatically; other clusters"
  echo "need a LoadBalancer provider (MetalLB on kind, 'minikube tunnel' etc.)"
  echo ""
  echo "Fallback: port-forward and curl localhost:"
  echo "  kubectl port-forward -n $NAMESPACE svc/$SERVICE_NAME $REST_LISTENER_PORT:$REST_LISTENER_PORT"
  exit 1
fi

echo "LoadBalancer endpoint: http://$LB_HOST:$REST_LISTENER_PORT"

# -- 10. The actual demo curl -----------------------------------------

say "10. curl -u $REST_USER:$REST_PASS ... http://$LB_HOST:$REST_LISTENER_PORT/v1/topics/$TOPIC"
RESPONSE=$(curl -sS -u "$REST_USER:$REST_PASS" \
    -H "Content-Type: application/json" \
    -d '{"key":"from-script","value":"hello-from-strimzi"}' \
    -w "\nHTTP %{http_code}" \
    "http://$LB_HOST:$REST_LISTENER_PORT/v1/topics/$TOPIC")

echo "$RESPONSE"

echo ""
if echo "$RESPONSE" | grep -q "HTTP 200" && echo "$RESPONSE" | grep -q '"partition"'; then
  echo "════════════════════════════════════════════════════════════════"
  echo "  SUCCESS — Strimzi + embedded REST proxy end-to-end works."
  echo "════════════════════════════════════════════════════════════════"
else
  echo "REST proxy returned an unexpected response; check broker logs:"
  echo "  kubectl logs -n $NAMESPACE -l strimzi.io/cluster=$CLUSTER_NAME,strimzi.io/kind=Kafka"
  exit 1
fi
