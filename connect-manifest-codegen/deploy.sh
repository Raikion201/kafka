#!/bin/bash
# Fast local deploy: build JAR + Docker image into minikube, no Kaniko (~30s).
# First run switches the KafkaConnect CR from spec.build (Kaniko) to spec.image (local).
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NS="${NS:-kafka}"
CLUSTER="${CLUSTER:-my-connect-cluster}"
IMAGE_NAME="my-connect-local"

cd "$SCRIPT_DIR"

echo "==> Building JAR..."
cd ..
./gradlew :connect-manifest-codegen:jar -q
cd "$SCRIPT_DIR"

TAG="${IMAGE_NAME}:$(date +%Y%m%d-%H%M%S)"
echo "==> Building Docker image into minikube: $TAG"
eval "$(minikube docker-env)"
docker build -t "$TAG" -f Dockerfile.connect .

echo "==> Patching KafkaConnect CR (image=$TAG)..."
CURRENT_BUILD=$(kubectl get kafkaconnect "$CLUSTER" -n "$NS" -o jsonpath='{.spec.build}' 2>/dev/null || echo "")
if [ -n "$CURRENT_BUILD" ] && [ "$CURRENT_BUILD" != "null" ]; then
    # First time: remove Kaniko build spec, add image
    kubectl patch kafkaconnect "$CLUSTER" -n "$NS" --type=json -p="[
      {\"op\":\"remove\",\"path\":\"/spec/build\"},
      {\"op\":\"add\",\"path\":\"/spec/image\",\"value\":\"$TAG\"}
    ]"
else
    # Subsequent runs: just update the image
    kubectl patch kafkaconnect "$CLUSTER" -n "$NS" --type=merge -p="{\"spec\":{\"image\":\"$TAG\"}}"
fi

echo "==> Waiting for Connect pod ready..."
elapsed=0
until kubectl get kafkaconnect "$CLUSTER" -n "$NS" \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null \
    | grep -q True; do
    sleep 5; elapsed=$((elapsed+5)); echo -n "."
    if [ $elapsed -ge 120 ]; then echo " TIMEOUT"; exit 1; fi
done
echo " done (${elapsed}s)"
