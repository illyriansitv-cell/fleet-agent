#!/usr/bin/env bash
set -euo pipefail

REGISTRY="13.140.141.3:5000"
IMAGE="$REGISTRY/fleet-agent:latest"
SSH_PASS="coza88nostra"
BACKEND_URL="${FLEETDEPLOY_BACKEND_URL:-https://web-production-946ce.up.railway.app}"
AGENT_SECRET="${AGENT_SECRET:-1ccdd0ce8afe0a88b986342ea666441771fffd42d9b615e455fce78f311b55da}"
CLAUDE_API_KEY="${CLAUDE_API_KEY:-}"

declare -A NODES
NODES[de]="13.140.141.3"
NODES[us]="94.72.119.135"
NODES[au]="46.250.240.132"
NODES[jp]="5.104.84.73"

REDIS_URL="redis://:VIp0nlN1-Wiffa2MgRpmyqLpaEJzG8Jj@dokploy-redis:6379"

echo "==> Building fleet-agent image on Germany registry node..."
sshpass -p "$SSH_PASS" ssh -o StrictHostKeyChecking=no root@13.140.141.3 "
  mkdir -p /tmp/fleet-agent-build/agent
"
# Copy source files to Germany builder
sshpass -p "$SSH_PASS" scp -o StrictHostKeyChecking=no \
  Dockerfile requirements.txt \
  root@13.140.141.3:/tmp/fleet-agent-build/

sshpass -p "$SSH_PASS" scp -o StrictHostKeyChecking=no \
  agent/__init__.py agent/main.py agent/metrics.py agent/llm.py agent/bus.py agent/reporter.py agent/executor.py \
  root@13.140.141.3:/tmp/fleet-agent-build/agent/

sshpass -p "$SSH_PASS" ssh -o StrictHostKeyChecking=no root@13.140.141.3 "
  cd /tmp/fleet-agent-build
  docker build -t $IMAGE .
  docker push $IMAGE
  rm -rf /tmp/fleet-agent-build
"
echo "==> Image built and pushed to $IMAGE"

for LABEL in de us au jp; do
  IP="${NODES[$LABEL]}"
  echo ""
  echo "==> Deploying to $LABEL ($IP)..."

  sshpass -p "$SSH_PASS" ssh -o StrictHostKeyChecking=no root@"$IP" "
    docker pull $IMAGE

    # Stop old agent if running
    docker stop fleet-agent 2>/dev/null || true
    docker rm fleet-agent 2>/dev/null || true

    # Start fleet-agent
    docker run -d \
      --name fleet-agent \
      --restart unless-stopped \
      --network dokploy-network \
      -v /var/run/docker.sock:/var/run/docker.sock \
      -v /etc/traefik:/etc/traefik \
      -e NODE_LABEL=$LABEL \
      -e REDIS_URL='$REDIS_URL' \
      -e FLEETDEPLOY_BACKEND_URL='$BACKEND_URL' \
      -e AGENT_SECRET='$AGENT_SECRET' \
      -e CLAUDE_API_KEY='$CLAUDE_API_KEY' \
      -e SCALE_CPU_THRESHOLD=80 \
      -e SCALE_CONSECUTIVE=3 \
      -e LOOP_INTERVAL=30 \
      $IMAGE

    # Start ollama if not already running
    if ! docker ps --format '{{.Names}}' | grep -q '^ollama$'; then
      docker run -d \
        --name ollama \
        --restart unless-stopped \
        --network dokploy-network \
        -v ollama_data:/root/.ollama \
        ollama/ollama:latest
      echo 'Ollama started, pulling qwen2.5:0.5b (this may take a few minutes)...'
      sleep 5
      docker exec ollama ollama pull qwen2.5:0.5b &
    else
      echo 'Ollama already running'
    fi

    echo 'Done on $LABEL'
  "
done

echo ""
echo "==> All nodes deployed. Verify with:"
for LABEL in de us au jp; do
  IP="${NODES[$LABEL]}"
  echo "  sshpass -p '$SSH_PASS' ssh root@$IP 'docker logs --tail 20 fleet-agent'"
done
