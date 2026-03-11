#!/bin/bash
# Helper to copy code to the cluster
#!/bin/bash
# scripts/deploy.sh
# Copies project code to the master node and distributes to workers

set -e  # Exit on any error

# ── Configuration ────────────────────────────────────────────────
MASTER_IP="your.master.ip.here"         # Replace with your master floating IP
MASTER_USER="ubuntu"                     # Replace if different
SSH_KEY="~/.ssh/your_key.pem"           # Replace with your key path
REMOTE_DIR="/home/ubuntu/project"
WORKERS=("10.0.0.2" "10.0.0.3" "10.0.0.4")  # Replace with internal worker IPs

# ── Colours for output ───────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()  { echo -e "${GREEN}[DEPLOY]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $1"; }
fail() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

# ── Step 1: Validate SSH key ─────────────────────────────────────
log "Checking SSH key..."
[ -f "${SSH_KEY/#\~/$HOME}" ] || fail "SSH key not found at $SSH_KEY"

SSH="ssh -i $SSH_KEY -o StrictHostKeyChecking=no"
SCP="scp -i $SSH_KEY -o StrictHostKeyChecking=no"

# ── Step 2: Create remote directory ─────────────────────────────
log "Creating remote project directory on master..."
$SSH $MASTER_USER@$MASTER_IP "mkdir -p $REMOTE_DIR/src $REMOTE_DIR/scripts $REMOTE_DIR/logs $REMOTE_DIR/docs"

# ── Step 3: Copy source code to master ──────────────────────────
log "Copying source code to master node..."
$SCP src/*.py          $MASTER_USER@$MASTER_IP:$REMOTE_DIR/src/
$SCP scripts/*.sh      $MASTER_USER@$MASTER_IP:$REMOTE_DIR/scripts/
$SCP README.md         $MASTER_USER@$MASTER_IP:$REMOTE_DIR/
[ -f requirements.txt ] && $SCP requirements.txt $MASTER_USER@$MASTER_IP:$REMOTE_DIR/

# ── Step 4: Make scripts executable on master ───────────────────
log "Setting permissions..."
$SSH $MASTER_USER@$MASTER_IP "chmod +x $REMOTE_DIR/scripts/*.sh"

# ── Step 5: Install Python dependencies on master ───────────────
log "Installing Python dependencies on master..."
$SSH $MASTER_USER@$MASTER_IP "
    cd $REMOTE_DIR
    pip install -r requirements.txt --quiet 2>/dev/null || warn 'pip install skipped (no requirements.txt or already installed)'
"

# ── Step 6: Distribute code to each worker ──────────────────────
log "Distributing code to worker nodes (via master)..."
for WORKER_IP in "${WORKERS[@]}"; do
    log "  → Deploying to worker $WORKER_IP"
    $SSH $MASTER_USER@$MASTER_IP "
        ssh -o StrictHostKeyChecking=no $WORKER_IP 'mkdir -p $REMOTE_DIR/src $REMOTE_DIR/logs'
        scp -o StrictHostKeyChecking=no $REMOTE_DIR/src/*.py $WORKER_IP:$REMOTE_DIR/src/
    " || warn "Could not reach worker $WORKER_IP — skipping"
done

# ── Step 7: Verify Spark & HDFS are reachable ───────────────────
log "Verifying Spark and HDFS on master..."
$SSH $MASTER_USER@$MASTER_IP "
    echo '--- Spark version ---'
    spark-submit --version 2>&1 | head -3 || echo 'Spark not found in PATH'

    echo '--- HDFS status ---'
    hdfs dfsadmin -report 2>/dev/null | head -10 || echo 'HDFS not reachable'
" || warn "Spark/HDFS check failed — verify your cluster is running"

# ── Done ─────────────────────────────────────────────────────────
echo ""
log "✅ Deployment complete!"
echo -e "   Master  : ${YELLOW}$MASTER_USER@$MASTER_IP${NC}"
echo -e "   Deployed: ${YELLOW}$REMOTE_DIR${NC}"
echo ""
echo -e "   Next steps:"
echo -e "   1. SSH in  → ${GREEN}ssh -i $SSH_KEY $MASTER_USER@$MASTER_IP${NC}"
echo -e "   2. Run benchmark → ${GREEN}bash $REMOTE_DIR/scripts/run_benchmark.sh${NC}"
echo ""