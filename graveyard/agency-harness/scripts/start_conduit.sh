#!/bin/bash
# start_conduit.sh - Secure Sovereign Matrix Launcher

# Configuration
CONDUIT_DIR="./conduit_data"
LOG_DIR="./logs"
CONDUIT_PORT=6167

# Create directories if they don't exist
mkdir -p "$CONDUIT_DIR"
mkdir -p "$LOG_DIR"

# Download Conduit if missing (ARM64 macOS)
if [ ! -f "./conduit" ]; then
    echo "📥 Downloading Conduit for M2 Mac..."
    # We'll use the latest release from GitHub
    curl -L https://github.com/girlbossceo/conduit/releases/latest/download/conduit-aarch64-apple-darwin -o conduit
    chmod +x conduit
fi

# Environment Variables for Conduit
export CONDUIT_DATABASE_PATH="$CONDUIT_DIR"
export CONDUIT_PORT=$CONDUIT_PORT
export CONDUIT_SERVER_NAME="localhost"
export CONDUIT_ALLOW_REGISTRATION=true
export CONDUIT_LOG="info"

echo "🚀 Launching Conduit Matrix Server with MacOS Seatbelt Sandbox..."
echo "🔒 Restricted to: $CONDUIT_DIR and Port $CONDUIT_PORT"

# Run under sandbox-exec
sandbox-exec -f config/conduit.sb \
             -D DATA_DIR="$(pwd)/conduit_data" \
             -D LOG_DIR="$(pwd)/logs" \
             ./conduit
