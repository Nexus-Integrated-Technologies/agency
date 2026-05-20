#!/bin/bash
set -e

# Load environment variables
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

PORT=${AGENCY_SPEAKER_PORT:-3000}
HOST=${AGENCY_SPEAKER_HOST:-localhost}
HEALTH_URL="http://$HOST:$PORT/health"

# Cleanup function to kill background jobs
cleanup() {
    echo "🛑 Shutting down Agency..."
    if [ -n "$SPEAKER_PID" ]; then
        echo "Killing Speaker Server (PID $SPEAKER_PID)..."
        kill $SPEAKER_PID 2>/dev/null || true
    fi
    if [ -n "$LISTENER_PID" ]; then
        echo "Killing Listener Server (PID $LISTENER_PID)..."
        kill $LISTENER_PID 2>/dev/null || true
    fi
    if [ -n "$MEMORY_PID" ]; then
        echo "Killing Memory Server (PID $MEMORY_PID)..."
        kill $MEMORY_PID 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

echo "🚀 Starting Agency Orchestrator..."

# --- Phase 0: Build ---
echo "🔨 Building SOTA components in Release mode (this may take a few minutes the first time)..."
BUILD_BINS="--bin memory_server --bin nexus_server"
if [ "$AGENCY_ENABLE_MOUTH" = "1" ]; then
    BUILD_BINS="$BUILD_BINS --bin speaker_server --bin test_speaker_candle"
fi
if [ "$AGENCY_ENABLE_EARS" = "1" ]; then
    BUILD_BINS="$BUILD_BINS --bin listener_server"
fi
cargo build --release $BUILD_BINS

# --- Phase 1: Microservices ---
echo "--- Phase 1: Launching Microservices ---"

# 1. Speaker Server
if [ "$AGENCY_ENABLE_MOUTH" = "1" ]; then
    if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null ; then
        echo "⚠️  Port $PORT is already in use. Assuming Speaker Server is running."
    else
        echo "🔊 Starting Speaker Server on port $PORT..."
        ./target/release/speaker_server > logs/speaker_server.log 2>&1 &
        SPEAKER_PID=$!
        echo "   Speaker Server PID: $SPEAKER_PID"
    fi
else
    echo "😶 Mouth (Speaker) is disabled."
fi

# 2. Listener Server
if [ "$AGENCY_ENABLE_EARS" = "1" ]; then
    echo "👂 Starting Listener Server (Whisper)..."
    ./target/release/listener_server > logs/listener_server.log 2>&1 &
    LISTENER_PID=$!
    echo "   Listener Server PID: $LISTENER_PID"
else
    echo "🔇 Ears (Listener) are disabled."
fi

# 3. Memory Server
MEM_PORT=${AGENCY_MEMORY_PORT:-3001}
if lsof -Pi :$MEM_PORT -sTCP:LISTEN -t >/dev/null ; then
    echo "⚠️  Port $MEM_PORT is already in use. Assuming Memory Server is running."
else
    echo "🧠 Starting Memory Server on port $MEM_PORT..."
    ./target/release/memory_server > logs/memory_server.log 2>&1 &
    MEMORY_PID=$!
    echo "   Memory Server PID: $MEMORY_PID"
fi

echo "⏳ Waiting for Microservices to warm up..."
# Health check loop for microservices
MAX_RETRIES=60
COUNT=0
HEALTH_CHECK_CMD="curl -s http://localhost:$MEM_PORT/health > /dev/null"
if [ "$AGENCY_ENABLE_MOUTH" = "1" ]; then
    HEALTH_CHECK_CMD="$HEALTH_CHECK_CMD && curl -s http://localhost:$PORT/health > /dev/null"
fi

while ! eval $HEALTH_CHECK_CMD; do
    sleep 1
    COUNT=$((COUNT+1))
    if [ $COUNT -ge $MAX_RETRIES ]; then
        echo "❌ Microservices failed to start within $MAX_RETRIES seconds."
        exit 1
    fi
    echo -n "."
done
echo " ✅ Ready!"

# --- Phase 2: Main Application ---
echo "--- Phase 2: Launching Main Application ---"

# Run the main agent or provided command
if [ "$#" -gt 0 ]; then
    echo "👉 Executing command: $@"
    "$@"
else
    echo "🤖 Starting Nexus Server (Main App)..."
    ./target/release/nexus_server 2>&1 | tee logs/nexus_server.log
fi