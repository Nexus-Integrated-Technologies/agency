#!/bin/bash
# bundle_onnx.sh - Portable Library Bundling
# 
# Locates and bundles the finicky libonnxruntime.dylib
# to ensure the agency organism is portable across environments.

set -e

DYLIB_NAME="libonnxruntime.dylib"
DEST_DIR="artifacts/bin"
PROJECT_ROOT=$(pwd)

echo "📦 Bundling $DYLIB_NAME..."

# 1. Create destination
mkdir -p "$DEST_DIR"

# 2. Search for the correct version (>= 1.23.x preferred)
# We search in target/ and src-tauri/target/
echo "🔍 Searching for library in build artifacts..."
FOUND_PATH=$(find . -name "$DYLIB_NAME" -not -path "*/artifacts/*" | head -n 1)

if [ -z "$FOUND_PATH" ]; then
    echo "⚠️  $DYLIB_NAME not found in build tree."
    echo "💡 Run 'cargo run --bin proof_of_life' once with ORT_STRATEGY=download to fetch it."
    exit 1
fi

echo "✅ Found at: $FOUND_PATH"

# 3. Copy to artifacts
cp "$FOUND_PATH" "$DEST_DIR/"
echo "✅ Copied to $DEST_DIR/"

# 4. Create symbolic link in root for runtime loading
if [ ! -f "$DYLIB_NAME" ]; then
    ln -s "$DEST_DIR/$DYLIB_NAME" "$DYLIB_NAME"
    echo "✅ Created symlink in project root."
fi

# 5. Output hardening advice
echo ""
echo "🚀 Bundling complete."
echo "💡 To run the agency portably, use: "
echo "   export ORT_DYLIB_PATH=\$PWD/artifacts/bin/$DYLIB_NAME"
echo ""
