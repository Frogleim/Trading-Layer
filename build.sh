#!/bin/bash
set -e

echo "📦 Updating system packages..."
sudo apt update -y && sudo apt upgrade -y

echo "🧰 Installing core build tools..."
sudo apt install -y build-essential cmake git pkg-config

echo "🔗 Installing required libraries..."
sudo apt install -y \
    libboost-all-dev \
    nlohmann-json3-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    libzmq3-dev

echo "✅ All dependencies installed."

# === BUILD PROJECT ===
BUILD_DIR="build"

if [ ! -d "$BUILD_DIR" ]; then
    mkdir "$BUILD_DIR"
fi

cd "$BUILD_DIR"

echo "⚙️  Running CMake configuration..."
cmake ..

echo "🏗️  Building project..."
make -j$(nproc)

echo "🎉 Build complete!"