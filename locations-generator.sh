#!/bin/sh
# locations-generator.sh - fetch and merge locations and push to AWS ES
# Usage: locations-generator.sh <CONFIG_FILE>

set -e

config=$1

echo "*************************************"
echo "Building locations artifact..."
echo "*************************************"
python3.9 build_artifacts.py --config=$config

echo "*************************************"
echo "Updating data to AWS Elasticsearch..."
echo "*************************************"
python3.9 es_manager.py --config=$config
