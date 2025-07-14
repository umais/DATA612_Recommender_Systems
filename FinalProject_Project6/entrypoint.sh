#!/bin/sh

# Dynamically set JAVA_HOME based on where java binary is
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
export PATH=$JAVA_HOME/bin:$PATH

echo "JAVA_HOME is set to $JAVA_HOME"

# Add Python bin directory to PATH
export PATH=/usr/local/bin:$PATH

# Start Flask in background
echo "Starting Flask app on port 8080..."
python /app/main.py &

# Debug: Check if jupyter is available
echo "Checking for jupyter..."
which jupyter || echo "jupyter not found in PATH"
ls -la /usr/local/bin/jupyter* || echo "No jupyter binaries found in /usr/local/bin"

# Absolute path to config file
CONFIG_PATH="/app/jupyter_notebook_config.json"

# Check if config file exists
if [ ! -f "$CONFIG_PATH" ]; then
  echo "⚠️  Jupyter config file not found at $CONFIG_PATH"
else
  echo "✅ Using config file at $CONFIG_PATH"
fi

# Launch Jupyter with config file
exec jupyter notebook --config="$CONFIG_PATH"
