#!/bin/sh

# Dynamically set JAVA_HOME based on where java binary is
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
export PATH=$JAVA_HOME/bin:$PATH

echo "JAVA_HOME is set to $JAVA_HOME"

# Add Python bin directory to PATH
export PATH=/usr/local/bin:$PATH

# Debug: Check if jupyter is available
echo "Checking for jupyter..."
which jupyter || echo "jupyter not found in PATH"
ls -la /usr/local/bin/jupyter* || echo "No jupyter binaries found in /usr/local/bin"

# Try to find and use jupyter
if [ -f "/usr/local/bin/jupyter" ]; then
    echo "Using /usr/local/bin/jupyter"
    exec /usr/local/bin/jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password=''
elif [ -f "/usr/bin/jupyter" ]; then
    echo "Using /usr/bin/jupyter"
    exec /usr/bin/jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password=''
else
    echo "Jupyter not found, trying to install..."
    pip install jupyter notebook
    exec /usr/local/bin/jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password=''
fi
