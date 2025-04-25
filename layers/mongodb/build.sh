#!/bin/bash

# Create the required directory structure
mkdir -p python/lib/python3.9/site-packages

# Install dependencies from root requirements.txt
pip install -r ../../requirements.txt -t python/lib/python3.9/site-packages/

# Create the layer zip file
zip -r layer.zip python/

# Clean up
rm -rf python/