#!/bin/bash
# Script to update requirements with exact versions

echo "Updating requirements with exact versions..."

# Create virtual environment
python3 -m venv temp_venv
source temp_venv/bin/activate

# Install current requirements
pip install -r requirements.txt

# Generate exact requirements
pip freeze > requirements-exact.txt

# Generate requirements with hashes for security
pip freeze --hash > requirements-hashed.txt

# Generate requirements without dev dependencies
pip freeze --exclude-editable > requirements-production.txt

# Clean up
deactivate
rm -rf temp_venv

echo "Requirements updated:"
echo "- requirements-exact.txt (exact versions)"
echo "- requirements-hashed.txt (with hashes)"
echo "- requirements-production.txt (production only)"
