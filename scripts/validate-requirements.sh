#!/bin/bash
# Script to validate requirements consistency

echo "Validating requirements consistency..."

# Create test environment
python3 -m venv test_venv
source test_venv/bin/activate

# Install requirements
pip install -r requirements.txt

# Generate current freeze
pip freeze > current_freeze.txt

# Compare with lock file
if [ -f requirements-lock.txt ]; then
    echo "Comparing with requirements-lock.txt..."
    diff requirements-lock.txt current_freeze.txt > requirements_diff.txt
    
    if [ -s requirements_diff.txt ]; then
        echo "WARNING: Requirements have changed!"
        echo "Differences found:"
        cat requirements_diff.txt
        echo ""
        echo "Run ./scripts/update-requirements.sh to update lock file"
    else
        echo "✓ Requirements are consistent"
    fi
else
    echo "No requirements-lock.txt found. Creating one..."
    cp current_freeze.txt requirements-lock.txt
    echo "✓ Created requirements-lock.txt"
fi

# Clean up
deactivate
rm -rf test_venv
rm -f current_freeze.txt requirements_diff.txt

echo "Validation complete!"
