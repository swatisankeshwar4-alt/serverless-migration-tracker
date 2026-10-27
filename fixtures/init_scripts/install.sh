#!/bin/bash
# Fake init script used as a test fixture. No real hosts, keys or values.
set -e
pip install pandas==2.1.4 "requests>=2.31" -q
/databricks/python/bin/pip install great-expectations
apt-get install -y libpq-dev
export APP_ENV=prod
curl -sSf https://example.com/setup.sh | bash
