#!/bin/bash

set -e

pip install maturin
pip install behave
python -m venv venv
source venv/bin/activate
maturin develop

behave tests