#!/bin/bash

set -e

pip install maturin
pip install behave
maturin develop

behave tests