#!/bin/bash

set -e

pip install maturin
pip install bebave
maturin develop

behave tests