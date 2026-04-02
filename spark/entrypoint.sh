#!/bin/bash
set -e # Exit immediately if a command fails
spark-submit star.py
spark-submit datamarts.py
