#!/usr/bin/env bash

hive -f task5v2.sql | sed 's/[\t]/,/g' | head -n -2  > result.csv
python plot.py
