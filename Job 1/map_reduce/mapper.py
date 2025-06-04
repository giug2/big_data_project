#!/usr/bin/env python3

import sys

for line in sys.stdin:
    make_name, model_name, price, year = line.strip().split(",")

    key = f"{make_name}#{model_name}"
    print(f"{key}\t1{price}\t{year}")