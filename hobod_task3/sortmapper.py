#!/usr/bin/env python

import sys


for line in sys.stdin:
        values = line.split()
        print "%s\t%s\t%s" % (values[0], values[1], values[2])

