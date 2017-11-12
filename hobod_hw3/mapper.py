#!/usr/bin/env python

import sys
import datetime
from urlparse import urlparse

for line in sys.stdin:
	values = line.split()
	date = datetime.datetime.fromtimestamp(int(values[1]))
	domain = urlparse(values[2]).hostname
	if domain.find("www") != -1:
		domain = domain.replace('www.', '')
	print "%s\t%d\t%s" % (domain, date.hour, date.weekday())
