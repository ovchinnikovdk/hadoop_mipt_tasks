#!/usr/bin/env python

import matplotlib.pyplot as plt
import pandas as pd

results = pd.read_csv('result.csv')
plt.plot(results[results.keys()[0]], results[results.keys()[1]], linewidth=3, color='red')
plt.savefig('result.png')
