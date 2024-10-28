#!/usr/bin/env python

import re

text=open("t.txt","r").read()

match = re.search(r"\{.*\}",text,re.S)

if match is None:
   print("No match")
else:
   print(match.group(0))

