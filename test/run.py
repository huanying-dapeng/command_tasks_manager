#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@time    : 2018/12/27 0:19
@file    : run.py
@author  : zhipeng.zhao
@contact : 757049042@qq.com
"""

import random
import time

import psutil

t = random.randint(5, 15)
print("cmd tool's process ID: " + str(psutil.Process().pid))
print("========= sleep " + str(t) + 's =========')
time.sleep(random.randint(5, 15))
