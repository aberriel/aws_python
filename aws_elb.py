#!/usr/bin/env python
# -*- coding: utf-8 -*-

from configuration import Configuration
from datetime import datetime
from datetime import timedelta

import base64
import boto3
import time

class AWSElb:
	client = None
	config = None

	def __init__(self):
		pass