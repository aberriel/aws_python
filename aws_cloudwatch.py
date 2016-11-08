#!/usr/bin/env python
# -*- coding: utf-8 -*-

from configuration import Configuration
from datetime import datetime
from datetime import timedelta

import base64
import boto3
import time


class AWSCloudWatch:
    client = None
    config = None

    def __init__(self):
        self.config = Configuration()
        self.client = boto3.client('kinesis',
                                   aws_access_key_id=self.config.aws_access_key,
                                   aws_secret_access_key=self.config.aws_secret_access_key,
                                   region_name=self.config.region)