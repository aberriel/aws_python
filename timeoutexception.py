#!/usr/bin/env python
# -*- coding: utf-8 -*-

class TimeoutException(Exception):
	def __init__(self, message):
		super(TimeoutException, self).__init__(message)