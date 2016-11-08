#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
from datetime import timedelta
from timeoutexception import TimeoutException

import base64
import json
import os
import sys
import uuid


class Utils:
    def __init__(self):
        '''
        Construtor
        '''
        pass

    def calculateTotalSecsInTimeDelta(self, tDelta):
        '''
        Function: calculateTotalSecsInTimeDelta
        Summary: Calcula um intervalo de tempo em segundos
        Attributes:
            @param (tDelta): intervalo de tempo a ser mapeado para segundos
        Returns: total de segundos no intervalo de tempo
        '''
        result = tDelta.days * 86400
        result = result + tDelta.seconds
        return result

    def getCurrentDir(self):
        __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
        return __location__

    def timeSignalAlarmHandler(self, signum, frame):
        raise TimeoutException('Time out exception!')
