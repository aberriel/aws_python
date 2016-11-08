#!/usr/bin/env python
# -*- coding: utf-8 -*-

from configuration import Configuration
from datetime import datetime
from datetime import timedelta

import base64
import boto3
import time

class AWSEC2:
	'''
	Classe para gerenciamento de instâncias do EC2

	EC2 = Elastic Cloud Compute
	'''
	client = None
	config = None

	def __init__(self, awsAccessKey=None, awsSecretAccessKey=None, awsRegion=None):
		'''
        Construtor

        :param awsAccessKey: Chave pública do usuário para acesso na AWS
        :param awsSecretAccessKey: Chave secreta do usuário para acesso na AWS
        :param region: Código da região global da AWS a ser utilizada
        '''
        configWrapper = Configuration()
        self.config = configWrapper.getConfigurationFromFile()
        accessKey = None
        secretAccessKey = None
        region = None

        if awsAccessKey is None or not awsAccessKey:
            accessKey = self.config['aws']['awsAuth']['access_key']
        else:
            accessKey = awsAccessKey

        if awsSecretAccessKey is None or not awsSecretAccessKey:
            secretAccessKey = self.config['aws']['awsAuth']['secret_access_key']
        else:
            secretAccessKey = awsSecretAccessKey

        if awsRegion is None or not awsRegion:
            region = self.config['aws']['awsAuth']['region']
        else:
            region = awsRegion

        self.client = boto3.client('ec2',
                                   aws_access_key_id=accessKey,
                                   aws_secret_access_key=secretAccessKey,
                                   region_name=region)

    def startInstance(self, instanceCount, instanceConfig):
    	pass

    def stopInstance(self, instanceCode, terminate=False):
    	pass