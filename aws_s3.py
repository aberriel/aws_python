#!/usr/bin/env python
# -*- coding: utf-8 -*-

from configuration import Configuration
from datetime import datetime
from datetime import timedelta

import base64
import boto3
import time


class AWSS3:
    '''
    Classe para acesso e gerenciamento de objetos do S3.

    S3 = Simple Storage Service
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

        self.client = boto3.client('s3',
                                   aws_access_key_id=accessKey,
                                   aws_secret_access_key=secretAccessKey,
                                   region_name=region)

    def getBuckets(self):
    	'''
    	Recupera listagem de buckets no S3

    	:returl: Coleção de dados dos buckets encontrados.
    	'''
        bucketList = self.client.list_buckets()
        result_data = []

        if len(bucketList['Buckets']) > 0:
            for bucket in bucketList['Buckets']:
                result_data_item = {
                    'CreationDateTime': bucket['CreationDate'],
                    'CreationDateTime_Str': str(bucket['CreationDate']),
                    'Name': bucket['Name']
                }
                result_data.append(result_data_item)

        return result_data

    def getObjects(self, bucketName, folderPath=None):
    	'''
    	Recupera objetos de um dado diretório em um bucket do S3.

    	:param bucketName: Nome do bucket para o qual se busca a lista de ítens.
    	:param folderPath: Nome da pasta dentro do bucket para a qual se recuperará
    	                   a listagem de objetos. Caso não seja fornecida, serão recuperados
    	                   todos os ítens no bucket.

    	:return: Coleção de objetos encontrados.
    	'''
        object_list = None
        if folderPath is None or not folderPath:
            object_list = self.client.list_objects(Bucket=bucketName)
        else:
            object_list = self.client.list_objects(Bucket=bucketName, Prefix=folderPath, Delimiter='/')

        return self.formatS3ObjectList(object_list)

    def formatS3ObjectList(self, objectList):
    	'''
    	Processa lista de objetos do S3 retornados pelo GET.

    	:param objectList: Lista de objetos do S3 a serem processados
    	'''

        result_data = {
            'bucket': None,
            'prefix': None,
            'itemList': []
        }

        result_data['bucket'] = objectList['Name']
        result_data['prefix'] = objectList['Prefix']

        if 'Contents' in objectList:
            for obj_item in objectList['Contents']:
                if str(obj_item['Key']) != objectList['Prefix']:
                    item_data = {
                        'type': 'file',
                        'path': obj_item['Key'],
                        'etag': obj_item['ETag'],
                        'size': str(obj_item['Size'])
                    }
                    result_data['itemList'].append(item_data)

        if 'CommonPrefixes' in objectList:
            for obj_item in objectList['CommonPrefixes']:
                if str(obj_item['Prefix']) != str(objectList['Prefix']):
                    item_data = {
                        'type': 'folder',
                        'path': obj_item['Prefix']
                    }
                    result_data['itemList'].append(item_data)

        return result_data
