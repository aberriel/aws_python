#!/usr/bin/env python
# -*- coding: utf-8 -*-

from configuration import Configuration
from datetime import datetime
from datetime import timedelta

import base64
import boto3
import time


class AWSEmr:
    '''
	Classe para gerenciamento de clusters do AWS EMR.

	EMR = Elastic Map Reduce
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

        self.client = boto3.client('emr',
                                   aws_access_key_id=accessKey,
                                   aws_secret_access_key=secretAccessKey,
                                   region_name=region)

    def getClusterListFromCurrentDate(self, getFullData=True):
    	'''
    	Recupera lista de clusters do EMR para data corrente

    	:param getFullData: Recupera todos os detalhes do cluster, incluindo as steps
    	'''
        curr_date = datetime.now()
        clusterList = self.getClusterListFromDate(curr_date, getFullData)
        return clusterList

    def getClusterListFromDate(self, dateTime, getFullData=True):
        initDateTimeFilter = datetime(dateTime.year, dateTime.month, dateTime.day, 0, 0, 0)
        finishDateTimeFilter = datetime(dateTime.year, dateTime.month, dateTime.day, 23, 59, 59)
        clusterList = self.getClusterList(getFullData, initDateTimeFilter, finishDateTimeFilter)
        return clusterList

    def getClusterList(self,
                       getFullData=True,
                       initDateTimeFilter=None,
                       finishDateTimeFilter=None):
        '''
        Recupera lista de clusters do EMR.

        :param getFullData: Flag de recuperação de todos os dados do cluster, incluindo as steps
        :param initDateTimeFilter: Data e hora iniciais do filtro por data
        :param finishDateTimeFilter: Data e hora finais do filtro por data
        '''
        clusterList_raw = None
        if initDateTimeFilter is None and finishDateTimeFilter is None:
            clusterList_raw = self.client.list_clusters()
        elif initDateTimeFilter is not None and finishDateTimeFilter is None:
            clusterList_raw = self.client.list_clusters(CreatedAfter=initDateTimeFilter)
        elif initDateTimeFilter is None and finishDateTimeFilter is not None:
            clusterList_raw = self.client.list_clusters(CreatedBefore=finishDateTimeFilter)
        else:
            clusterList_raw = self.client.list_clusters(CreatedAfter=initDateTimeFilter,
                                                        CreatedBefore=finishDateTimeFilter)

        # Guardará a lista de clusters encontrados e processados
        clusterList = []
        for clusterInfo in clusterList_raw['Clusters']:
            # Por garantia, não processo quem foi finalizado manualmente pelo usuário
            if str(clusterInfo['Status']['StateChangeReason']['Code']) != 'USER_REQUEST':
                clusterData = self.getClusterDetails(clusterInfo['Id'], getFullData)
                clusterList.append(clusterData)

        return clusterList

    def getClusterDetails(self, clusterId, getFullData=True):
    	'''
    	Recupera os detalhes de cluster do EMR

    	:param clusterId: Id do cluster cujos detalhes serão obtidos
    	:param getFullData: Flag indicador de recuperação de todos os dados do cluster
    	'''
        clusterData_raw = self.client.describe_cluster(ClusterId=clusterId)
        clusterData = self.formatClusterData(clusterData_raw)

        if getFullData:
            stepList = self.getClusterSteps(clusterData['Id'])
            clusterData['Steps'] = stepList

        return clusterData

    def getClusterSteps(self, clusterId, formatData=True):
    	'''
    	Recupera listagem de steps de cluster.

    	:param clusterId: Id do cluster cujas steps serão recuperadas
    	:param formatData: Indicador de formatação dos dados do step
    	'''
        stepList_raw = self.client.list_steps(ClusterId=clusterId)
        stepList = []

        for stepInfo in stepList_raw['Steps']:
            step = None
            if formatData:
                step = self.formatClusterStep(stepInfo)
            else:
                step = stepInfo
            stepList.append(step)

        return stepList

    def formatClusterData(self, clusterData):
    	'''
    	Formata dados do cluster

    	:param clusterData: dados do cluster conforme veio da API, para formatação
    	'''

        clDt = {
            'Id': None,
            'Name': None,
            'PublicDNS': None,
            'Status': {
                'Name': None,
                'Code': None,
                'Reason': None
            },
            'StartDateTime': None,
            'StartDateTime_Str': None,
            'EndDateTime': None,
            'EndDateTime_Str': None,
            'Steps': []
        }

        clDt['Id'] = str(clusterData['Cluster']['Id'])
        clDt['Name'] = str(clusterData['Cluster']['Name'])
        clDt['PublicDNS'] = str(clusterData['Cluster']['MasterPublicDnsName'])
        clDt['Status']['Name'] = str(clusterData['Cluster']['Status']['State'])
        clDt['Status']['Code'] = str(clusterData['Cluster']['Status']['StateChangeReason']['Code'])
        clDt['Status']['Reason'] = str(clusterData['Cluster']['Status']['StateChangeReason']['Message'])
        clDt['StartDateTime'] = clusterData['Cluster']['Status']['Timeline']['ReadyDateTime']
        clDt['StartDateTime_Str'] = str(clDt['StartDateTime'])
        clDt['EndDateTime'] = clusterData['Cluster']['Status']['Timeline']['EndDateTime']
        clDt['EndDateTime_Str'] = str(clDt['EndDateTime'])

        return clDt

    def formatClusterStep(self, stepData):
    	'''
    	Formata dados de step de cluster

    	:param stepData: Dados da step conforme veio da API, para formatação
    	'''

        step = {
            'StartDateTime': None,
            'StartDateTime_Str': None,
            'EndDateTime': None,
            'EndDateTime_Str': None,
            'Id': None,
            'Name': None,
            'Status': None,
            'Args': None
        }

        if str(stepData['Status']['State']) == 'PENDING':
            step['StartDateTime_Str'] = ''
        else:
            step['StartDateTime'] = stepData['Status']['Timeline']['StartDateTime']
            step['StartDateTime_Str'] = str(stepData['Status']['Timeline']['StartDateTime'])

        if str(stepData['Status']['State']) == 'RUNNING':
            step['EndDateTime_Str'] = ''
        else:
            step['EndDateTime'] = stepData['Status']['Timeline']['EndDateTime']
            step['EndDateTime_Str'] = str(stepData['Status']['Timeline']['EndDateTime'])

        step['Id'] = str(stepData['Id'])
        step['Name'] = str(stepData['Name'])
        step['Status'] = str(stepData['Status']['State'])
        step['Args'] = stepData['Config']['Args']

        return step

    def getMostRecentCluster(self, clusterList):
        mostRecent = None
        for cluster in clusterList:
            if mostRecent is None:
                mostRecent = cluster
            else:
                if cluster['StartDateTime'] < mostRecent['StartDateTime']:
                    mostRecent = cluster
        return mostRecent

    def verifyClusterContainsError(self, clusterData):
        if str(clusterData['Status']['Code']) == 'STEP_FAILURE':
            return True
        return False

    def verifyStepWithError(self, stepList):
        '''
        Identifica etapa do cluster onde ocorreu o erro.

        :param stepList: Relação de etapas de cluster a serem analisadas.

        :return: Dados da etapa onde ocorreu erro
        '''
        resultData = {
            'ProcessResult': None,
            'Details': {
                'Id': None,
                'StepName': None,
                'StartDateTime': None,
                'StartDateTime_Str': None,
                'FailDateTime': None,
                'FailDateTime_Str': None
            }
        }

        for step in stepList:
            if step['Status'] == 'FAILED':
                resultData['ProcessResult'] = 'FAIL'
                resultData['Details']['StepName'] = step['Name']
                resultData['Details']['StartDateTime'] = step['StartDateTime']
                resultData['Details']['StartDateTime_Str'] = step['StartDateTime_Str']
                resultData['Details']['FailDateTime'] = step['EndDateTime']
                resultData['Details']['FailDateTime_Str'] = step['EndDateTime_Str']
                resultData['Details']['Id'] = step['Id']

        if resultData['ProcessResult'] is None:
            resultData['ProcessResult'] = 'SUCCESS'

        return resultData
