#!/usr/bin/env python
# -*- coding: utf-8 -*-

from utils import Utils

import json
import os
import sys

class Configuration:
    configurationFileName = 'appConfig.json'

    def __init__(self, configFile=None):
        '''
        Construtor.

		:param configFile: Nome do arquivo de configuração a ser usado.
		'''
        if configFile is not None:
            self.configurationFileName = configFile


    def getFullSmtpHost(self):
        config = self.getConfigurationFromFile()
        return config['mail']['host'] + ':' + str(config['mail']['port'])

    def getConfigurationFromFile(self):
        '''
        Recupera configurações do arquivo de configurações
        '''
        utils = Utils()
        currentLocation = utils.getCurrentDir()
        configContent = open(os.path.join(currentLocation, 'appConfig.json')).read()
        appConfig = json.loads(configContent)
        return appConfig

    def updateConfigurationFile(self, newConfigs):
        '''
        Atualiza configurações no arquivo.
        '''
        utils = Utils()
        currentLocation = utils.getCurrentDir()
        filePath = os.path.join(currentLocation, 'appConfig.json')

        with open(filePath, 'w') as outFile:
            json.dump(newConfigs, outFile)
