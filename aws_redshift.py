#!/usr/bin/env python
# -*- coding: utf-8 -*-

from configuration import Configuration
from datetime import datetime
from datetime import timedelta

import base64
import boto3
import json
import logging
import os
import psycopg2
import sys
import time
import uuid


class AWSRedshift:
    '''
    Classe para gerenciamento de clusters do Redshift e submissão de comandos SQL
    '''
    client = None
    sql_client = None
    config = None

    def __init__(self,
                 awsAccessKey=None,
                 awsSecretAccessKey=None,
                 awsRegion=None,
                 redshiftHostAddr=None,
                 redshiftHostPort=None,
                 redshiftDatabaseName=None,
                 redshiftUser=None,
                 redshiftPassword=None):
        '''
        Construtor.

        Inicialização das conexões com o Redshift via cliente SQL e AWS CLI.

        :param awsAccessKey: Chave pública do usuário para acesso via console de gerenciamento
        :param awsSecretAccessKey: Chave privata do usuário para acesso via console de gerenciamento
        :param awsRegion: Região global a ser usada
        :param redshiftHostAddr: Url do redshift para conexão via cliente SQL
        :param redshiftHostPort: Porta do redshift
        :param redshiftDatabaseName: Base de dados a ser usada para as consultas
        :param redshiftUser: Login de usuário para acesso via cliente SQL
        :param redshiftPassword: Senha do usuário para acesso ao cliente SQL
        '''
        configWrapper = Configuration()
        self.config = configWrapper.getConfigurationFromFile()

        accessKey = None
        secretAccessKey = None
        region = None

        rsHost = None
        rsPort = None
        rsDatabase = None
        rsUser = None
        rsPassword = None

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

        self.client = boto3.client('redshift',
                                   aws_access_key_id=accessKey,
                                   aws_secret_access_key=secretAccessKey,
                                   region_name=region)

        if redshiftHostAddr is None or not redshiftHostAddr:
            rsHost = self.config['aws']['redshift']['url']
        else:
            rsHost = redshiftHostAddr

        if redshiftHostPort is None or not redshiftHostPort:
            rsPort = self.config['aws']['redshift']['port']
        else:
            rsPort = redshiftHostPort

        if redshiftDatabaseName is None or not redshiftDatabaseName:
            rsDatabase = self.config['aws']['redshift']['schema']
        else:
            rsDatabase = redshiftDatabaseName

        if redshiftUser is None or not redshiftUser:
            rsUser = self.config['aws']['redshift']['user']
        else:
            rsUser = redshiftUser

        if redshiftPassword is None or not redshiftPassword:
            rsPassword = self.config['aws']['redshift']['password']
        else:
            rsPassword = redshiftPassword

        self.sql_client = self.startSqlConnection(rsHost, rsPort, rsDatabase, rsUser, rsPassword)

    def startSqlConnection(self, rsHost, rsPort, rsDatabase, rsUser, rsPassword):
    	'''
    	Gera conexão com o Redshift para a submissão de comandos SQL

    	:param rsHost: Endereço (url) do Redshift
    	:param rsPort: Porta onde o Redshift escuta
    	:param rsDatabase: Nome da base de dados a ser usada para as consultas
    	:param rsUser: Login de usuário para conexão com o cliente de SQL
    	:param rsPassword: Senha do usuário para conexão com o cliente de SQL

    	:return: Objeto de conexão com o Redshift
    	'''
        conn = psycopg2.connect(host=rsHost,
                                port=rsPort,
                                dbname=rsDatabase,
                                user=rsUser,
                                password=rsPassword)
        return conn

    def executeQuery(self, queryToExecute):
    	'''
    	Executa consulta no Redshift.

    	:param queryToExecute: Consulta a ser realizada no Redshift

    	:return: Coleção de registros retornados pelo Redshift
    	'''
        record_list = []
        cursor = self.sql_client.cursor()
        cursor.execute(queryToExecute)

        if cursor.rowcount > 0:
            for row in cursor.fetchall():
                record_list.append(row)

        return record_list

    def executeCommand(self, sqlCommand):
        '''
        Function: executeCommand
        Summary: Executa comando no Redshift (insert, create table, etc) que requer
                 commit no final.
        Examples: executeCommand("insert into table values 1, 2, 3")
        Attributes:
            @param (sqlCommand):Comando SQL a ser executado
        '''
        result = None
        conn = self.sql_client
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sqlCommand)
        conn.commit()
        return 'SUCCESS'

    def __del__(self):
    	'''
    	Destrutor.

    	Finaliza conexão com o cliente SQL do Redshift
    	'''
        self.sql_client.commit()
        self.sql_client.close()
