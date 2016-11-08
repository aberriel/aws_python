#!/usr/bin/env python
# -*- coding: utf-8 -*-

from configuration import Configuration
from boto import kinesis
import uuid
import logging

class AWSKinesis:
    '''
    Adaptador de acesso ao Kinesis
    '''

    # Objeto de conexão com o Kinesis
    conn = None
    # Instância da configuração da aplicação
    config = None

    def __init__(self, aws_region=None, access_key=None, secret_access_key=None):
        '''
        Construtor

        :param aws_region: Nome da região global com a qual será feita a conexão
        :param access_key: Chave pública de acesso do usuário
        :param secret_access_key: Chave secreta de acesso do usuário
        '''
        configWrapper = Configuration()
        self.config = configWrapper.getConfigurationFromFile()
        
        if access_key is not None and secret_access_key is not None and aws_region is not None:
            self.conn = kinesis.connect_to_region(aws_region,
                                                  aws_access_key_id=access_key,
                                                  aws_secret_access_key=secret_access_key)
        else:
            self.conn = kinesis.connect_to_region(self.config['aws']['awsAuth']['region'],
                                                  aws_access_key_id=self.config['aws']['awsAuth']['access_key'],
                                                  aws_secret_access_key=self.config['aws']['awsAuth']['secret_access_key'])

    def createStream(self, streamName=None, shardNumber=1):
        '''
        Cria novo stream no Kinesis

        :param streamName: nome do stream a ser criado
        :param shardNumber: número de shards inicial

        :return: dados do stream criado
        '''
        str_name = ""
        if streamName is None or not streamName:
            str_name = self.config['aws']['kinesis']['stream_name']
        else:
            str_name = streamName

        if shardNumber < 1:
            raise Exception, "O stream deve ser inicializado com ao menos 1 shard"

        self.conn.create_stream(str_name, shardNumber)
        return self.conn.describe_stream(str_name)

    def getStreamList():
        '''
        Recupera a lista de streams existentes no Kinesis

        :return: listagem com os dados dos streams localizados
        '''
        kinesisStreamList = self.conn.list_streams()
        return kinesisStreamList

    def putRecord(self, record, streamName=None, partitionKey=None):
        '''
        Insere um registro único no Kinesis.

        :param record: registro a ser inserido, no formato JSON
        :param streamName: nome do stream onde o registro será inserido
        :param partitionKey: identificador do local onde será inserido (uma forma de identificar o shard)

        :return: resposta do Kinesis à requisição de inserção do registro
        '''
        str_name = ""
        if partitionKey is None or not partitionKey:
            partitionKey = str(uuid.uuid4())

        if streamName is None or not streamName:
            str_name = self.config['aws']['kinesis']['stream_name']
        else:
            str_name = streamName

        return self.conn.put_record(str_name, record, partitionKey)

    def putRecords(self, records, streamName=None):
        '''
        Insere registros em lote (até 500) no Kinesis.

        :param record: array de registros a serem inseridos
        :param streamName: nome do stream onde os registros serão inseridos

        :return: resposta do Kinesis à requisição de inserção dos registros
        '''
        str_name = ""
        
        if streamName is None or not streamName:
            str_name = self.config['aws']['kinesis']['stream_name']
        else:
            str_name = streamName
    
        self.conn.put_records(records, str_name)

    def getShardList(self, streamName=None, getOnlyOpenedShards=True):
        """
        Retorna lista de shards do Kinesis.

        Args:
            streamName (str): nome do stream do qual se recuperará a lista de shards
            getOnlyOpenedShards (bool): indica se serão filtrados somente os shards abertos

        Returns:
            Dict[str,str]: listagem com os dados dos shards encontrados
        """

        str_name = ""
        finalList = None
        if streamName is None or not streamName:
            str_name = self.config['aws']['kinesis']['stream_name']
        else:
            str_name = streamName

        streamDescription = self.conn.describe_stream(str_name)
        shardList = streamDescription['StreamDescription']['Shards']

        if getOnlyOpenedShards:
            #Se quero somente os shards funcionais
            finalList = list()
            for shardInfo in shardList:
                if 'EndingSequenceNumber' not in shardInfo['SequenceNumberRange']:
                    finalList.append(shardInfo)
        else:
            #Preciso de todos. Apenas repasso a lista recebida
            finalList = shardList

        return finalList

    def splitShard(self, streamName=None, shardToSplit=None, newStartHashKey=None):
        '''
        Realiza o split em shard do Kinesis.
        O split é a operação onde são gerados 2 shards a partir de um, onde cada um
        contêm uma porção de keys.

        :param streamName: nome do stream onde será feito o splir em um dos shards
        :param shardToSplit:
        '''
        str_name = None
        if streamName is None or not streamName:
            str_name = self.config['aws']['kinesis']['stream_name']
        else:
            str_name = streamName

        self.conn.split_shard(str_name, shardToSplit, newStartHashKey)

    def mergeShard(self, streamName=None, shardToMerge1=None, shardToMerge2=None):
        '''
        Realiza o merge de 2 shards.
        O processo de merge tem por fim reduzir o número de shards ativos, reduzindo
        assim o valor cobrado ao fim do mês ao cliente. Nesse processo, 2 shards são
        desativados (porém mantidos para fins de acesso aos dados armazenados neles)
        e um novo é criado, com a faixa de hash keys equivalente à junção dos 2 shards
        desativados.
        '''
        pass
