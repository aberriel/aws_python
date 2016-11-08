#!/usr/bin/env python
# -*- coding: utf-8 -*-

from configuration import Configuration
from datetime import datetime
from datetime import timedelta
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText

import email
import json
import smtplib


class Mail:
    '''
    Métodos para validação e envio de e-mail
    '''
    # Cliente SMTP para envio de e-mail
    smtp_client = None
    # Instância das configurações da aplicação
    config = None
    # Remetente da mensagem
    mail_from = None
    # Destinatário da mensagem
    mail_to = []
    # Assunto da mensagem
    mail_subject = None
    # Conteúdo (corpo) da mensagem
    mail_msg = None

    def __init__(self, origin=None, destination_arr=None, subject=None, message=None):
        '''
        Construtor
        Inicializa o cliente de e-mail.

        :param origin: Remetente da mensagem
        :param destination: Destinatário da mensagem
        :param subject: Assunto da mensagem
        :param message: Conteúdo da mensagem em si
        '''
        configWrapper = Configuration()
        self.config = configWrapper.getConfigurationFromFile()
        self.smtp_client = smtplib.SMTP(configWrapper.getFullSmtpHost())
        self.smtp_client.starttls()
        self.smtp_client.login(self.config['mail']['user'], self.config['mail']['password'])


        if origin is None or not origin:
            self.mail_from = self.config['mail']['mail_from']
        else:
            self.mail_from = origin

        if destination_arr is None or not destination:
            self.mail_to = self.config['mail']['mail_to']
        else:
            self.mail_to = destination_arr

        if subject is not None:
            self.mail_subject = subject

        if message is not None:
            self.mail_msg = message

    def sendMail(self, msgFormat='plain', subject=None, message=None):
    	'''
    	Realiza o envio do e-mail.

    	:param subject: Assunto da mensagem enviada.
    	:param message: Conteúdo da mensagem em si.
    	'''
        if message is not None:
            self.mail_msg = message

        if subject is not None:
            self.mail_subject = subject

        if self.mail_to is not None:
            for mail_dest in self.mail_to:
                msg_template = MIMEMultipart()
                msg_template['From'] = self.mail_from
                msg_template['To'] = mail_dest['addr']
                msg_template['Subject'] = self.mail_subject
                body = self.mail_msg
                msg_template.attach(MIMEText(body, msgFormat))
                text = msg_template.as_string()
                self.smtp_client.sendmail(self.mail_from, self.mail_to, text)

    def __del__(self):
    	'''
    	Destrutor.
    	Finaliza a conexão com o servidor SMTP.
    	'''
        if self.smtp_client is not None:
            self.smtp_client.quit()
