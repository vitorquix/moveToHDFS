import json
import argparse
import os
import Utils
import pandas as pd
import logging
from datetime import datetime, date, timedelta

############## Configs #####################################################
with open('configs.json', 'r') as configFile:
    config = json.loads(configFile.read())
entrada = config["pipelines"]
arg_pipeline = "load-pdv"

#Configs#####################################################################
logger = logging.getLogger()
logging.basicConfig(
    filename="logs/{}-ATUALIZACAO_PDV.log".format(datetime.today().strftime("%Y-%m-%d")),
    level=logging.INFO,
    format='%(asctime)s: %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
dateNow = (datetime.today()  + timedelta(days=1)).strftime('%Y%m%d')

# pipeline a ser executado###################################################
pipelineInfo = [j for j in entrada if j["pipeline"] == arg_pipeline][0]

############## Abrindo conexao SFTP ########################################
if pipelineInfo["sourceProtocol"] == "sftp":
    sftp = Utils.cria_conexao_sftp(pipelineInfo["host"], pipelineInfo["port"], pipelineInfo["user"],
                                   pipelineInfo["passw"])
    sftp.chdir(pipelineInfo["sourceFolderFTP"])

    # verificar se existe arquivo no SFTP####################################
    if sftp.listdir():
        for files in sftp.listdir():
            if files == "PDV_RAW.csv":
                print("### 1-INICIANDO PROCESSO DE MIGRACAO DA PLANILHA PDV DO SFTP ###")
                # copia do arquivo sftp para local ##################################
                copia_arquivo = Utils.copia_arquivo_sftp_para_local(sftp, pipelineInfo["sourceFolderFTP"], files, pipelineInfo["stagingFolderIn"])
                print("COPIANDO ARQUIVO:" + files)
                logger.info("Migrando arquivo do SFTP: {}".format(files))
                # tratamento do arquivo (Process_PDV_File.py) #######################
                fileConvertido = 'PONTOS_DE_VENDA_WEB_' + dateNow + '.csv'
                print("CONVERTENDO ARQUIVO:" + fileConvertido)
                dfArquivo = pd.read_csv(pipelineInfo["stagingFolderIn"] + "/" + files, header=0, delimiter=";",
                                         encoding='ISO-8859-2')[["UNIDADE", "PDV", "FORNECEDOR", "OPERACAO", "ILHA"]]
                dfArquivo.to_csv(pipelineInfo["stagingFolderOut"] + "/" + fileConvertido, index=None, sep=";", encoding='ISO-8859-2')
                logger.info("Convertendo arquivo: {}".format(fileConvertido))
                # move arquivos para o hdfs ########################################
                print("### 2-INICIANDO PROCESSO DE MIGRACAO PARA HDFS ###")
                returnHdfs = None
                print("COPIANDO ARQUIVO PARA O HDFS: %s " % fileConvertido)
                returnHdfs = Utils.copia_arquivo_para_hdfs(pipelineInfo["stagingFolderOut"], fileConvertido, pipelineInfo["destinationFolderHDFS"])
                logger.info("Migrando arquivo para o HDFS: {}".format(fileConvertido))
                if returnHdfs[0] == 1:
                    print("FALHA AO MOVER ARQUIVO PARA O HDFS: {}".format(returnHdfs[1]))
                    logger.error("Falha ao mover arquivo para o HDFS: {}".format(returnHdfs[1]))
                # deleta os arquivos da staging (entrada e dados) ##################
                path = [pipelineInfo["stagingFolderIn"], pipelineInfo["stagingFolderOut"]]
                if copia_arquivo:
                    print("### 3-INICIANDO PROCESSO DE LIMPEZA DO STAGING ###")
                    for i in path:
                        for filename in os.listdir(i):
                            print("REMOVENDO O ARQUIVO: %s DA STAGING: %s " % (filename, i))
                            os.remove(i + filename)
                            logger.info("Removendo arquivo da staging: {}".format(filename))
                # deleta os arquivos no sftp ########################################
                if returnHdfs[0] == 0:
                    print("### 4-INICIANDO PROCESSO DE LIMPEZA DO SFTP ###")
                    print("REMOVENDO ARQUIVO DO SFTP: %s " % files)
                    sftp.remove(files)
                    logger.info("Removendo arquivo do SFTP: {}".format(files))
            if files != 'PDV_RAW.csv':
                print("### ARQUIVO {} NO SFTP FORA DO FORMATO 'PDV_RAW.CSV' ###".format(files))
                logger.info("Arquivo {} no sftp fora do formato 'PDV_RAW.CSV' ###".format(files))
    else:
        print("### SFTP VAZIO ###")
    # close da sessao
    sftp.close()