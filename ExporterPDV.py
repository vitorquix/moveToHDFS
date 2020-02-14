import json
import argparse
import os
import Utils
import pandas as pd

############## Inicia processo #############################################
with open('configs.json', 'r') as configFile:
    config = json.loads(configFile.read())
entrada = config["pipelines"]
arg_pipeline = "load-pdv"

# pipeline a ser executado###################################################
pipelineInfo = [j for j in entrada if j["pipeline"] == arg_pipeline][0]

############## Abrindo conexao SFTP ########################################
if pipelineInfo["sourceProtocol"] == "sftp":
    sftp = Utils.cria_conexao_sftp(pipelineInfo["host"], pipelineInfo["port"], pipelineInfo["user"],
                                   pipelineInfo["passw"])
    sftp.chdir(pipelineInfo["sourceFolderFTP"])

    # verificar se existe arquivo no SFTP####################################
    if sftp.listdir():
        print("### 1-INICIANDO PROCESSO DE MIGRACAO DA PLANILHA PDV DO SFTP ###")
        # copia do arquivo sftp para local ##################################
        copia_arquivo = Utils.copia_arquivo_sftp_para_local(sftp, sftp.listdir(), pipelineInfo["sourceFolderFTP"],
                                                            pipelineInfo["folderConversao"],
                                                            pipelineInfo["sourceFilePrefix"])

        # tratamento do arquivo (Process_PDV_File.py) #######################
        lista_arquivos = [files for files in os.listdir(pipelineInfo["folderConversao"]) if
                          files.endswith(".csv") and files.startswith('PDV_RAW')]
        for arquivo in lista_arquivos:
            print("CONVERTENDO ARQUIVO:" + arquivo)
            dfArquivo = pd.read_csv(pipelineInfo["folderConversao"] + "/" + arquivo, header=0, delimiter=";",
                                    encoding='ISO-8859-2')[["UNIDADE", "PDV", "FORNECEDOR", "OPERACAO", "ILHA"]]
            dfArquivo.to_csv(
                pipelineInfo["destinationFolderConversao"] + "/" + arquivo.replace('PDV_RAW', 'PONTOS_DE_VENDA'),
                index=None, sep=";", encoding='ISO-8859-2')

        # move arquivos para o hdfs ########################################
        print("### 2-INICIANDO PROCESSO DE MIGRACAO PARA HDFS ###")
        returnHdfs = None
        for files in os.listdir(pipelineInfo["destinationFolderConversao"]):
            print("COPIANDO ARQUIVO PARA O HDFS: %s " % files)
            returnHdfs = Utils.copia_arquivo_para_hdfs(pipelineInfo["destinationFolderConversao"], files,
                                                       pipelineInfo["destinationFolderHDFS"])

        # deleta os arquivos da staging (entrada e dados) ##################
        path = [pipelineInfo["folderConversao"], pipelineInfo["destinationFolderConversao"]]
        if copia_arquivo:
            print("### 3-INICIANDO PROCESSO DE LIMPEZA DO STAGING ###")
            for i in path:
                for filename in os.listdir(i):
                    print("REMOVENDO O ARQUIVO: %s DA STAGING: %s " % (filename, i))
                    os.remove(i + filename)

        # deleta os arquivos no ftp ########################################
        if returnHdfs == 0:
            print("### 4-INICIANDO PROCESSO DE LIMPEZA DO SFTP ###")
            filesInRemoteArtifacts = sftp.listdir()
            for file in filesInRemoteArtifacts:
                print("REMOVENDO ARQUIVO DO SFTP: %s " % file)
                sftp.remove(file)

    else:
        print("### NAO EXISTE ARQUIVO NO SFTP ###")
    # close da sessao
    sftp.close()
