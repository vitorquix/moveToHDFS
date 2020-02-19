import ftplib
import paramiko
import subprocess
import os

# path de entrada para arquivos de cargas dos projetos
HDFS_CARGAS_PATH = "/oi/digital/stage/ctl_cargas/"
STAGING_CARGAS_PATH = "/data2/digital_stage/app/develop/scala/Load-digital/nsassis/dados/"
FILE_ALL_WILDCARD = "*"


##################### SFTP  ############################

def cria_conexao_sftp(host, porta, username, senha):
    """
    Funcao repsonsavel por criar a conexao sftp
    :param host:  nome  ou ip do servidor sftp
    :param porta: porta associada ao servico
    :param username: nome do usuario da conexao
    :param senha: senha do usuario
    :return: conexao sftp
    """
    porta = int(porta)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, porta, username, senha)
    sftp = ssh.open_sftp()
    sftp.sshclient = ssh
    return sftp


def copia_arquivo_sftp_para_local(ftp_conn, remote_dir, filename, local_dir):
    """
    Funcao responsavel por mover o(s) arquivo(s)  do  diretorio de ftp para o  diretorio local
    :param ftp_conn: conexao ftp
    :param remote_dir: diretorio remoto do ftp
    :param local_dir: diretorio local
    :param file_prefix: prefixo do arquivo
    """
    try:
        ftp_conn.get(remote_dir + filename, local_dir + filename)
        return True
    except Exception as e:
        print "Erro - %s" % e
        return False


############### FTP ###################################

# Funcao de criacao da conexao ftp
def cria_conexao_ftp(host, username, senha):
    """
    Funcao responsavel por criar a conexao ftp
    :param host: nome ou ip do servidor ftp
    :param username: nome do usuario da conexao
    :param senha: senha do usuario
    :return: conexao ftp
    """
    ftpObj = ftplib.FTP(host)
    ftpObj.login(username, senha)
    return ftpObj


def recupera_ftp_diretorios(ftp_conn, parentFolder):
    """
    Funcao responsavel pela captura da lista de subdiretorios do diretorio parent
    :param ftp_conn: conexao ftp
    :param parentFolder: diretorio no qual serao procurados os subdiretorios
    :return: lista de diretorios
    """
    ftp_conn.cwd(parentFolder)
    ret_list = []
    ftp_conn.retrlines('LIST', ret_list.append)
    folders = [x.replace(" ", "").split("<DIR>")[-1] for x in ret_list if "<DIR>" in x]
    return folders


def verifica_ftp_diretorio_existe(ftp_conn, parentFolder, folder_name):
    """
    Funcao responsavel pela verificacao de existencia de um subdiretorio X dado um dirtorio Y
    :param ftp_conn: conexao ftp
    :param parentFolder: caminho do diretorio a ser analisado
    :param folder_name: nome do subdiretorio a ser procurado
    :return: Booleano referente a existencia ou nao do subdiretorio
    """
    folders = recupera_ftp_diretorios(ftp_conn, parentFolder)
    return folder_name in folders


def cria_ftp_diretorio(ftp_conn, parentFolder, new_folder_name):
    """
    Funcao responsavel pela criacao de um subdiretorio X em um diretorio Y no servidor FTP
    :param ftp_conn: conexao ftp
    :param parentFolder: caminho do diretorio a ser analisado
    :param new_folder_name: nome do subdiretorio a ser criado
    :return: codigo de retorno da operacao make dir
    """
    codigo_retorno = ftp_conn.mkd(parentFolder + "/" + new_folder_name)
    print "Diretorio criado no servidor FTP : %s/%s " % (parentFolder, new_folder_name)
    return codigo_retorno


def copia_arquivo_local_para_ftp(ftp_conn, local_dir, file_prefix, pipeline_date, ftp_dir):
    """
    Funcao responsavel por mover o(s) arquivo(s)  do  diretorio de staging para o  diretorio ftp
    :param ftp_conn: conexao ftp
    :param local_dir: diretorio de staging
    :param file_prefix: prefixo do nome do arquivo
    :param ftp_dir: diretorio ftp de destino
    """
    try:
        for filename in os.listdir(local_dir):
            if filename.endswith(".csv") and filename.startswith(file_prefix) and (pipeline_date in filename):
                ftp_conn.cwd(ftp_dir + "/" + file_prefix)
                ftp_conn.storbinary('STOR %s' % filename, open(local_dir + "/" + filename, 'rb'))
                print "Arquivo " + filename + " enviado para " + ftp_dir + "/" + file_prefix + "/" + filename
        return True
    except Exception as e:
        print "Erro - %s" % e
        return False


################  HDFS ###############################
def verifica_safe_hdfs(hdfsPath):
    """
    Funcao responsavel por verificar se  o path em questao esta na estrutura padrao de cargas
    :param hdfsPath:
    :return: Boolean
    """
    if (hdfsPath.find(HDFS_CARGAS_PATH) == 0):
        return True
    else:
        return False


def verifica_hdfs_diretorio_existe(diretorio_hdfs):
    """
    Funcao repsonsavel por verificar se o diretorio do hdfs  informado existe no cluster
    :param diretorio_hdfs:
    :return: codigo de retorno da chamada hdfs - 0 sucesso ou 1 erro
    """
    print "Executando comando de verificacao de diretorio no hdfs:  hdfs dfs -test -e %s" % diretorio_hdfs
    p1 = subprocess.Popen([" hdfs dfs -test -e %s" % diretorio_hdfs], stdout=subprocess.PIPE, shell=True)
    output, errorInfo = p1.communicate()
    return_code = p1.returncode
    print "Fim da Verificacao de diretorio . [RETURN: %s, OUT: %s, ERROR: %s]" % (return_code, output, errorInfo)
    return return_code


def copiar_hdfs_files_para_local_staging(diretorio_origem_hdfs, file_prefix, pipeline_date, diretorio_destino):
    """
    Funcao responsavel pela copia dos arquivos do hdfs para  um diretorio local (client)
    :param diretorio_origem_hdfs:  diretorio de  origem dos arquivos
    :param hdfs_padroes_arquivos: pattern de nomenclatura  do arquivo
    :param diretorio_destino: diretorio local (client) de  detino dos arquivos copiados
    :return: codigo de retorno da execuao - 0 sucesso ou 1  erro
    """

    # remocao do identificador  unicode e  criacao de string unica com os  itens da lista
    print "Executando comando de copia de arquivos:  hdfs dfs -copyToLocal %s/%s  %s" \
          % (
          diretorio_origem_hdfs, file_prefix + FILE_ALL_WILDCARD + pipeline_date + FILE_ALL_WILDCARD, diretorio_destino)
    p1 = subprocess.Popen(["hdfs dfs -copyToLocal %s/%s  %s"
                           % (
                           diretorio_origem_hdfs, file_prefix + FILE_ALL_WILDCARD + pipeline_date + FILE_ALL_WILDCARD,
                           diretorio_destino)], stdout=subprocess.PIPE, shell=True)
    output, errorInfo = p1.communicate()
    return_code = p1.returncode
    print "Fim da copia de arquivos . [RETURN: %s, OUT: %s, ERROR: %s]" % (return_code, output, errorInfo)
    return return_code


def mover_hdfs_files(diretorio_origem_hdfs, file_prefix, pipeline_date, diretorio_destino_hdfs):
    """
    Funcao responsavel pela movimentacao de arwquivos entre diretorios do hdfs
    :param diretorio_origem_hdfs:
    :param hdfs_padroes_arquivos:
    :param diretorio_historico_hdfs:
    :return:
    """
    # strHDFSPadroesArquivos = ",".join([str(arq) for arq in hdfs_padroes_arquivos])  # remocao do identificador  unicode e  criacao de string unica com os  itens da lista
    # strHDFSPadroesArquivos = str(file_prefix)  # remocao do identificador  unicode e  criacao de string unica com os  itens da lista
    print "Executando comando de movimentacao de arquivos no hdfs:  hdfs dfs -mv %s/{%s}  %s" \
          % (diretorio_origem_hdfs, file_prefix + FILE_ALL_WILDCARD + pipeline_date + FILE_ALL_WILDCARD,
             diretorio_destino_hdfs)
    p1 = subprocess.Popen(["hdfs dfs -mv %s/{%s}  %s"
                           % (
                           diretorio_origem_hdfs, file_prefix + FILE_ALL_WILDCARD + pipeline_date + FILE_ALL_WILDCARD,
                           diretorio_destino_hdfs)], stdout=subprocess.PIPE, shell=True)
    output, errorInfo = p1.communicate()
    return_code = p1.returncode
    print "Fim da movimentacao de arquivos . [RETURN: %s, OUT: %s, ERROR: %s]" % (return_code, output, errorInfo)
    return return_code


def copia_arquivo_para_hdfs(local_dir, local_file, diretorio_hdfs):
    """
    Funcao responsavel por copiar o arquivo para o diretorio do hdfs no cluster
    :param diretorio_hdfs: diretorio do hdfs
    :param local_file: arquivo a ser disponibilizado no hdfs
    :return: codigo de retorno da chamada hdfs - 0 sucesso ou 1 erro
    """
    p1 = subprocess.Popen(["hdfs dfs -put %s/%s %s" % (local_dir, local_file, diretorio_hdfs)], stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, shell=True)
    output, errorInfo = p1.communicate()
    return_code = p1.returncode
    return return_code, output


########## Local #############

def verifica_existe_arquivos_local(local_dir, file_prefix, pipeline_date):
    """
    Funcao que verifica se existem arquivos no diretorio de staging
    :param local_dir:
    :param file_prefix:
    :param pipeline_date:
    :return:
    """
    lista_arquivos = [files for files in os.listdir(local_dir) if
                      files.endswith(".csv") and files.startswith(file_prefix) and (pipeline_date in files)]
    return True if len(lista_arquivos) > 0 else False


def limpa_staging(diretorio_staging, file_prefix, pipeline_date):
    if (verifica_existe_arquivos_local(diretorio_staging, file_prefix, pipeline_date)):
        print "Executando comando de limpeza de staging : rm  %s/%s*" % (diretorio_staging, file_prefix)
        p1 = subprocess.Popen(["rm  %s/%s*" % (diretorio_staging, file_prefix)], shell=True)
        output, errorInfo = p1.communicate()
        return_code = p1.returncode
        print "Fim da limpeza do diretorio de staging . [RETURN: %s, OUT: %s, ERROR: %s]" % (
        return_code, output, errorInfo)
        return return_code
    else:
        print "Diretorio de staging vazio"
        return 0


def verifica_safe_local_staging(local_staging):
    """
    Funcao responsavel por verificar se  o path em questao esta na estrutura padrao de staging
    :param local_staging:
    :return: Boolean
    """
    if (local_staging.find(STAGING_CARGAS_PATH) == 0):
        return True
    else:
        return False