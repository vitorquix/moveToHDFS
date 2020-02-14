import paramiko
import os

##################### SFTP  ############################
def cria_conexao_sftp(host,porta,username,senha):
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
    ssh.connect(host,porta,username,senha)
    sftp = ssh.open_sftp()
    sftp.sshclient = ssh
    return sftp

def copia_arquivo_sftp_para_local(ftp_conn, list_files, remote_dir, local_dir, file_prefix):
    """
    Funcao responsavel por mover o(s) arquivo(s)  do  diretorio de ftp para o  diretorio local
    :param ftp_conn: conexao ftp
    :param remote_dir: diretorio remoto do ftp
    :param local_dir: diretorio local
    :param file_prefix: prefixo do arquivo
    """
    try:
        for filename in list_files:
            if filename.endswith(".csv") and filename.startswith(file_prefix):
                print("COPIANDO ARQUIVO:" + filename)
                ftp_conn.get(remote_dir + filename, local_dir + filename)
        return True
    except Exception as e:
        print "Erro - %s" % e
        return False

########## Local #############
def verifica_existe_arquivos_local(local_dir,file_prefix):
    """
    Funcao que verifica se existem arquivos no diretorio de staging
    :param local_dir:
    :param file_prefix:
    :return:
    """
    lista_arquivos = [files for files in os.listdir(local_dir) if files.endswith(".csv") and files.startswith(file_prefix)]
    return True if len(lista_arquivos) > 0 else False

########## HDFS #############
def copia_arquivo_para_hdfs(local_file, diretorio_hdfs):
    """
    Funcao repsonsavel por copiar o arquivo para o diretorio do hdfs no cluster
    :param diretorio_hdfs: diretorio do hdfs
    :param local_file: arquivo a ser disponibilizado no hdfs
    :return: codigo de retorno da chamada hdfs - 0 sucesso ou 1 erro
    """
    try:
        p1 = subprocess.Popen([" hdfs dfs -put %s" % local_file + diretorio_hdfs],stdout=subprocess.PIPE,shell=True)
        output, errorInfo = p1.communicate()
        return_code = p1.returncode
    return return_code