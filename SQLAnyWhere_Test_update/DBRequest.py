import pyodbc
from datetime import datetime, date
from shutil import move, copy
from subprocess import Popen, PIPE, STDOUT
from logging import basicConfig, info, error, DEBUG
from os import path, walk, rename

def start():
    print(f'INICIO: {datetime.now()}')
    dsn_aux = 'DSN=AUX'
    dir_init = 'C:\\Compare\\'
    dir_file_origin = f'\\\\Servername\\C$\\REQUESTS\\' #copio requests.sql.old do servidor de origem
    dir_file_pending = f'{dir_init}\\idle\\' #grava no servidor para processar
    dir_file_done = f'{dir_init}\\done\\' #bkp dos ja processados
    log_dir = f'{dir_init}\\logs\\'
    filename = 'requests.sql.old'
       
    if_log_file_not_exits_then_created( log_dir )
    message_start()

    #verifica o valor sequencial pare renomear o arquivo
    dir_list = [ dir_file_pending, dir_file_done ]
    maxfileseq = get_max_seq( dir_list )
    fileseq = str(int(maxfileseq) + 1).zfill(3)
    print(f'maxfileseq: {fileseq}')

    request = Request(filename, '', True, dsn_aux, '')
    
    #STEP 1 - buscar e mover o arquivo request.sql.old do servidor A para servidor B
    move_file(request, dir_file_origin, dir_file_pending, request.filename)

    #STEP 2 - renomear o arquivo request_old.sql acrescentando um sequencial exemplo: request.sql.old_001
    add_seq_file_name(request, dir_file_pending, fileseq) if request.status else message_error_step_before(request, True)

    #STEP 3 - carregar dados no banco aux
    #execute_sql(request, request.dsn, request.filename_seq, dir_file_pending) if request.status else message_error_step_before(request, False)

    #STEP 4 - mover o arquivo processado para pasta processado
    #move_file(request, dir_file_pending, dir_file_done, request.filename_seq) if request.status else message_error_step_before(request, True)

    #STEP 5 - verificar se existem outros arquivos pendentes de processamento
    file_list = get_other_file_to_process(request, dir_file_pending) if request.status else message_error_step_before(request, False)

    #STEP 6 - se existir arquivos pendentes, processar todos, se um falhar, seguir para o proximo
    if file_list:
        execute_all_sql(request, file_list, dir_file_pending, dir_file_done) 
        
    print(f'Request: {request}')
    #FIM
    message_end(request)

def execute_command(process, command):
    proc = Popen(command, stdout=PIPE, stderr=STDOUT)
    stdout, stderr = proc.communicate()
    message = f'    {stdout} - {stderr}'
    process.write_message_and_write_log(f'   RETORNO => {message}')
    info('')    
    return message

def execute_sql(request, dsn_name, script_name, dir_script):
    cmd_sql = f"C:\\Program Files\\SQL Anywhere 16\\Bin64\\dbisql -c {dsn_name} -d1 -nogui -onerror continue -codepage 1252  call MigrateStatement('{dir_script}{script_name}')"
    request.write_message_and_write_log(f'   COMADO SQL: {cmd_sql}')
    message = execute_command(request, cmd_sql)

    if "Database server not found" in str(message):
        request.write_message_and_write_log(f'   ERRO na execucao do script SQL: {script_name}, executado no DSN: {dsn_name}, verificar se o ODBC esta configurado corretamente.', 'ERROR')
        request.status = False    
    elif ( ("b''" in str(message)) or ("Execution time:" in str(message)) ):
        request.write_message_and_write_log(f'   Script SQL: {script_name}, executado no {dsn_name}, com SUCESSO.')
        request.status = True
    else:
        request.write_message_and_write_log(f'   ERRO na execucao do script SQL: {script_name}, executado no DSN: {dsn_name}, orrigir antes de continuar.', 'ERROR')
        request.status = False

def execute_all_sql(request, file_list, dir_file_pending, dir_file_done):
    try:
        for f_list in file_list:
            execute_sql(request, request.dsn, f_list, dir_file_pending)
            if request.status:
                    #os processados com sucesso, mover o arquivo para pasta processado
                    move_file(request, dir_file_pending, dir_file_done, f_list)
    except Exception as e:
        error = f'ERRO Execute Query: {str(e)}'
        return error

def get_other_file_to_process(request, dir_file_pending):
    file_list = get_files_list(dir_file_pending)
    if file_list:
        request.write_message_and_write_log(f'Arquivos encontrados: {file_list}')
    else:
        request.write_message_and_write_log(f'Nao ha arquivos pendentes de processamento.')

    return file_list

def add_seq_file_name(request, dir_file, fileseq):
    file_dir_name = f'{dir_file}{request.filename}'
    file_name_new = f'{request.filename}_{fileseq}'
    if verify_if_file_exist(file_dir_name):
        request.filename_seq = rename_file(request, dir_file, request.filename, file_name_new)
    else:
        request.write_message_and_write_log('Nenhum arquivo encontrato para acrescentar sequencial')

def rename_file(request, dir_file, file_name_old, file_name_new):
    file_dir_name_old = f'{dir_file}{file_name_old}'
    file_dir_name_new = f'{dir_file}{file_name_new}'
    try:
        rename(file_dir_name_old, file_dir_name_new)
        request.write_message_and_write_log(f'Arquivo renomeado com SUCESSO para: {file_name_new}')
        return file_name_new
    except Exception as e:
        request.status = False
        request.write_message_and_write_log(f'ERRO ao renomear o arquivo {file_dir_name_old} : . {str(e)} ', 'ERROR')
        return ''    

def message_error_step_before(request, status):
    request.write_message_and_write_log('{:>^100}'.format( f'*' ))
    request.write_message_and_write_log('{:=^100}'.format( f'WARNING Verificar o passo anterior.' ))
    request.write_message_and_write_log('{:<^100}'.format( f'*' ))
    request.status = status

def message_start():
    info('{:=^100}'.format( f'INICIO: {datetime.now()} ' ))
    print(f'INICIO: {datetime.now()}')

def message_end(request):
    request.write_message_and_write_log('{:=^100}'.format( f'FIM: {datetime.now()} ' ))
    print(f'FIM: {datetime.now()}')
    quit()

def get_max_seq(dir_list):
    list_file = []
    list_value = []

    for directory in dir_list:  
        list_file.extend(get_files_list(directory))

    for file_value in list_file:
        list_value.append(file_value[17:])

    max_value = max(list_value)
    return max_value

def get_files_list(dir_file):
    for _, _, files in walk(dir_file):
        return files   

def move_file(request, server_origin, server_destiny, filename):
    file_dir_origin = f'{server_origin}{filename}'   
    if verify_if_file_exist(file_dir_origin):
        request.write_message_and_write_log(f'  Arquivo: {file_dir_origin} encontrado, movendo...')      
        try:
            move(file_dir_origin, server_destiny)
        except Exception as e:
            request.write_message_and_write_log(f'  ERRO ao tentar mover o arquivo: {file_dir_origin} para: {server_destiny} => Menssagem de erro: {str(e)}', 'ERROR')
            request.status = False
        else:
            request.write_message_and_write_log(f'  SUCESSO na mover do arquivo: {file_dir_origin} para: {server_destiny} .')
            request.status = True        
    else:
        request.write_message_and_write_log(f'  Arquivo: {file_dir_origin} , nao encontrado para mover.')
        request.status = False

def verify_if_file_exist(file_dir_name):
    return True if path.exists(file_dir_name) else False

def if_log_file_not_exits_then_created(log_dir):
    today = str(date.today())
    log_file = f'REQUEST_{today}.log'
    log_level = DEBUG
    output_format = '%(asctime)s - %(levelname)s : %(message)s'
    filename = f'{log_dir}{log_file}'
    basicConfig(filename=filename, level=log_level, format=output_format)

class Request():
    def __init__( self, filename, message, status, dsn, filename_seq ):
        self.__filename = filename
        self.__message = message
        self.__status = status
        self.__dsn = dsn
        self.__filename_seq = filename_seq

    @property
    def filename(self):
        return self.__filename

    @filename.setter
    def filename(self, filename):
        self.__filename = filename

    @property
    def message(self):
        return self.__message

    @message.setter
    def message(self, message):
        self.__message = message  

    @property
    def status(self):
        return self.__status

    @status.setter
    def status(self, status):
        self.__status = status  

    @property
    def dsn(self):
        return self.__dsn

    @dsn.setter
    def dsn(self, dsn):
        self.__dsn = dsn  

    @property
    def filename_seq(self):
        return self.__filename_seq

    @filename_seq.setter
    def filename_seq(self, filename_seq):
        self.__filename_seq = filename_seq

    def __str__(self):
        return f'''
                    filename: {self.__filename},
                    message: {self.__message},
                    status: {self.__status},
                    dsn: {self.__dsn},
                    filename_seq: {self.__filename_seq}
                '''
    def write_message_and_write_log(self, message, log_type='INFO'):
        error(f'\n{message}') if (log_type == 'ERROR') else info(f'\n{message}')
        self.__message = f'{self.__message}{message} \n'
        print(message)

if __name__ == "__main__":
    start()
