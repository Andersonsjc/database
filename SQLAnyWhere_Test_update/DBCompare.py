"""
Steps: 
1 - Ler query 
2 - Executar query banco old e gravar log no banco aux
3 - Executar query banco new e gravar log no banco aux
"""
import pyodbc
from datetime import datetime
import time as t

def start():
    print(f'INICIO: {datetime.now()}')
    dsn_old = 'DSN=DBOLD'
    dsn_new = 'DSN=DBNEW'
    dsn_aux = 'DSN=AUX'

    for i in range(40): 
        print(f'Interaction: {i} - {datetime.now()}')
        result = readquery(dsn_aux)

        for res in result:
            #print (f'SAIDA: {res}')
            #0-dbcm_seq, 1-dbcm_query, 2-dbcm_dbst_seq_old, 3-dbcm_time_old, 4-dbcm_error_old, 5-dbcm_dbst_seq_new, 6-dbcm_time_new, 7-dbcm_error_new
            dbcompare = DBCompare(res[0],res[1],res[2],res[3],res[4],res[5],res[6],res[7])
            
            db_old(dbcompare, dsn_old, dsn_aux)
            
            db_new(dbcompare, dsn_new, dsn_aux)
        
            #print(f'Compare: {dbcompare}')
        
    print(f'FIM: {datetime.now()}')

#Step: 1 - Ler query
def readquery(dsn_aux):
    query = f'''select top 1000
                    dbcm_seq,
                    dbcm_query,
                    dbcm_dbst_seq_old, 
                    dbcm_time_old, 
                    dbcm_error_old, 
                    dbcm_dbst_seq_new, 
                    dbcm_time_new, 
                    dbcm_error_new
                from dbcompare
            where dbcm_dbst_seq_old = 1
                or dbcm_dbst_seq_new = 1
                ;'''

    result = execute_query(query, dsn_aux)
    #print(f"queries para testar: {result}")

    if not result:
        print('{:=^100}'.format(f'Nao foi encontrado novas queries em Idle para testar!'))
        quit()

    return result

#Step: 2 - Executar query banco old e gravar log
def db_old(dbcompare, dsn_old, dsn_aux):
    dbcompare.status_old = 2 #Running
    update(dbcompare, dsn_aux)
    #t.sleep(10) #usado para testar a gravacao do status 2 no banco
    time_init = datetime.now()
    result = execute_query(dbcompare.query, dsn_old)
    time_finish = datetime.now()

    if 'ERRO Execute Query' in result:
        result = result.replace("'"," ") #Tratamento para demover aspas simples, causa erro no update
        dbcompare.message_old = result
        dbcompare.status_old = 4 #Error
        dbcompare.time_old = 0
        #print(f'exception old dbcompare: {dbcompare}')
    else:
        running_time = time_finish - time_init
        dbcompare.time_old = (f'{running_time}')
        dbcompare.status_old = 3 #Done
        dbcompare.message_old = '' #Caso exista limpa mensagem anterior
        #print (f'result: {result}')

    update(dbcompare, dsn_aux)

#Step: 3 - Executar query banco new e gravar log
def db_new(dbcompare, dsn_new, dsn_aux):
    dbcompare.status_new = 2 #Running
    update(dbcompare, dsn_aux)
    time_init = datetime.now()
    result = execute_query(dbcompare.query, dsn_new)
    time_finish = datetime.now()

    if 'ERRO Execute Query' in result:
        result = result.replace("'"," ")
        dbcompare.message_new = result
        dbcompare.status_new = 4 #Error
        dbcompare.time_new = 0
        #print(f'exception new dbcompare: {dbcompare}')
    else:
        running_time = time_finish - time_init
        dbcompare.time_new = (f'{running_time}')
        dbcompare.status_new = 3 #Done
        dbcompare.message_new = '' #Caso exista limpa mensagem anterior
        #print (f'result: {result}')

    update(dbcompare, dsn_aux)

#update generico baseado nas informacoes do class DBCompare()
def update(dbcompare, dsn_aux):
    query = f'''update dbcompare 
                    set dbcm_dbst_seq_old = {dbcompare.status_old},
                        dbcm_error_old = '{dbcompare.message_old}',
                        dbcm_time_old = '{dbcompare.time_old}',
                        dbcm_dbst_seq_new = {dbcompare.status_new},
                        dbcm_error_new = '{dbcompare.message_new}',
                        dbcm_time_new = '{dbcompare.time_new}'
                    where dbcm_seq = {dbcompare.seq};
            '''

    query = query.replace("None", "NULL") # Python retorna None, no banco quero gravar como NULL
    #print(f'Query Update: {query}')
    con = pyodbc.connect(dsn_aux)
    cursor = con.cursor()
    try:
        cursor.execute(query)
        con.commit()
    except Exception as e:
        error = f'ERRO UPDATE: {str(e)}'
        print(error)
        quit() #Se nao conseguir gravar no banco auxiliar, parar o processo e exibir o erro
    finally:
        if cursor:
            cursor.close

#Execucao de querys de forma generica, erro pode ser tratado pela funcao que invocou
def execute_query(query, dsn_name):
    con = pyodbc.connect(dsn_name)
    cursor = con.cursor()
 
    try:
        cursor.execute(query)
        if query.startswith('update') or query.startswith('delete') or query.startswith('insert'):
            con.commit()
            return 'Commit ok'
        else:
            result = cursor.fetchall()
            return result
    except Exception as e:
        error = f'ERRO Execute Query: {str(e)}'
        #print(error)
        return error
    finally:
        if cursor:
            cursor.close

#classe com os atributos do registro a ser lido e gravado no banco aux
class DBCompare():
    def __init__(self, seq, query, status_old, time_old, message_old, status_new, time_new, message_new):
        self.__seq = seq
        self.__query = query
        self.__status_old = status_old
        self.__time_old = time_old
        self.__message_old = message_old
        self.__status_new = status_new
        self.__time_new = time_new
        self.__message_new = message_new

    @property
    def seq(self):
        return self.__seq

    @seq.setter
    def seq(self, seq):
        self.__seq = seq

    @property
    def query(self):
        return self.__query

    @query.setter
    def query(self, query):
        self.__query = query

    @property
    def status_old(self):
        return self.__status_old
    
    @status_old.setter
    def status_old(self, status_old):
        self.__status_old = status_old

    @property
    def time_old(self):
        return self.__time_old
    
    @time_old.setter
    def time_old(self, time_old):
        self.__time_old = time_old

    @property
    def message_old(self):
        return self.__message_old

    @message_old.setter
    def message_old(self, message_old):
        self.__message_old = message_old

    @property
    def status_new(self):
        return self.__status_new

    @status_new.setter
    def status_new(self, status_new):
        self.__status_new = status_new

    @property
    def time_new(self):
        return self.__time_new
    
    @time_new.setter
    def time_new(self, time_new):
        self.__time_new = time_new

    @property
    def message_new(self):
        return self.__message_new

    @message_new.setter
    def message_new(self, message_new):
        self.__message_new = message_new

    def __str__(self):
        return f'''
                    seq: {self.seq},
                    query: {self.query}, 
                    status_old: {self.status_old}, 
                    time_old: {self.time_old}, 
                    message_old: {self.message_old}, 
                    status_new = {self.status_new},
                    time_new = {self.time_new},
                    message_new = {self.message_new}
                '''

if __name__ == "__main__":
    start()
