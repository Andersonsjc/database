# -*- coding: UTF-8 -*-
from os import getcwd, walk
from re import search, match
from hashlib import sha256
from datetime import datetime
from socket import gethostname

def start():
    print('{:-^50}'.format('BEGIN PROCESS'))
    path_ini = getcwd()
    host_name = gethostname()
    print(f'Hostname = {host_name}')

    #i = 0
    list_hash = []
    list_file = []

    if host_name == 'LOCALHOST':
        dir_file_done = f'{path_ini}\\done'
    else:
        dir_file_done = f'C:\\done'
    #print(f'Path files = {dir_file_done}')

    list_file = get_list_files(dir_file_done)
    #list_file = ['requests.sql.old']
    list_file.sort()

    info = information(len(list_file))

    for file_name in list_file:
        path_file = f'{dir_file_done}\{file_name}'
        print(f'{str(datetime.now())} - File: {file_name}') 
        try:
            with open(path_file,'r', encoding='ISO-8859-1') as file_open:
                for line in file_open:
                    #i += 1
                    try:
                        #print(f'{line}',end="")
                        if (test_line(line)):
                            line_split = line.split(",")
                            #print('-'*100)
                            #print(f'line slipt: {line_split}')
                            if (is_query(line_split)):
                                new_line = concate_all(line_split)
                                #print(f'new_line: {new_line}')
                                if (new_hash(info, new_line, list_hash)):
                                    with_prepare_parameter(info, new_line)
                    except IndexError as ie:
                        #rint(f'ERRO Line: {ie}')
                        continue
                    except Exception as e:
                        print(f'ERRO Line: {e}')

        except Exception as e:
            print(f'ERRO File: {e}')


    print(info)
    print(f'    With :? Percent = {round(info.qtd_with_prepare/info.qtd_total,2)*100}%')
    print(f'    Without :? Percent = {round(info.qtd_without_prepare/info.qtd_total,2)*100}%')   
    print('')
    #print(f'Extra information:')
    #print(f'    Without Any Parameter = {info.qtd_without_parameter}')
    #print(f'    Without Parameter Percent = {round(info.qtd_without_parameter/info.qtd_total,2)*100}%')
    #print('')
    #print(f'    Qtd Call Procedure = {info.qtd_call_prodecure}')
    print(f'    Qtd Select = {info.qtd_select}')
    print(f'    Qtd Update = {info.qtd_update}')
    print(f'    Qtd Insert = {info.qtd_insert}')
    print(f'    Qtd Delete = {info.qtd_delete}')
    print('')
    #print(f'    Call Prodedure Percent = {round(info.qtd_call_prodecure/info.qtd_total,2)*100}%')
    print(f'    SELECT Percent = {round(info.qtd_select/info.qtd_total,2)*100}%') 
    print(f'    UPDATE Percent = {round(info.qtd_update/info.qtd_total,2)*100}%')
    print(f'    INSERT Percent = {round(info.qtd_insert/info.qtd_total,2)*100}%')
    print(f'    DELETE Percent = {round(info.qtd_delete/info.qtd_total,2)*100}%')
    print('')
    print('{:-^50}'.format('END PROCESS'))        

def get_list_files(dir_file_done):
    for _, _, files in walk(dir_file_done):
        return files  

def with_prepare_parameter(info, line):
    if ':?' in line:
        #print(f'with prepare : {line}')
        info.add_with_prepare()
    else:
        #print(f'without prepare : {line}')
        info.add_without_prepare()

    if line.startswith(',select'):
        #print(f'Select : {line}')
        info.add_select()
    elif line.startswith(',update'):
        #print(f'Update : {line}')
        info.add_update()
    elif line.startswith(',insert'):
        #print(f'Insert : {line}')
        info.add_insert()
    elif line.startswith(',delete'):
        #print(f'Delete : {line}')
        info.add_delete()
    elif line.startswith(',call'):
        #print(f'Call Procedure : {line}')
        info.add_call_procedure()


def with_prepare_paramete_full(info, line):
    line = line.replace("where 1 = 1","where ")
    if ':?' in line:
        #print(f'with prepare :? : {line}')
        info.add_with_prepare()
    elif search(' in\s*\([\s\'\w]+',line):
        #print(f'without prepare using IN : {line}')
        info.add_without_prepare()
    elif search(',insert\sinto[\s*\w+]',line): 
        #print(f'without prepare insert values : {line}')
        info.add_without_prepare()
    elif search('like\s(\'[\%\w+]\w+)',line): 
        #print(f'without prepare like : {line}')
        info.add_without_prepare()
    elif search('\(\s*([\w\'\" ]\w+.\w+[\'\" ,\)])',line):
        #select "admin"."uswg_path"(7731) as "uswg_path
        #select "admin"."user_prog_fte"(18231,0,0) as "fte"
        #call "instrumentation"."Configuration_ParameterList"(38889)
        #print(f'without prepare with one or more value(s) : {line}')
        info.add_without_prepare()
    elif search('between\s*(\'\w+\')\s*and\s*(\'\w+\')',line):
        #select "risk_seq",    "risk_desc",    "risk_start",    "risk_end",    "risk_text"    from "engineering"."risk_assessment"    where "risk_start"    between '20201115'    and '20201128'    order by "risk_start" asc
        #print(f'without using between : {line}')
        info.add_without_prepare()
    elif search('\((\'\w+[\w-]\w+[\w-]\w+[\w-]\s\w+\:\w+\:\w+[\'\.][\w\',][\w\'\"])',line):
        #select "admin"."GetNetHours"('20201120 08:00:02','20201120 18:00:02',2) as "hour
        #select "admin"."GetNetDays"('2020-11-06 05:45:00.0',"now"(),1) as "post_notification_days"
        #print(f'without with datetime : {line}')
        info.add_without_prepare()
    elif search('[=><][\s*\w\']+',line):
        #select *    from "certification"."avop_deliverable_dates"    where "avdt_opev_seq" = '2526'  
        #print(f'without prepare with comparation : {line}')
        info.add_without_prepare()
    else:
        #select *   from "safety"."type"
        #print(f'without any parameter : {line}')
        info.add_without_parameter()

def new_hash(info, new_line, list_hash):
    h = sha256()
    h.update(new_line.encode('utf-8'))
    #print (f'hexdigest: {h.hexdigest()}')
    if h.hexdigest() not in list_hash:
        list_hash.append(h.hexdigest())
        info.add_qtd_total()
        return True
    else:
        return False

def is_query(line_split):
    #return True if (match(r'^select |insert |delete |update | call', line_split[4])) else False
    return True if (match(r'^select |insert |delete |update ', line_split[4])) else False

def test_line(line):
    return True if (line.split(",")[3] == 'PREPARE') else False

def concate_all(line_split):
    len_line = len(line_split)
    concate_line = ''
    i=4
    while(i <= len_line):
        concate_line = concate_line + ',' + line_split[i]
        i +=1
        if i == len_line:
            return concate_line

class information():
    def __init__(self, qtd_file):
        self._qtd_with_prepare = 0
        self._qtd_without_prepare = 0
        self._qtd_without_parameter = 0
        self._qtd_call_prodecure = 0
        self._qtd_insert = 0
        self._qtd_update = 0
        self._qtd_delete = 0
        self._qtd_select = 0
        self._qtd_total = 0
        self._qtd_file = qtd_file

    @property
    def qtd_select(self):
        return self._qtd_select

    def add_select(self):
        self._qtd_select += 1

    @property
    def qtd_delete(self):
        return self._qtd_delete

    def add_delete(self):
        self._qtd_delete += 1

    @property
    def qtd_update(self):
        return self._qtd_update

    def add_update(self):
        self._qtd_update += 1

    @property
    def qtd_insert(self):
        return self._qtd_insert

    def add_insert(self):
        self._qtd_insert += 1

    @property
    def qtd_call_prodecure(self):
        return self._qtd_call_prodecure

    def add_call_procedure(self):
        self._qtd_call_prodecure += 1

    @property
    def qtd_with_prepare(self):
        return self._qtd_with_prepare

    def add_with_prepare(self):
        self._qtd_with_prepare += 1

    @property
    def qtd_without_prepare(self):
        return self._qtd_without_prepare

    def add_without_prepare(self):
        self._qtd_without_prepare += 1

    @property
    def qtd_without_parameter(self):
        return self._qtd_without_parameter

    def add_without_parameter(self):
        self._qtd_without_parameter += 1

    @property
    def qtd_total(self):
        return self._qtd_total

    def add_qtd_total(self):
        self._qtd_total += 1

    @property
    def qtd_file(self):
        return self._qtd_file

    @qtd_file.setter
    def qtd_file(self, qtd_file):
        self._qtd_file = qtd_file

    def __str__(self):
        return f''' <<====== RESULT ======>>
    Qtd File Read = {self.qtd_file}
    Total Analysed = {self.qtd_total}
    With :? = {self.qtd_with_prepare} 
    Without :? = {self.qtd_without_prepare} 
                '''

if __name__ == "__main__":
    start()