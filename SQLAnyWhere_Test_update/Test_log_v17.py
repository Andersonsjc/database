import pyodbc

def print_log(text):
    None #print(text)

def start():
    print('INICIO Processo')
    try:
        max_range = 10
        dsn = 'DSN=DB'

        session = open_connection(dsn)
        if_exists_drop_table(session)      
        create_table(session)

        for i in range(max_range):
            print(f'Inicio - Interacao: {i}')
            
            list_values_to_insert = [ 'A', 'B', 'C', 'D', 'E']
            insert(session, list_values_to_insert)
            
            select_table(session, '*')

            seq = i+1
            if seq <= max_range:
                disctionary = { "seq": seq, "desc": f'desc_{seq+21}', "type": f'type_{seq+21}' }
                update(session, disctionary)

                select_table(session, seq)

            print_log('Fim - Interacao: {i}')
    except Exception as e:
        error = f'ERRO Execute Query: {str(e)}'
        print_log(error)
    finally:
        #if_exists_drop_table(session)
        close_connection(session)
        print_log('Conexao fechada')
        print('FIM Processo')

def close_connection(session):
    if session:
        session.close

def open_connection(dsn):
    conn = pyodbc.connect(dsn)
    session = conn.cursor()
    print_log('Conexao aberta')
    return session

def select_table(session, param):   
    try:
        if param == '*':
            session.execute("SELECT * FROM admin.test_log")
        else:
            session.execute("SELECT * FROM admin.test_log WHERE tstl_seq = ?", (param))

        result = session.fetchall()
        print_log (f'Result:')
        for res in result:
            print_log(f'    {res}')

        print_log('Select ok')
    except Exception as e:
        error = f'  ERRO Execute Query: {str(e)}'
        print_log(error)
        return error

    return 'Executed with sucessfully'

def create_table(session):  
    try:
        session.execute("create table admin.test_log ( tstl_seq  integer not null default autoincrement primary key,  tstl_desc varchar(200),  tstl_type varchar(200) );")
        print_log('Create table ok')
    except Exception as e:
        error = f'  ERRO Execute Query: {str(e)}'
        print_log(error)
        return error

    return 'Executed with sucessfully'

def if_exists_drop_table(session):
    try:
        session.execute("drop table if exists admin.test_log;")
        print_log('Drop table ok')
    except Exception as e:
        error = f'  ERRO Execute Query: {str(e)}'
        print_log(error)
        return error

    return 'Executed with sucessfully'

def update(session, dictionay_values):
    print_log('Begin Update')
    print_log(f'    Interacao: {dictionay_values}')
    v_seq = dictionay_values.get("seq")
    v_desc = dictionay_values.get("desc")
    v_type = dictionay_values.get("type")
    print_log(f'    Seq: {v_seq}')
    print_log(f'    Desc: {v_desc}')
    print_log(f'    Type: {v_type}')

    try:
        session.execute("UPDATE admin.test_log set tstl_desc = ?, tstl_type = ? where tstl_seq = ?", (v_desc, v_type, v_seq))
        session.commit()
    except Exception as e:
        error = f'  ERRO Execute Query: {str(e)}'
        print_log(error)
        return error
    print_log('End Update')

    return 'Executed with sucessfully'

def delete(session, list_values):
    print_log('Begin Delete')
    for value in list_values:
        print_log(f'    Interacao: {value}')

        try:
            session.execute("DELETE admin.test_log where tstl_seq in ?", (value))
            session.commit()
        except Exception as e:
            error = f'  ERRO Execute Query: {str(e)}'
            print_log(error)
            return error
    print_log('End Delete')

    return 'Executed with sucessfully'

def insert(session, list_values):
    print_log('Begin Insert')
    for value in list_values:
        print_log(f'    Interacao: {value}')
        v_description = f'Desc_{value}'
        v_type = f'Type_{value}'
    
        try:
            prepared = ("INSERT INTO admin.test_log (tstl_desc, tstl_type) VALUES (?, ?)")
            session.execute(prepared, ( v_description, v_type))
            session.commit()
        except Exception as e:
            error = f'  ERRO Execute Query: {str(e)}'
            print_log(error)
            return error
    print_log('End Insert')

    return 'Executed with sucessfully'

if __name__ == "__main__":
    start()
