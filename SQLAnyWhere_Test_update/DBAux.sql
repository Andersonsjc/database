-----------------------------------------------------------------------
-- Script que cria as tabelas do banco AUX para receber as queries
-- contidas nos arquivos request.sql
-----------------------------------------------------------------------
if exists (select 1 from systable where table_name = 'dbcompare_status') then
	drop table dbcompare_status;
end if;

if exists (select 1 from systable where table_name = 'dbcompare') then
	drop table dbcompare;
end if;

create table dbcompare_status
(
    dbst_seq integer not null default autoincrement primary key,
    dbst_desc varchar(10) not null unique
);
input into dbcompare_status format ascii;
1,'Idle'
2,'Running'
3,'Done'
4,'Error'
END
commit;

create table dbcompare
(
    dbcm_seq integer not null default autoincrement primary key,
	dbcm_hash varchar(255) not null,
    dbcm_dbst_seq_old integer not null references dbcompare_status(dbst_seq),
    dbcm_error_old long varchar null default null,    
    dbcm_time_old varchar(20) null default 0,
    dbcm_dbst_seq_new integer not null references dbcompare_status(dbst_seq),
	dbcm_error_new long varchar null default null,
	dbcm_time_new varchar(20) null default 0,
	dbcm_query long varchar not null,
	dbcm_file_seq integer null
);

CREATE INDEX dbcompare_hash ON dbcompare ( dbcm_hash ASC );
CREATE INDEX dbcompare_file_seq ON dbcompare ( dbcm_file_seq ASC );

/*
Insert de Teste

insert into dbcompare(dbcm_hash, dbcm_dbst_seq_old, dbcm_error_old, dbcm_dbst_seq_new, dbcm_error_new, dbcm_query)
values('9d16c3104b8b56d341d18d931f64ddb3',1,null,1,null,'select top 10 teste_seq, teste_desc from teste where teste_seq = 123;');

insert into dbcompare(dbcm_hash, dbcm_dbst_seq_old, dbcm_error_old, dbcm_dbst_seq_new, dbcm_error_new, dbcm_query)
values('4ec06b03ea6a3d67ed06dfb57b74d81e',1,null,1,null,'select * from teste_log;');

commit;

select  d.dbcm_seq, 
        so.dbst_desc, 
        d.dbcm_error_old, 
        d.dbcm_time_old, 
        sn.dbst_desc, 
        d.dbcm_error_new, 
        d.dbcm_time_new,
        d.dbcm_query,
		d.dbcm_file_seq
  from dbcompare d
  join dbcompare_status so
    on d.dbcm_dbst_seq_old = so.dbst_seq
  join dbcompare_status sn
    on d.dbcm_dbst_seq_new = sn.dbst_seq;

*/











