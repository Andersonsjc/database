---------------------------------------------------------------------------------
--
-- Migro queries de um arquivo requests.sql para tabelas afim de futura comparação
-- entre versões de bancos de dados
--
-- Exemplo:
-- 	call MigrateStatement('C:\\DBCompare\\requests.sql.old.xxx');
--------------------------------------------------------------------------------

if exists (select proc_name from SYS.SYSPROCEDURE where proc_name='MigrateStatement') then
    DROP PROCEDURE MigrateStatement;
end if;

CREATE PROCEDURE MigrateStatement
(
	@sFile_path	long varchar
)

BEGIN

  --------------------------------------------------------------------------------
  -- VARIÁVEIS
  --------------------------------------------------------------------------------

  declare @sStmt1 long varchar;
  declare @sStmt2 long varchar;
  declare @sStmt3 long varchar;
  declare @sStmtTemp long varchar;
  declare @shostvar_value long varchar;
  declare @iPosition int;
  declare @iCount int;
  declare @sHostvar_type varchar(200);
  declare @iDebug 	integer;
  declare @sDebug 	varchar(200);
  declare @iLen integer;
  declare @iTemp  	integer;
  declare @sChar varchar(200);
  declare @sFileSeq varchar(200);
  
    --------------------------------------------------------------------------------
  -- DEBUG
  --------------------------------------------------------------------------------
  set @iDebug = 0;
  set @sDebug = string('MigrateStatement(''',@sFile_path,''')');
  set @sFileSeq = cast(substr(@sFile_path,len(@sFile_path)-2,3) as int);
  
  message 'File seq: ' + @sFileSeq type status to client; 
  message @sDebug+': Início' type status to client;

  --------------------------------------------------------------------------------
  -- VALIDAÇÃO
  --------------------------------------------------------------------------------
 	
 	-- NULL Parameters
 	if @sFile_path is null then
			return NULL;
	end if;

  --------------------------------------------------------------------------------
  -- INICIO
  --------------------------------------------------------------------------------
	
	-- Por segurança, esvazio a tabela temporária
	truncate table satmp_request_time;
	
	-- Chamo procedure que processa arquivo de requests
	call sa_get_request_times(@sFile_path);
    
  -- Para cada query processada, 
  for stmtLoop as stmtCursor dynamic scroll cursor for
    select req_id as @iReq_id,
      trim(stmt) as @sStmt
      from satmp_request_time
      for read only
    do
           
        -- DEBUG
        --message 'Req_id: ' || @iReq_id type status to client;
        
        -- Filtro as queries
        if @sStmt like '%dbo.sp_sql_type_name%' or
           @sStmt like '%fileversion%'  				or
           @sStmt like 'create variable%'	  		or
           @sStmt = '0'						      		or
           @sStmt like 'truncate %'		      		or
           @sStmt like 'checkp%'			      		or
           @sStmt like 'set %'						      or
           @sStmt like 'PREPARE %'				      or
           @sStmt like '%commit%'               or
           @sStmt like '%rollback%'             or
           @sStmt like '%connection_property%'  or
           @sStmt like '%drop conn%'  					or
           @sStmt like '%dummy%'                or
           @sStmt like '%sys.sysinfo%'          or
           @sStmt like '%sys.sysoptions%'       or
           @sStmt like '%select 1%'             or
           @sStmt like '%MigrateStatement%' or
           @sStmt like '%dbcompare%' or
		   @sStmt like 'execute immediate%' or
		   @sStmt like 'fetch next%' or		   
		   @sStmt like 'close %' or
		   @sStmt like 'open %' or
		   @sStmt like 'drop variable %' or
		   
           @sStmt    = ''  then
          
           set @sStmt = NULL;
      
          else
          
  set @sStmt1 = NULL;
  set @sStmt2 = '';
  set @iCount = 0;
  set @iPosition = 1;
  set @iLen = len(@sStmt);


                while @iPosition <= @iLen

               LOOP
                
                  set @sStmt1 = substr(@sStmt,@iPosition,1);
                  if @sStmt1 = '?' then

       			      if substr(@sStmt,@iPosition-1,1) = ':' then 
    
                          select hostvar_value, hostvar_type
                            into @shostvar_value, @sHostvar_type
                            from satmp_request_hostvar
                          where req_id = @iReq_id 
                            and hostvar_num = @iCount;
                            
                            -- Modifico valor do parâmetro
                            if @sHostvar_type in ('timestamp','varchar','char','nvarchar') then
                                set @sStmt1 = string('''',@shostvar_value,'''');
                            else
                                set @sStmt1 = @shostvar_value;
                            end if;
    
                          set @iCount = @iCount + 1;
                      end if;
				  end if;
				  if @sStmt1 = ':' then
				    if substr(@sStmt,@iPosition+1,1) = '?' then 
    					set @sStmt1 = '';
					end if;
				  end if;
				
                  set @sStmt2 = @sStmt2||@sStmt1;

                  set @iPosition = @iPosition + 1;
                  
               end LOOP;
               
        end if;
        
        -- Insiro query nas tabelas que serão utilizadas para teste
        if @sStmt2 is not NULL then
          
          if not exists(select 1 from dbcompare where dbcm_hash = hash(@sStmt2)) then            

             insert into dbcompare (dbcm_hash, dbcm_dbst_seq_old, dbcm_dbst_seq_new, dbcm_query, dbcm_file_seq) 
			     on existing skip
             values (hash(@sStmt2),1, 1, trim(@sStmt2), @sFileSeq);
                
          end if;	
            
        end if;
  
  end for;  
	
	-- DEBUG
  message @sDebug+': Fim!' type status to client;

	return 0;
  
END