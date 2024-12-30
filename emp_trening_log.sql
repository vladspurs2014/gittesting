create or replace package emp_trening_log_pkg is
  
/* В пакете будет реализовано логгирование, компиляция пакетов и получение метаданных созданных таблиц */

  procedure put_log (code_in varchar2 default null, text_in varchar2 default null, p_msg varchar2 default null,
					 p_upd number default 0,        p_ins number default 0,        p_del number default 0);
      
  procedure save_log (code_in varchar2 default null, text_in varchar2 default null, p_msg varchar2 default null,
					  p_upd number default 0,        p_ins number default 0,        p_del number default 0);
  
  procedure compile_pkg (p_pkg_name varchar2);
  
  type table_record is record (msg   varchar2(100),
							   ttype varchar2(100),
							   info  varchar2(100)
							  );
							  
  type table_collect_nt is table of table_record;
  
  function table_data (p_table_name varchar2) return table_collect_nt pipelined;
  
  end emp_trening_log_pkg;
  
end emp_trening_pkg;
/

create or replace package body emp_trening_log_pkg
  
  procedure put_log (code_in varchar2 default null, text_in varchar2 default null, p_msg varchar2 default null,
					 p_upd number default 0,        p_ins number default 0,        p_del number default 0);
/*для логгирования*/
  is
  begin
  
	insert into emp_log (user_namee, act_date, er_code, er_msg, oper_msg, upd_row, ins_row, del_row)
	values (user, trunc(sysdate), code_in, text_in, p_msg, p_upd, p_ins, p_del);
	commit;
	
  exception
	
	when others then
		raise_application_error(-20001, sqlerrm);
  end put_log;

  procedure save_log (code_in in varchar2, text_in in varchar2)
/*эта процедура является продолжением "put_log", которая будет вызываться в контексте автономной транзакции и записывать информацию об ошибках даже в случае выполнения
rollback в основном приложении*/
  is
  
    pragma_autonomous_transaction;
	
  begin

	put_log(code_in, text_in, p_msg, p_upd, p_ins, p_del);
	
  end save_log;
  
  procedure compile_pkg ((p_pkg_name varchar2)
/* процедура для компиляции пакетов. Используется именно динамический sql, чтобы можно было компилировать разные пакеты, передавая в качестве параметра имя пакета.
В динамическую строку sql через конкатенацию передаются параметр и локальная переменная, поскольку в них хранятся имена объектов б.д */
  is
    l_owner varchar2(10);
  begin
  
    select o.owner
    into l_owner
     from all_objects o
    where o.object_name = upper(p_pkg_name)
    group by o.owner;
  
    execute immediate 'alter package ' || l_owner || '.' || p_pkg_name || ' compile specification';
    execute immediate 'alter package ' || l_owner || '.' || p_pkg_name || ' compile body';
  
  exception
    when others then
	  save_log(code_in => sqlcode, text_in => sqlerrm);
	  
  end compile_pkg;
  
  function table_data (p_table_name varchar2) return table_collect_nt pipelined
/* получаем информацию о созданных иаблицах */
  is
    l_rec table_record;
	cursor tab_cur (c_table_name varchar2) is
	  with cte_1 as
	 (
	   select 'Основная информация' as msg,
		      o.object_type as ttype,
			  to_char('Статус объекта: ' || o.status || '.' || 'Время последнего DDL: ' || o.last_ddl_time) as info
	    from all_objects o
	   where o.object_name = upper(c_table_name)
	   and o.object_type = 'TABLE'
	   and o.owner = 'EMP_SH'
	   group by o.status, o.last_ddl_time, o.object_type
	 ),
	 
	  cte_2 as
	 (
	   select 'Структура таблицы' as msg,
		      'COLUMN' as ttype,
			  to_char(c.column_name || ' ' ||
			          case
					    when c.data_type = 'VARCHAR2' then c.data_type || '(' || c.data_length || ')'
						else c.data_type end
					 ) as t_info
        from all_tab_columns c
	   where c.table_name = upper(c_table_name)
	   and c.owner = 'EMP_SH'
	   order by c.column_id
	 ),
	  
	  cte_3 as
	 (
	   select 'Структура таблицы' as msg,
	          'ИНДЕКСЫ' as ttype,
	          to_char('column "' || a.column_name || '" has index "' || a.index_name || '"') as t_info
	    from all_ind_columns a
		join all_indexes b on a.index_name = b.index_name
	   where a.table_name = upper(c_table_name)
	   and b.constraint_index = 'NO'
	   group by a.index_name, a.column_name
	 )
	 
	 select * from cte_1
	 union all
	 select * from cte_2
	 union all
	 select * from cte_3;
	 
  begin
    for i in tab_cur(p_table_name) loop
	  l_rec := i;
	  
	  pipe row (l_rec);
	end loop;
	
	return;
	
  exception
    when others then
	  if tab_cur%isopen then
	    close tab_cur;
	  end if;
	  
	  save_log(code_in => sqlcode, text_in => sqlerrm);
	  
  end table_data;
	   
end emp_trening_log_pkg;
/