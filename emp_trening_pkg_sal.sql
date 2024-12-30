create or replace package emp_trening_sal_pkg is
/* В этом пакете выполняется: 
 1. Индексация заработных плат работников, чей размер зп попадает под определенные условия;
 2. Создается функция для составного триггера "emp_job_trigger"
*/

  function sal_job (job_in emp.job%type) return emp.sal%type;
  
  type number_nested_table is table of emp.sal%type;
  
  procedure indexes_sal (job_in emp.job%type);
  
  function return_zap_trig(p_empno number, p_job varchar2) return emp%rowtype;
  
end emp_trening_sal_pkg;
/

create or replace package body emp_trening_sal_pkg is

  function sal_job (job_in emp.job%type) return emp.sal%type
  --функция возвращает зарплату директора компании. Функция вызывается в SP "indexes_sal". Там же описана логика задачи
  is
    cursor cur_sal is
	  select sal
	   from emp
	  where job = job_in;
	l_sal emp.sal%type := 0;
	l_cnt number 	   := 0;
	no_data exception;
	PRAGMA EXCEPTION_INIT (no_data, 100);
  
  begin
    emp_trening_log.save_log(p_msg => 'Старт расчета функции sal_job для получения зарплаты ключевой должности');
	
	if not cur_sal%isopen then
	  open cur_sal;
	end if;
	
	fetch cur_sal into l_sal;
	l_cnt := l_cnt + cur_sal%rowcount;
	
	emp_trening_log.save_log(p_msg => 'Явный курсор должен передать локальной переменной одно значение - ', p_ins => l_cnt);
	
	if cur_sal%notfound then
	  raise no_data;
    end if;
	
	close cur_sal;
	
	return l_sal;
	
	emp_trening_log.save_log(p_msg => 'Расчет функции sal_job для получения зарплаты ключевой должности завершился успешно')
	
  exception
    when no_data then
	  emp_trening_log.save_log(p_msg => 'Расчет зарплаты для важной должности завершился ошибкой отсутствия наличия данных в таблице');
	  
	  if cur_sal%isopen then
	    close cur_sal;
	  end if;
	  
	when others then
	  emp_trening_log.save_log(code_in => sqlcode, text_in => sqlerrm, p_msg => 'Расчет зарплаты для важной должности завершился по причине другой ошибки');
	  
	  if cur_sal%isopen then
	    close cur_sal;
	  end if;
	  
  end sal_job;
  
  procedure indexes_sal (job_in emp.job%type)
/*задача: проиндексировать заработную плату тем сотрудникам, чей прежний размер з/п соответствует условию*/
  is
    
	big_bucks number := sal_job(job_in);
	min_sal   number := big_bucks / 5;
	cursor ename_sal_cur (max_sal in number) is
	  select ename, sal
	   from emp
	  where job != job_in
	  and sal < min_sal;
	type nt_type is table of ename_sal_cur%rowtype;
	names_old_sal nt_type := nt_type();
	new_salary number_nested_table := number_nested_table();
	l_cnt number := 0;
	l_upd number := 0;
	l_ins number := 0;
	no_data_sal exception;
	PRAGMA EXCEPTION_INIT(no_data_sal, 100);
	
  begin
    emp_trening_log.save_log(p_msg => 'Старт расчета SP indexes_sal для индексации зарплат других работников');
	
	if not ename_sal_cur%isopen then
	  open ename_sal_cur(big_bucks);
	end if;
	
/* BULK COLLECT позволяет создавать новое место для элементов коллекции без использования метода "extend"*/

    fetch ename_sal_cur bulk collect into names_old_sal;
	
	for k in 1 .. names_old_sal.count loop
	  l_cnt := l_cnt + 1;
	end loop;
	
	if l_cnt = 0 then
	  raise no_data_sal;
	end if;
	
	emp_trening_log.save_log(p_msg => 'Явный курсор, используя BULK COLLECT, передал коллекции кол-во элементов - ', p_ins => l_cnt);
	
	forall indx in names_old_sal.first .. names_old_sal.last
	save exceptions
	  update emp
	  set sal = min_sal
	  where ename = names_old_sal(indx).ename
	  returning sal bulk collect into new_salary;
	  l_upd := l_upd + sql%rowcount;
	commit;
	
	emp_trening_log.save_log(p_msg => 'Индексация зарплаты работников завершилась успешно. Было обновлено строк - ', p_upd => l_upd);
	
	close ename_sal_cur;
	
/*После индексации з/п (выполнения обновления "sal"), нужно сформировать отчет, содержащий имена сотрудников, их прошлую и актуальную з/п.
  Сформировать отчет можно 2-мя способами:
  Способ №1: представлен ниже;
  Способ №2: реализуется с помощью dml-триггера (after) на уровне строки для команды "update"*/
  
    emp_trening_log.save_log(p_msg => 'Начало формирование отчета по результатам выполненной индексации');
    
    forall i in names_old_sal.first .. names_old_sal.last
    save exceptions
      insert into emp_report
	  values (names_old_sal(i).ename, names_old_sal(i).sal, new_salary(i));
	  l_ins := l_ins + sql%rowcount;
	commit;
	
	emp_trening_log.save_log(p_msg => 'Формирование отчета с результатами индексации завершилось успешно. Было вставлено строк - ', p_ins => l_ins);
	
  exception
    when no_data_sal then
	  emp_trening_log.save_log(p_msg => 'Ошибка: Курсор не вернул данные: не все условия для проведения индексации зарплаты были соблюдены');
	  
	  if ename_sal_cur%isopen then
	    close ename_sal_cur;
	  end if;
	  
	when others then
	  if ename_sal_cur%isopen then
	    close ename_sal_cur;
	  end if;
	  
	  for o in 1 .. sql%bulk_exceptions.count loop
	  save_log(
				code_in => sql%bulk_exceptions(o).error_code,
			    text_in => sqlerrm(-1 * sql%bulk_exceptions(o).error_code),
				p_msg   => 'Расчет SP indexes_sal для индексации зарплат других работников завершился с ошибкой'
				);
    end loop;
  
  end indexes_sal;
  
  function return_zap_trig (p_empno number, p_job varchar2) return emp%rowtype
--функция создается для применения в составном триггере emp_job_trigger
  is
    l_row number := 0;
	l_zap emp%rowtype;
	
  begin
    emp_trening_log.save_log(p_msg => 'Старт расчета функции return_zap_trig для составного триггера emp_job_trigger');
	
	select *
	into l_zap
	 from emp
	where empno = p_empno
	and job = p_job;
	l_row := l_row + sql%rowcount;
	
	emp_trening_log.save_log(p_msg => 'Конструкция select ... into отработала успешно и вернула одну запись - ', p_ins => l_row);
	
	return l_zap;
	
	emp_trening_log.save_log(p_msg => 'Расчет функции return_zap_trig для составного триггера emp_job_trigger завершился успешно');
	
  exception
    when others then
	  emp_trening_log.save_log(code_in => sqlcode, text_in => sqlerrm,
							   p_msg => 'Расчет функции return_zap_trig для составного триггера emp_job_trigger завершился с ошибкой');
							   
  end return_zap_trig;
  
end emp_trening_sal_pkg;
/