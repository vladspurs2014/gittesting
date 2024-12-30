/*требование: должность сотрудника может быть переименована. Ввиду этого сотрудник подписывает доп.соглашение, в котором он переводится на новую должность.
Основная таблица должна сохранить данные как с новой должностью, так и с прежней. Поэтому используются поля "date_start" (дата вступления в должность)
и "date_end" (дата смены должности). Задача реализуется с помощью составного триггера*/

create or replace trigger emp_job_trigger
for update on emp
compound trigger
  l_job_new varchar2(50);
  l_bool boolean;
  l_zap emp%rowtype;
  l_ins number := 0;
  l_upd number := 0;
  
  before each row
  
  is
  begin
    if updating ('job') then
	  l_boll := true;
	  l_job_new := :new.job;
	  l_zap := emp_trening_sal_pkg.return_zap_trig(:old.empno, :old.job);
	end if;
  
  end before each row;
  
  after statement
  
  is
  begin
    if l_bool then
	  update emp
	  set date_start = trunc(sysdate);
	  where job = l_job_new;
	  l_upd := l_upd + sql%rowcount;
	  commit;
	  
	  insert into emp(empno, ename, job, mgr, hiredate, sal, comm, deptno, date_start, date_end)
	  values (l_zap.empno, l_zap.ename, l_zap.job, l_zap.mgr, l_zap.hiredate, l_zap.sal, l_zap.comm, l_zap.deptno, l_zap.date_start, trunc(sysdate));
	  l_ins := l_ins + sql%rowcount;
	end if;
	
	emp_trening_log.save_log(p_msg => 'Выполнение триггера emp_job_trigger завершилось успешно', p_ins => l_ins, p_upd => l_upd);
	
  end after statement;
exception
  when others then
    emp_trening_log.save_log(code_in => sqlcode, text_in => sqlerrm, 
							 p_msg => 'Выполнение триггера emp_job_trigger завершилось с ошибкой');

end emp_job_trigger;