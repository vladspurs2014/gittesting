/* Логика: пакетная процедура "emp_trening_debt_pkg.load_pipe_data" выполняет расчет динамики дебиторской задолженности клиентов. Формирует отчет и заполняет таблицу.
   Процедура принимает в себя параметр, на основании которого вычисляются срезы, по которым выполняется расчет. Процедура должна запускаться каждую пятницу рано утром.
   Для этого создается джоб, планировщик заданий PL/SQL. Джоб будет запускать пакетную процедуру каждую пятницу в 01:00 по мск.*/
   
begin
  dbms_scheduler.create_job(
							 job_name   	 => 'debt_job',
							 job_type 		 => 'plsql_block',
							 job_action      => 'begin emp_trening_debt_pkg.load_pipe_data; end;',
							 start_date      => systimestamp,
							 repeat_interval => 'FREQ=WEEKLY; BYDAY=FRI; BYHOUR=01; BYMINUTE=0; BYSECOND=0;'
							 end_date 		 => to_date('31.12.2025','dd.mm.yyyy'),
							 enabled 	     => 'TRUE'
							);
							
exception
  when others then
    emp_trening_log_pkg.save_log(code_in => sqlcode, text_in => sqlerrm);

end;