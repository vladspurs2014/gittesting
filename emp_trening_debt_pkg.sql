create or replace package emp_trening_debt_pkg is
  
/* В пакете будет реализовано:
 1)Формирование отчета в динамике по остаткам дебиторской задолженности на актуальную дату формирования отчета и прошлую дату: неделя, месяц, квартал, год.
 2) Формирование отчета-рейнтинга топ-10 по занимаемой доли клиентом в портфеле всех клиентов банка.
*/

  type deb_nt_collec is table of emp_obj_deb;
  
  def_limit constant pls_integer := 100;
  
  type deb_table is table of emp_debitor_report%rowtype;
  
  type deb_rec is ref cursor return emp_debitor_report%rowtype;
  
  type emp_rec is record (rec_var varchar2(5), rec_date date);
  
  type var_rec is varray(5) of emp_rec;
  
  function pipe_deb (p_dataset deb_rec, p_limit pls_integer default def_limit) return deb_nt_collec pipelined;
  
  procedure load_pipe_data (p_date date);
  
  procedure load_pers_box (p_date date, p_kpi varchar);
  
end emp_trening_debt_pkg;
/
  
create or replace package body emp_trening_debt_pkg

  function pipe_deb (p_dataset deb_rec, p_limit pls_integer default def_limit) return deb_nt_collec pipelined
/*PIPELINE функция позволит преобразовывать данные из таблицы "emp_debitor" в таблицу "emp_debitor_report". Функция будет вызываться в цикле другой SP.
  Бизнес-логика задачИ будет описана в SP*/
  is
    l_object emp_obj_deb := emp_obj_deb(
										company_name => null,
										actual_date  => null,
										date_srez    => null,
										period_type  => null,
										c_sum        => null,
										c_sum_last   => null,
										dynamicc      => null
										);
	l_collec deb_table := deb_table();
	
  begin
    emp_trening_log.save_log(p_msg => 'Старт расчета функции pipe_deb и подготовка к преобразованию входящего датасета.');
	
	loop
	  fetch p_dataset bulk collect into l_collec limit p_limit;
	  exit when l_collec.count = 0;
	  
	  for i in 1 .. l_collec.count loop
	    l_object.company_name := l_collec(i).company_name;
		l_object.actual_date  := l_collec(i).actual_date;
		l_object.date_srez    := l_collec(i).date_srez;
		l_object.period_type  := l_collec(i).period_type;
		l_object.c_sum        := l_collec(i).c_sum;
		l_object.c_sum_last   := l_collec(i).c_sum_last;
		l_object.dynamicc     := l_collec(i).dynamicc;
		
		pipe row (l_object);
	  end loop;
	end loop;
	
	close p_dataset;
	
	return;
	
	emp_trening_log.save_log(p_msg => 'Расчет функции pipe_deb завершился успешно. Датасет подготовлен к конвейерной передаче.');
	
  exception
    when others then
	  if p_dataset%isopen then
	    close p_dataset;
	  end if;
	  
	  emp_trening_log.save_log(code_in => sqlcode, text_in => sqlerrm, p_msg => 'Расчет функции pipe_deb и преобразование датасета завершилось ошибкой.');
	  
  end pipe_deb;
  
  procedure load_pipe_data (p_date date)
/*в таблице "emp_debitor" хранится информация обо всех дебиторах и размерах их деб.задолженности на дату. Нужно сформировать отчет, в котором будет консолидирована сумма задол-
женности дебитора на отчетную дату и дату среза (прошлую дату), а также расчитана динамика (либо задолженность погашается, либо увеличивается). В качестве срезов будут использованы:
недельные, месячные, квартальные и годовые данные. Поэтому конвейерная функция, написанная выше, будет вызываться в цикле*/
  is
    l_collec navi_tp_1 := navi_tp_1();
	l_date date        := p_date;
	l_ins number 	   := 0;
	l_del number 	   := 0;
	
  begin
    emp_trening_log.save_log(p_msg => 'Старт расчета SP load_pipe_data');
	
	delete from emp_debitor_report dr
	where dr.actual_date = l_date;
	l_del := l_del + sql%rowcount;
	commit;
	
	emp_trening_log.save_log(p_msg => 'Предварительное удаление данных из таблицы emp_debitor_report за отчетную дату ' || p_date || '. Удалено строк - ',
						     p_del => l_del);
							 
    l_collec := navi_tp_1 (emp_rec('R', l_date),
						   emp_rec('W', trunc(l_date -7,'dd')),
						   emp_rec('M', trunc(l_date,'mm') -1),
						   emp_rec('Q', trunc(l_date,'q') - 1),
						   emp_rec('Y', trunc(l_date,'y') - 1)
						   );
	for i in 1 .. l_collec.count loop
	  insert into emp_debitor_report
	  select *
	   from (
			  table(
					pipe_deb(
							  cursor(
							          select t.namee,
											 l_date as actual_date,
											 l_collec(i).rec_date as date_srez,
											 l_collec(i).rec_var  as period_type,
											 t.c_sum,
											 t.c_sum_last,
											 (t.c_sum - t.c_sum_last) as dynamicc
									   from (
											  select *
											   from (
													  select d.namee,
															 sum(d.deb_dolg) as sum_dolg,
															 case
															   when d.period_type = 'R' then 'start'
															   when d.period_type = l_collec(i).rec_var then 'end'
															 end lbl
													   from emp_debitor d
													  where d.period_type in ('R' and l_collec(i).rec_var)
													  group by d.namee,
															   d.period_type
													)
													 pivot
													       (
														     sum(sum_dolg)
															 for lbl in ('start' as c_sum, 'end' as c_sum_last)
														    )
											) t
									)
							)
					)
			);
	l_ins := l_ins + sql%rowcount;
	emp_trening_log.save_log(p_msg => 'Заполнение таблицы emp_debitor_report за дату расчета ' || l_collec(i).rec_date || '. И за срез - ' || l_collec(i).rec_var || 'Вставлено строк - ',
							 p_ins => l_ins);
	commit;
	end loop;
  
  exception
    when others then
	  emp_trening_log.save_log(code_in => sqlcode, text_in => sqlerrm, p_msg => 'Заполнение таблицы emp_debitor_report завершилось ошибкой');
  
  end load_pipe_data;
  
  procedure load_pers_box (p_date date, p_kpi varchar)
/*Составить топ-10 клиентов по занимаемой доли суммы по КПЭ от общего портфеля всех клиентов банка*/
  is
    l_con_date constant date := to_date('01.01.2024','dd.mm.yyyy');
	l_date				date := p_date;
	l_kpi				varchar2(10);
	l_ins				number := 0;
	check_kpi			exception;
  
  begin
    emp_trening_log.save_log(p_msg => 'Старт заполнения таблицы emp_pers_box и выполнения SP load_pers_box.');
	
	emp_trening_log.save_log(p_msg => 'Проверка на наличие верного КПЭ ' || p_kpi || '.');
	
	if p_kpi in ('ACT_OST', 'PSV_OST', 'OD_OST', 'ZAP_OST')
	  then
	    l_kpi := p_kpi;
	  else
	    raise check_kpi;
	end if;
	
	emp_trening_log.save_log(p_msg => 'Проверка выполнена успешно: передан верный КПЭ ' || l_kpi || '.');
	
	emp_trening_log.save_log(p_msg => 'Предварительное очищение таблицы emp_pers_box');
	
	--используется truncate, поскольку историчность сохранения данных по срезам не требуется
	execute immediate 'truncate table emp_pers_box drop all storage';
	
	emp_trening_log.save_log(p_msg => 'Заполнение таблицы emp_pers_box за дату ' || l_date || ' и за КПЭ ' || l_kpi || '.');
	
	insert into emp_pers_box
   (
    date_report,
	client_id,
	client_name,
	tb_id,
	tb_name,
	kpi,
	sum_rev,
	pers,
	rnk
   )
   
   with data_sourse as
  (
    select *
	 from 
	      (
		    select xx.client_id,
				   xx.tb_id,
				   xx.c_summ,
				   xx.date_report,
				   xx.kpi,
				   round(xx.c_summ/xx.total_bank_sum,4) * 100 as pers,
				   row_number() over (
									   partition by xx.tb_id order by round(xx.c_summ/xx.total_bank_sum,4) * 100 desc
									  ) as rnk
			 from
				 (
				  select d.*,
					     sum(d.c_summ) over (partition by d.tb_id) as total_bank_sum
				   from emp_data_client d
				  where d.kpi = l_kpi
				  and d.date_report = l_date
				  and d.c_summ != 0
				 ) xx
		  ) yy
		   where yy.rnk <= 10
   ),
   
   tb_sourse as
  (
    select t.id_tb,
		   t.tb_name,
		   
     from emp_spr_tb t
	where t.date_end >= l_con_date
  )
  
  select ds.date_report,
		 ds.client_id,
		 c.client_name,
		 ds.tb_id,
		 ts.tb_name,
		 ds.kpi,
		 ds.c_summ,
		 ds.pers,
		 ds.rnk
   from data_sourse ds
   join tb_sourse ts on ds.tb_id = ts.id_tb
   join emp_clients c on ds.client_id = c.client_id;
   l_ins := l_ins + sql%rowcount;
   commit;
   
   emp_trening_log.save_log(p_msg => 'Заполнение таблицы emp_pers_box за дату ' || l_date || ' и за КПЭ ' || l_kpi || ' выполнено успешно. Вставлено строк: ',
							p_ins => l_ins);
  
  exception
    when check_kpi then
	  emp_trening_log.save_log(p_msg => 'Заполнение таблицы emp_pers_box и выполнение SP load_pers_box завершилось ошибкой: передан неверный КПЭ: ' || l_kpi);
	  rollback;
	  
	when others then
	  emp_trening_log.save_log(code_in => sqlcode, text_in => sqlerrm, p_msg => 'Заполнение таблицы emp_pers_box и выполнение SP load_pers_box завершилось ошибкой');
	  rollback;
	  
  end load_pers_box;
  
end emp_trening_debt_pkg;
/