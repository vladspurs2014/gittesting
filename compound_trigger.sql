create or replace trigger order_detail
  for insert or update or delete
  on emp_orders_detail
  compound trigger

  l_ord_zap emp_orders%rowtype;
  before each row

  is
  begin
  /*в случае обновления значения или вставки новой строки в таблицу emp_orders_detail, пакетная процедура "emp_trening_pkg.get_discount" вернет скидку для нового ид.
   Процедура обращается к таблице "orders", в которой уже хранится нужный ид. Затем пересчитывается поле "sum_order"*/
    if inserting or updating then
      emp_trening_pkg.get_discount(:new.id_order);
	  l_ord_zap.discount := emp_trening_pkg.order_zap.discount;
	  :new.sum_order := new.price * new.qty * (1 - l_ord_zap.discount / 100);
	  l_ord_zap.id := :new.id_order;
    end if;

  end before each row;

  after each row
  is
  begin
    if deleting then
	  l_ord_zap.id = :old.id_order;
	end if;
  end after each row;
  
  after statement
  is
  begin
    update emp_orders
	set amount = (
	              select sum(d.sum_order)
				   from emp_orders_detail d
				  where d.id_order = l_ord_zap.id
				 )
    where id = l_ord_zap.id;
  end after statement;

end order_detail;