use [TreningSUBD];
begin transaction;

/*update Project
set budget = 5100
where number = 4;*/

select *
from dbo.AuditBudget;

commit;