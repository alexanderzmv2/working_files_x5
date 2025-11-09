-- очищаем от лишних символов и оставляем уникальные значения для использования в разметке ручных корректировок
select 
	distinct
	loyalty_operation_batch_id
	, lower(
		trim(
			regexp_replace(
				regexp_replace(
					concat(lty_oper_batch_comment_txt, '_', lty_oper_batch_comment_int_txt)
					,'(№|INC|INC_)?\d{5,10}','','g'
				)
			, '\s{2,}', ' ', 'g'
			)
		)
	) as batch_comment
from 
	grp_em.dim_loyalty_operation_batch
where
	valid_to_dttm = '5999-01-01 00:00:00.000'