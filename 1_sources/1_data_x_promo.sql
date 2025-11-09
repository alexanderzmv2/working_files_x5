-- часть данных скрыта "*"
-- основной скрипт для выгрузки данных из витрины токенизации баллов
with 
_tech_stores as (
	select 
		lty_point_mvnt_initial_dt
		, loyalty_lifetime_payment_rk
		, store_tech_owner_desc
		, store_tech_mvnt_nm
	from 
		*cb.*nt_movement_fin
	where 
		1=1
		and lty_point_mvnt_initial_dt between '{date_from}'::date - interval '35 days' and '{date_to}'::date
		and lty_point_mvnt_dt between '{date_from}'::date and '{date_to}'::date
		and lty_point_type_nm in ('Баллы','Апельсинки')
		and lty_point_duration_code in ('bon/','endl/','expr/')
		and 
			(
			store_tech_owner_desc in ('5kaD*', 'PerekD*','GamificationTC5*','GamificationTCX*','GamificationEPL*')
						  or
			store_tech_mvnt_nm in ('5ka*', 'Perek*')
			)
		and delete_flg = 0
)
select 
	date_trunc('month', p.lty_point_mvnt_dt)::date dt_month
	,p.lty_promo_owner_iss_owner_nm
	,regexp_replace(p.lty_promotion_iss_owner_nm, E'[\n]+', '', 'g' ) as lty_promotion_iss_owner_nm
	,b.loyalty_operation_batch_id
	,p.lty_point_duration_desc
	,p.lty_point_mvnt_type_desc
	,p.lty_partner_owner_nm
	,_ts.store_tech_owner_desc
	,p.lty_partner_nm
	,_ts.store_tech_mvnt_nm
	,p.division_nm
	,case 
		when 
			(
				p.lty_promo_owner_iss_owner_nm like any(array['Базовые*%','Любимые*%'])
				or 
				p.lty_promotion_iss_owner_nm like 'Подписка*%Базовое%'
			) 
			then ac.lty_promotion_action_chain_nm
	end lty_promotion_action_chain_nm
	, sum(p.lty_point_rub_amt) as  lty_point_rub_amt
from
	*cb.*ement_x_promo p
left join
	_tech_stores _ts
		on 1=1
		and p.lty_point_mvnt_initial_dt = _ts.lty_point_mvnt_initial_dt
		and p.loyalty_lifetime_payment_rk = _ts.loyalty_lifetime_payment_rk
left join 
	*OTION_ACTION_CHAIN ac 
		on 1=1
		and p.lty_promotion_action_chain_rk =  ac.lty_promotion_action_chain_rk
		and valid_to_dttm = '5999-01-01 00:00:00.000'
		and ac.delete_flg = 0
left join
	*LTY_OPERATION_BATCH b
		on p.lty_operation_batch_rk =  b.loyalty_operation_batch_rk
		and b.valid_to_dttm = '5999-01-01 00:00:00.000'
where
	1=1
	and p.lty_point_mvnt_dt between '{date_from}'::date and '{date_to}'::date
	and p.delete_flg = 0
group by 
	1,2,3,4,5,6,7,8,9,10,11,12