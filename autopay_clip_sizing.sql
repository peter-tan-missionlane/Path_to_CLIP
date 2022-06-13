create or replace temporary table autopay_base_20220609 as 
Select apsc.Account_ID
  ,  apsc.Autopay_Source
  , apsc.Customer_ID
  , apsc.Day_of_Month
  , apsc.Fixed_Amount
  , apsc.Funding_Account_ID
  , APSC.Policy_Type
  , APSC.Schedule_Status,
assu.Schedule_id, assu.Schedule_status as Sched_Status, apsc.ETL_INSERt_TIMESTAMP_EST::date as Created_Date, assu.ETL_INSERt_TIMESTAMP_EST::date as Update_date
from "EDW_DB"."PUBLIC"."AUTOPAY_SCHEDULE_CREATED" as apsc
left join "EDW_DB"."PUBLIC"."AUTOPAY_SCHEDULE_STATUS_UPDATED" as assu
on apsc.schedule_id = assu.schedule_id
where apsc.Policy_Type ilike '%Monthly%' and TRIM(APSC.Schedule_Status) = 'ACTIVE' and (assu.Schedule_status is null or assu.Schedule_status = 'COMPLETE')
;


create or replace table AUTOPAY_DATA_20220609 as (
select b.*
  , a.EXTERNAL_ACCOUNT_ID
  , a.STATEMENT_END_DT
  , a.STATEMENT_NUM
  , case when b.ACCOUNT_ID is not null then 1 else 0 end as auto_pay_flag
from autopay_base_20220609 as b 
  left join  "EDW_DB"."PUBLIC"."ACCOUNT_STATEMENTS"  as a
on a.ACCOUNT_ID = b.ACCOUNT_ID and b.Created_Date <= a.STATEMENT_END_DT
--BETWEEN a.STATEMENT_START_DT AND a.STATEMENT_END_DT
)
;



////May snapshot of CLIP7 decline rate for autopay-enrolled customers
/// Can filter for statement in the where clause, and be sure to change the model_decline_flag based on statement of interest

create or replace temporary table drv1 as
select
    a.card_id
    ,c.account_id
    ,a.outcome
    ,(POST_CLIP_LINE_LIMIT - PRE_CLIP_LINE_LIMIT) as CLIP_AMT
    ,decision_data:"clip_model_c_20210811_risk_group"::INT AS clip_risk_group_INT
    ,case 
        //when outcome ilike '%approved%' and clip_amt = 100 and clip_risk_group_INT >= 7 then 1 //s7
        when outcome ilike '%decline%' and clip_risk_group_INT >= 7 then 1 //s11 and 18
        else 0
     end as model_decline_flag
    ,case when auto_pay_flag = 1 then 1 else 0 end as autopay_flag 
from (select * from edw_db.public.clip_results_data where statement_number = 18 and evaluated_timestamp between '2022-05-01' and '2022-05-31') a
    left join edw_db.public.accounts_customers_bridge bridge 
        on bridge.card_id = a.card_id
     left join (select * from autopay_data_20220609 where auto_pay_flag = 1 and statement_num = 18 and statement_end_dt between '2022-05-01' and '2022-05-31') c
        on c.account_id = bridge.account_id
;

select
    autopay_flag
    ,model_decline_flag
    ,count(distinct card_id)
from drv1
group by 1,2
;
