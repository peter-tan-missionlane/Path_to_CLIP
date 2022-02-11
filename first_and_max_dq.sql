use database SANDBOX_DB;
use schema user_tb;

///Define the segmentation of population

DROP TABLE IF EXISTS sandbox_db.user_tb.acct_clip;
Create temp table sandbox_db.user_tb.acct_clip as (
select
   card_id
  ,b.account_id
  ,b.customer_id
  ,statement_number
  ,substring(b.statement_end_dt,1,7) as stmt_month
  ,to_char(EVALUATED_TIMESTAMP,'YYYY-MM') AS month_evaluated
  ,credit_limit_orig_usd as ICL
  ,outcome
  ,DECISION_DATA
  ,CLIP_RISK_GROUP
  ,CLIP_POLICY_NAME
  ,TEST_SEGMENT
  ,(POST_CLIP_LINE_LIMIT - PRE_CLIP_LINE_LIMIT) as CLIP_AMT
  ,decision_data:"never_delinquent__passed" as no_DQ_flag1
  ,decision_data:"delinquency__passed" as no_DQ_flag2
  ,COALESCE(no_DQ_flag1, no_DQ_flag2) AS no_DQ_Flag
  ,decision_data:"average_utilization_3_months"::FLOAT as util_at_clip1 
  ,decision_data:"average-credit-line-utilization"::FLOAT as util_at_clip2
  ,decision_data:"average_credit_line_utilization_last_3_statements"::FLOAT as util_at_clip3
  ,decision_data:"current_principal_utilization"::FLOAT as util_at_clip4
  ,decision_data:"internal__fis_tsys__PRINCIPAL_UTILIZATION"::FLOAT as util_at_clip5
  ,COALESCE(util_at_clip1,util_at_clip2,util_at_clip3,util_at_clip4,util_at_clip5) as util_at_clip
  ,case when util_at_clip < 0.1  then 'A.<10%'
      when util_at_clip <0.3  then 'B.10%-30%'
      when util_at_clip <.5  then 'C.30%-50%'
      when util_at_clip <0.8  then 'D.50%-80%'
      when util_at_clip >=0.8  then 'E.>80%'
      end as util_band
from EDW_DB.PUBLIC.CLIP_RESULTS_DATA a
  inner join
      (select * 
          from edw_db.Public.account_statements
          where statement_num = 7
                  and customer_id not in (select user_id as customer_id from sandbox_db.user_tb.BWEISS_FRAUD_ATTACK_MAY_24)) b
     on a.card_id = b.account_id
);

create or replace temporary table stmt7_clip_segmentation_drv as
select
     *
    ,case when outcome ilike '%approved%' and clip_amt > 100 then 'A) Approved, >minCLIP'
          when outcome ilike '%approved%' and clip_amt = 100 and util_band ilike '%A.%' then 'C) Approved, minCLIP, Low Util'
          when outcome ilike '%approved%' and clip_amt = 100 then 'B) Approved, minCLIP, High Risk'
          when outcome ilike '%ineligible%' and (no_dq_flag ilike '%false%') then 'D) Ineligible - DQ Cut'
          when outcome ilike '%ineligible%'                                  then 'E) Ineligible - Hardcut'
          when outcome ilike '%declined%' and (util_at_clip < 0.1)           then 'F) Declined - Low Util'
          when outcome ilike '%declined%'                                    then 'G) Declined - High Risk'
     end as CLIP7_outcome_group
     //Update to latest definition of minCLIP and >minCLIP based on stmt 7 grid
     
from sandbox_db.user_tb.acct_clip      
where statement_number = 7
    and month_evaluated between '2021-01-01' and '2022-01-01'
;
create or replace temporary table first_DQ as
select
    clip7_outcome_group
    ,b.*
    ,case when b.delinquency_d005_ever_cnt >= 1 then statement_num
        else null
     end as DQ_stmt
from stmt7_clip_segmentation_drv a
    left join edw_db.public.account_statements b
        on a.card_id = b.account_id
where b.statement_num <= 7
    and a.clip7_outcome_group ilike '%DQ%'
order by b.account_id, b.statement_num
;
////////// max DQ of those DQ'd
create or replace temporary table first_and_max_DQ as
select
    a.account_id
    ,min(a.DQ_stmt) as first_DQ_stmt
    ,case when b.DELINQUENCY_D060_EVER_CNT > 0 or b.DELINQUENCY_D090_EVER_CNT > 0 or b.DELINQUENCY_D120_EVER_CNT > 0 or b.DELINQUENCY_D150_EVER_CNT > 0 or b.DELINQUENCY_D180_EVER_CNT > 0 then 'DQ60+'
            when b.DELINQUENCY_D030_EVER_CNT > 0 then 'DQ30'
            when b.DELINQUENCY_D005_EVER_CNT > 0 then 'DQ5'
        else 'Other'
        end as Max_dq_bucket_reached
from first_DQ a
    left join (select * from edw_db.public.account_statements where statement_num = 7) b
        on a.account_id = b.account_id
group by 1,3
;


select
    first_dq_stmt
    ,max_dq_bucket_reached
    ,count(distinct account_id)
from first_and_max_DQ
group by 1,2
