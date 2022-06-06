///Email payment rates
create or replace temporary table past_due_email_drv1 as
select
    b.customer_id
    ,b.account_id
    ,a.event_dt as email_sent_date
    ,a.template_nm
    ,d.statement_num
    ,date(d.payment_due_dt) as payment_due_dt
    ,c.amount as payment_amt
    ,date(c.transaction_date) as payment_date
    ,d.payment_due_min_stmt_usd
    ,d.TOTAL_BALANCE_STMT_USD
    ,d.delinquency_d005_ever_cnt
    //,d.payment_paid_stmt_usd
    ,row_number() over(partition by b.account_id, payment_amt, statement_num order by b.account_id asc) as row_num
from (select * from edw_db.public.event_email where event_dt between '2022-04-04' and '2022-05-03' and (template_nm ilike '%payment-due-clip%'))  a
  left join edw_db.public.CUSTOMER_ACCOUNT_RELATIONSHIP_CUR b
    on try_to_number(a.user_id)  = b.customer_id
  left join (select * from edw_db.public.PAYMENT_TRANSACTIONS where event_type ilike '%executed%') c
    on b.customer_id = c.customer_id
  left join edw_db.public.account_statements d
    on b.account_id = d.account_id 
where abs(datediff(day,d.payment_due_dt, a.event_dt)) < 5
    and abs(datediff(day,c.transaction_date, a.event_dt)) < 5
    and statement_num <= 6    
;
    

create or replace temporary table past_due_email_drv2 as
select
    *
    ,case when total_balance_stmt_usd = 0 then 0 else (payment_amt-payment_due_min_stmt_usd)/total_balance_stmt_usd end as payvol
from past_due_email_drv1
where row_num = 1
    and delinquency_d005_ever_cnt is null or delinquency_d005_ever_cnt < 1
;


///Payment amount rates
select
    template_nm
    ,email_sent_date
    ,statement_num
    ,case when payment_amt > 0 then 'Made Payment'
        else 'No Payment'
     end as made_payment_flag
    ,case when payment_date <= payment_due_dt then 'A) On Time'
        when payment_date <= payment_due_dt + 3 then 'B) Within Grace Period'
        else 'C)Late'
     end as payment_timing
    ,case when payment_amt < payment_due_min_stmt_usd then 'A) < Min Pay'
        when payment_amt = payment_due_min_stmt_usd then 'B) = Min Pay'
        when payment_amt < TOTAL_BALANCE_STMT_USD then 'C) < Total Balance'
        when payment_amt = TOTAL_BALANCE_STMT_USD then 'D) = Total Balance'
        when payment_amt > TOTAL_BALANCE_STMT_USD then 'E) > Total Balance'
     end as payment_amt_bucket_1
    ,case when payvol <= 0 then 'A) Min Pay'
        when payvol < 0.1 then 'B) (0,0.1)'
        when payvol < 0.3 then 'C) [0.1,0.3)'
        when payvol < 0.7 then 'D) [0.3,0.7)'
        when payvol > 0.7 then 'E) > 0.7'
     end as payment_amt_bucket_2
    ,count(distinct account_id)
from past_due_email_drv2
group by 1,2,3,4,5,6,7
;



////% of emailed making payments
select
    //a.email_sent_date
    a.template_nm
    //,a.statement_num
    ,sum(a.total_emails) as totalemails
    ,sum(b.total_payers) as totalpayers
    ,totalpayers/totalemails as payment_rate
from
    (select
        //a.event_dt as email_sent_date
        a.template_nm
        //,d.statement_num
        ,count(distinct user_id) as total_emails
    from edw_db.public.event_email a
    left join edw_db.public.CUSTOMER_ACCOUNT_RELATIONSHIP_CUR b
        on try_to_number(a.user_id)  = b.customer_id
    left join (select * from edw_db.public.account_statements where statement_num <= 6) d
        on b.account_id = d.account_id 
    where event_dt between '2022-04-04' and '2022-05-03'
      and (template_nm ilike '%payment-due%')
      and abs(datediff(day,d.payment_due_dt, a.event_dt)) < 5
    group by 1 ) a
  join
      (select
          //email_sent_date
          template_nm
          //,statement_num
          ,count(distinct account_id) as total_payers
      from past_due_email_drv2
      where payment_date between email_sent_date and payment_due_dt
        and payment_amt >= payment_due_min_stmt_usd
      group by 1 ) b
        on //a.email_sent_date = b.email_sent_date
             a.template_nm = b.template_nm
            //and a.statement_num = b.statement_num
  group by 1


