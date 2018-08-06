with interactions as (
select customer_account,event_type,event_start_date,event_start_datetime,source_flag,min(event_start_datetime) as event_datetime,
row_number() over(partition by customer_account,event_start_datetime,source_flag order by event_start_datetime asc) as source_order_rn
  from (select account_number as customer_account,event_start_date,event_intent,event_type,event_start_datetime,event_end_date,event_end_datetime,
        case when event_type in ('Engineer Support Calls','Contact Centre Calls','Chat','Message') then 'contact_centre' else 'digital' end as source_flag
        from `skyuk-uk-ds-csg-prod.csg_customer_journey_eu.Full_Customer_journey`
        ) foo
      group by customer_account,event_type,event_start_date,source_flag,event_start_datetime    
  ),
digital_interactions as (
select t.customer_account,t.event_type,t.event_start_date,t.event_start_datetime as digital_first,cc.event_start_datetime as cc_first,
row_number() over (partition by t.customer_account,cc.event_start_datetime order by cc.event_start_datetime asc) as dedup
from interactions t
left join (
    select customer_account,event_type,event_start_date,event_start_datetime,source_flag
    from interactions cc
    where cc.source_flag='contact_centre' and cc.source_order_rn=1    
) cc on t.customer_account=cc.customer_account
      and cc.event_start_datetime > t.event_start_datetime
      and (cc.event_start_date <=date_add(t.event_start_date,interval 7 day))
where t.source_flag='digital' and t.source_order_rn=1
)
/* test output */
 select 
 event_start_date,
 count(customer_account) as customer_count
 from digital_interactions t
 where t.dedup=1
 and event_start_date='2018-07-30'
 group by event_start_date
 limit 100