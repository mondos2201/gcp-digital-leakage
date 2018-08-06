with digital_interactions as (
select switch_id,customer_account,interaction_start_datetime,interaction_end_datetime,interaction_start_date,interaction_end_date,channel
from (select t.Fast_Switching_Interaction_ID as switch_id, t.Account_number as customer_account,
            t.interaction_start_datetime, t.interaction_end_datetime,
            t.interaction_start_date, t.interaction_end_date,event_type as channel,
            t.interaction_operation_group as digital_ops_group,t.interaction_intent as intent,t.interaction_Sub_Category as intent_sub_category,
            row_number() over (partition by t.Account_number, t.Fast_Switching_Interaction_ID order by t.Account_Interaction_ID asc) as rn
            from `skyuk-uk-ds-csg-prod.csg_customer_journey_eu.Full_Customer_journey_Interaction_Summary` t
            where t.event_type in ('Web', 'App', 'Interactive') 
    ) first_interaction
 where first_interaction.rn=1
),
centre_interactions as (
select customer_account,event_type,event_start_date,event_start_datetime,event_end_date,event_end_datetime
  from (select account_number as customer_account,event_start_date,event_intent,event_type,event_start_datetime,event_end_date,event_end_datetime,
        row_number() over (partition by account_number,event_start_datetime order by event_start_datetime asc) as rn
        from `skyuk-uk-ds-csg-prod.csg_customer_journey_eu.Full_Customer_journey` t
        where (t.event_type in ('Engineer Support Calls','Contact Centre Calls','Chat') or 
                              (t.event_type='Message' and t.conversation_direction='Inbound')) 
                and t.CALL_DIRECTION ='INBOUND' 
                and t.time_spent_customer > 0
        ) first_interaction
 where first_interaction.rn=1
)
--#output
select switch_id,customer_account,interaction_start_datetime,event_start_datetime,
timestamp_diff(event_start_datetime,interaction_start_datetime, SECOND) as leak_sec

from (select switch_id,t.customer_account,cc.event_start_datetime,interaction_start_datetime,
      row_number() over (partition by t.customer_account,cc.event_start_datetime order by cc.event_start_datetime asc) as dedup
      from digital_interactions t
      left join centre_interactions cc on t.customer_account=cc.customer_account
      and cc.event_start_datetime > t.interaction_start_datetime
      and (cc.event_start_date <=date_add(t.interaction_start_date,interval 7 day))
      ) output_data
where output_data.dedup =1
--and date(output_data.interaction_start_datetime)>='2018-07-30'