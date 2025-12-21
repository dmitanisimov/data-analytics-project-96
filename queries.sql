with tab as (
    select * from sessions
    where medium in ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),

joined as (
    select
        t.visitor_id,
        t.visit_date,
        t.source as utm_source,
        t.medium as utm_medium,
        t.campaign as utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id
    from tab as t
    left join leads as l on t.visitor_id = l.visitor_id
    where l.created_at is NULL or t.visit_date <= l.created_at
),

rank as (
    select
        *,
        ROW_NUMBER() over (partition by lead_id order by visit_date desc) as rnk
    from joined
)

select
    visitor_id,
    visit_date,
    utm_source,
    utm_medium,
    utm_campaign,
    lead_id,
    created_at,
    amount,
    closing_reason,
    status_id
from rank
where rnk = 1
order by
    amount desc nulls last,
    visit_date asc,
    utm_source asc,
    utm_medium asc,
    utm_campaign asc;
