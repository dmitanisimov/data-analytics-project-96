with joined as (
    select
        t.visitor_id,
        t.medium as utm_medium,
        t.campaign as utm_campaign,
        t.visit_date::date as visit_date,
        case
            when t.source ilike 'vk%' then 'vk'
            else t.source
        end as utm_source,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        row_number() over (
            partition by t.visitor_id
            order by t.visit_date desc
        ) as rnk
    from sessions t
        left join leads l on t.visitor_id = l.visitor_id
            and t.visit_date <= l.created_at
    where t.medium <> 'organic'
),

seslead as (
    select
        *
    from joined
    where rnk = 1
),

grouped as (
    select
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        count(visitor_id) as visitors_count,
        count(distinct lead_id) as leads_count,
        count(lead_id) filter (
            where closing_reason = 'Успешно реализовано'
                or status_id = 142
        ) as purchases_count,
        sum(amount) filter (
            where closing_reason = 'Успешно реализовано'
                or status_id = 142
        ) as revenue
    from seslead
    group by
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign
),

costs as (
    select
        campaign_date::date as visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        sum(daily_spent) as total_cost
    from (
        select
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        from ya_ads
        union all
        select
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        from vk_ads
    ) ads
    group by
        campaign_date::date,
        utm_source,
        utm_medium,
        utm_campaign
)

select
    g.visit_date,
    g.visitors_count,
    g.utm_source,
    g.utm_medium,
    g.utm_campaign,
    c.total_cost,
    g.leads_count,
    g.purchases_count,
    g.revenue
from grouped g
    left join costs c on g.visit_date = c.visit_date
        and g.utm_source = c.utm_source
        and g.utm_medium = c.utm_medium
        and g.utm_campaign = c.utm_campaign
order by
    g.revenue desc nulls last,
    g.visit_date asc,
    g.visitors_count desc,
    g.utm_source asc,
    g.utm_medium asc,
    g.utm_campaign asc;
