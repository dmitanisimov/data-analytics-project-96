
with tab as (
select * from sessions 
where medium IN ('cpc', 'cpm', 'cpa','youtube','cpp','tg','social')
),



SesLead AS (
  SELECT    
    t.visitor_id,
    TO_CHAR(t.visit_date, 'DD-MM-YYYY') as visit_date,
    t.source AS utm_source,
    t.medium AS utm_medium,
    t.campaign AS utm_campaign,
    l.lead_id,
    l.created_at,
    l.amount,
    l.closing_reason,
    l.status_id
  FROM tab AS t
  LEFT JOIN leads l
    ON t.visitor_id = l.visitor_id
    AND t.visit_date <= l.created_at
),



grouped AS (
  SELECT
    visit_date,
    utm_source,
    utm_medium,
    utm_campaign,
    COUNT(DISTINCT visitor_id) AS visitors_count,
    COUNT(DISTINCT lead_id) FILTER (
      WHERE lead_id IS NOT NULL
    ) AS leads_count,
    COUNT(DISTINCT lead_id) FILTER (
      WHERE closing_reason = 'Успешно реализовано' OR status_id = 142
    ) AS purchases_count,
    SUM(amount) FILTER (
      WHERE closing_reason = 'Успешно реализовано' OR status_id = 142
    ) AS revenue
  FROM SesLead
  GROUP BY visit_date, utm_source, utm_medium, utm_campaign
),

costs AS (
  SELECT
    TO_CHAR(campaign_date, 'DD-MM-YYYY') AS visit_date,
    utm_source,
    utm_medium,
    utm_campaign,
    SUM(daily_spent) AS total_cost
  FROM (
    SELECT campaign_date, daily_spent, utm_source, utm_medium, utm_campaign
    FROM ya_ads
    UNION ALL
    SELECT campaign_date, daily_spent, utm_source, utm_medium, utm_campaign
    FROM vk_ads
  ) ads
  GROUP BY campaign_date, utm_source, utm_medium, utm_campaign
)



SELECT
  g.visit_date,
  g.visitors_count,
  g.utm_source,
  g.utm_medium,
  g.utm_campaign,
  COALESCE(c.total_cost, 0) AS total_cost,
  g.leads_count,
  g.purchases_count,
  g.revenue
FROM grouped AS g
LEFT JOIN costs as c
  ON g.visit_date = c.visit_date
 AND g.utm_source = c.utm_source
 AND g.utm_medium = c.utm_medium
 AND g.utm_campaign = c.utm_campaign
ORDER BY
  g.visit_date ASC,
  g.visitors_count DESC,
  g.utm_source,
  g.utm_medium,
  g.utm_campaign,
  g.revenue DESC NULLS LAST;
    