


with tab as (
select * from sessions 
where medium IN ('cpc', 'cpm', 'cpa','youtube','cpp','tg','social')
),
joined AS (
  SELECT 
    t.visitor_id,
    t.visit_date,
    t.source AS utm_source,
    t.medium AS utm_medium,
    t.campaign AS utm_campaign,
    l.lead_id,
    l.created_at,
    l.amount,
    l.closing_reason,
    l.status_id
  FROM tab as t
  LEFT JOIN leads l ON t.visitor_id = l.visitor_id
  WHERE l.created_at IS NULL OR t.visit_date <= l.created_at
),
rank AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY lead_id ORDER BY visit_date DESC) AS rnk
  FROM joined
)
SELECT
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
FROM rank
WHERE rnk = 1
ORDER BY
  amount DESC NULLS LAST,
  visit_date ASC,
  utm_source,
  utm_medium,
  utm_campaign;