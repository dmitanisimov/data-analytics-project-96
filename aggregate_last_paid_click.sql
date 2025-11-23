WITH tab AS (
    SELECT
        visitor_id,
        visit_date,
        CASE
            WHEN source ILIKE 'vk%' THEN 'vk'
            ELSE source
        END AS utm_source,
        medium   AS utm_medium,
        campaign AS utm_campaign
    FROM sessions
    WHERE medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),

SesLead AS (
    SELECT
        t.visitor_id,
        t.visit_date,
        t.visit_date::date AS visit_day,
        t.utm_source,
        t.utm_medium,
        t.utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        ROW_NUMBER() OVER (
            PARTITION BY l.lead_id
            ORDER BY t.visit_date DESC
        ) AS rn
    FROM tab t
    JOIN leads l
      ON t.visitor_id = l.visitor_id
),

grouped AS (
    SELECT
        t.visit_date::date AS visit_date,
        t.utm_source,
        t.utm_medium,
        t.utm_campaign,

        COUNT(DISTINCT t.visitor_id)
            FILTER (WHERE sl.rn = 1) AS visitors_count,


        COUNT(DISTINCT sl.lead_id) FILTER (
            WHERE sl.rn = 1
              AND sl.visit_day = t.visit_date::date
              AND t.visit_date <= sl.created_at
        ) AS leads_count,

        COUNT(sl.lead_id) FILTER (
            WHERE sl.rn = 1
              AND (sl.closing_reason = 'Успешно реализовано'
                   OR sl.status_id = 142)
        ) AS purchases_count,

        SUM(sl.amount) FILTER (
            WHERE sl.rn = 1
              AND (sl.closing_reason = 'Успешно реализовано'
                   OR sl.status_id = 142)
        ) AS revenue

    FROM tab t
    LEFT JOIN SesLead sl
      ON t.visitor_id  = sl.visitor_id
     AND t.visit_date  = sl.visit_date
     AND t.utm_source  = sl.utm_source
     AND t.utm_medium  = sl.utm_medium
     AND t.utm_campaign = sl.utm_campaign

    GROUP BY
        t.visit_date::date,
        t.utm_source,
        t.utm_medium,
        t.utm_campaign
),

costs AS (
    SELECT
        campaign_date::date AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM (
        SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM ya_ads
        UNION ALL
        SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM vk_ads
    ) ads
    GROUP BY
        campaign_date::date,
        utm_source,
        utm_medium,
        utm_campaign
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
FROM grouped g
LEFT JOIN costs c
  ON g.visit_date  = c.visit_date
 AND g.utm_source  = c.utm_source
 AND g.utm_medium  = c.utm_medium
 AND g.utm_campaign = c.utm_campaign
ORDER BY
    g.visit_date ASC,
    g.visitors_count DESC,
    g.utm_source ASC,
    g.utm_medium ASC,
    g.utm_campaign ASC,
    g.revenue DESC NULLS LAST
;