WITH joined AS (
    SELECT
        t.visitor_id,
        t.medium AS utm_medium,
        t.campaign AS utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        t.visit_date::date AS visit_date,
        CASE
            WHEN t.source ILIKE 'vk%' THEN 'vk'
            ELSE t.source
        END AS utm_source,
        ROW_NUMBER() OVER (
            PARTITION BY t.visitor_id
            ORDER BY t.visit_date DESC
        ) AS rnk
    FROM sessions AS t
    LEFT JOIN leads AS l
        ON
            t.visitor_id = l.visitor_id
            AND t.visit_date <= l.created_at
    WHERE t.medium <> 'organic'
),
    
seslead AS (
    SELECT * FROM joined
    WHERE rnk = 1
),

grouped AS (
    SELECT
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        COUNT(visitor_id) AS visitors_count,
        COUNT(DISTINCT lead_id) AS leads_count,
        COUNT(lead_id) FILTER (
            WHERE closing_reason = 'Успешно реализовано'
               OR status_id = 142
        ) AS purchases_count,
        SUM(amount) FILTER (
            WHERE closing_reason = 'Успешно реализовано'
               OR status_id = 142
        ) AS revenue
    FROM seslead
    GROUP BY
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign
),

costs AS (
    SELECT
        campaign_date::date AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM (
        SELECT
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM ya_ads
        UNION ALL
        SELECT
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM vk_ads
    ) AS ads
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
    c.total_cost,
    g.leads_count,
    g.purchases_count,
    g.revenue
FROM grouped AS g
LEFT JOIN costs AS c
    ON g.visit_date = c.visit_date
   AND g.utm_source = c.utm_source
   AND g.utm_medium = c.utm_medium
   AND g.utm_campaign = c.utm_campaign
ORDER BY
    g.revenue DESC NULLS LAST,
    g.visit_date ASC,
    g.visitors_count DESC,
    g.utm_source ASC,
    g.utm_medium ASC,
    g.utm_campaign ASC;
