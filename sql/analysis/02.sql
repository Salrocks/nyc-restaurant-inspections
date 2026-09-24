select
	violation_code,
	MIN(violation_description) as description,
	COUNT(*) as times_cited,
	ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_critical
from fct_violation fv
where is_critical is true
group by violation_code
order by times_cited desc
limit 10
