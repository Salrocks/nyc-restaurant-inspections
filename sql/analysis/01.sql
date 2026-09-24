select
	dr.boro,
	COUNT(*) filter (where fi.grade = 'A') as grade_a,
	COUNT(*) filter (where fi.grade = 'B') as grade_b,
	COUNT(*) filter (where fi.grade = 'C') as grade_c,
	COUNT(*) as graded_inspections,
	ROUND(100.0 * COUNT(*) FILTER (WHERE fi.grade = 'A') / COUNT(*), 1) AS pct_a
from fct_inspection fi
left join dim_restaurant dr
on fi.camis = dr.camis
where fi.grade in ('A','B','C') and dr.boro is not null
group by dr.boro
order by dr.boro DESC
