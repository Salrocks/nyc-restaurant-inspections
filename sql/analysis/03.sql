SELECT
    r.cuisine_description,
    COUNT(DISTINCT r.camis) AS restaurants,
    COUNT(*) AS inspections,
    ROUND(AVG(i.score), 1) AS avg_score,
    ROUND(100.0 * COUNT(*) FILTER (WHERE i.grade = 'A')
          / NULLIF(COUNT(*) FILTER (WHERE i.grade IN ('A','B','C')), 0), 1) AS pct_a
FROM fct_inspection i
JOIN dim_restaurant r ON r.camis = i.camis
WHERE i.score IS NOT NULL
  AND r.cuisine_description IS NOT NULL
GROUP BY r.cuisine_description
HAVING COUNT(DISTINCT r.camis) >= 50
ORDER BY avg_score DESC
LIMIT 15;