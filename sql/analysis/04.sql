WITH graded AS (
    SELECT
        camis,
        inspection_date,
        grade,
        LEAD(grade) OVER (PARTITION BY camis ORDER BY inspection_date) AS next_grade
    FROM fct_inspection
    WHERE grade IN ('A', 'B', 'C')
      AND inspection_date IS NOT NULL
)
SELECT
    grade AS starting_grade,
    COUNT(*) FILTER (WHERE next_grade IS NOT NULL) AS with_followup,
    COUNT(*) FILTER (WHERE next_grade = 'A') AS improved_to_a,
    ROUND(100.0 * COUNT(*) FILTER (WHERE next_grade = 'A')
          / NULLIF(COUNT(*) FILTER (WHERE next_grade IS NOT NULL), 0), 1) AS pct_to_a
FROM graded
WHERE grade IN ('B', 'C')
GROUP BY grade
ORDER BY grade;
