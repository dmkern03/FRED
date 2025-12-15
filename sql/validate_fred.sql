-- =============================================================================
-- FRED Data Pipeline - Validation Queries
-- =============================================================================

-- Row counts across all layers
SELECT 'bronze_rates' AS layer_table, COUNT(*) AS rows FROM investments.fred.bronze_rates
UNION ALL
SELECT 'bronze_metadata', COUNT(*) FROM investments.fred.bronze_metadata
UNION ALL
SELECT 'silver_rates', COUNT(*) FROM investments.fred.silver_rates
UNION ALL
SELECT 'silver_metadata', COUNT(*) FROM investments.fred.silver_metadata
UNION ALL
SELECT 'gold_rates', COUNT(*) FROM investments.fred.gold_rates;

-- Series coverage
SELECT 
    m.series_id,
    m.friendly_name,
    m.frequency,
    COUNT(r.date) AS observation_count,
    MIN(r.date) AS first_date,
    MAX(r.date) AS last_date
FROM investments.fred.silver_metadata m
LEFT JOIN investments.fred.silver_rates r ON m.series_id = r.series_id
GROUP BY m.series_id, m.friendly_name, m.frequency
ORDER BY m.series_id;

-- Latest values
SELECT 
    series_id,
    friendly_name,
    date,
    value,
    units
FROM investments.fred.gold_rates
WHERE date = (SELECT MAX(date) FROM investments.fred.gold_rates g2 WHERE g2.series_id = gold_rates.series_id)
ORDER BY series_id;

-- Data freshness
SELECT 
    series_id,
    MAX(date) AS latest_date,
    DATEDIFF(CURRENT_DATE, MAX(date)) AS days_since_update
FROM investments.fred.silver_rates
GROUP BY series_id
ORDER BY days_since_update DESC;

-- Constraint validation
SELECT 
    'Orphaned rates (no metadata)' AS check_name,
    COUNT(*) AS count
FROM investments.fred.silver_rates r
LEFT JOIN investments.fred.silver_metadata m ON r.series_id = m.series_id
WHERE m.series_id IS NULL;
