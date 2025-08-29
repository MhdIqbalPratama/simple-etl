BEGIN;

DO $$
BEGIN
    -- Update table statistics for better query performance
    ANALYZE silver;
    ANALYZE gold_entities;
    
    -- Log refresh activity
    RAISE NOTICE 'Gold views refreshed at %', CURRENT_TIMESTAMP;
    RAISE NOTICE 'Updated statistics for silver and gold_entities tables';
END $$;

-- Update any computed columns or derived data if needed
-- This is where you could add triggers or computed fields

-- Get current statistics for logging
DO $$
DECLARE
    silver_count INTEGER;
    entities_count INTEGER;
    recent_articles INTEGER;
    entity_types_summary TEXT;
BEGIN
    -- Get silver table count
    SELECT COUNT(*) INTO silver_count FROM silver WHERE processed = TRUE;
    
    -- Get entities count
    SELECT COUNT(*) INTO entities_count FROM gold_entities;
    
    -- Get recent articles (last 24 hours)
    SELECT COUNT(*) INTO recent_articles 
    FROM silver 
    WHERE processed = TRUE 
      AND updated_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours';
    
    -- Get entity types summary
    SELECT STRING_AGG(
        FORMAT('%s: %s', entity_type, cnt), 
        ', ' ORDER BY cnt DESC
    ) INTO entity_types_summary
    FROM (
        SELECT entity_type, COUNT(*) as cnt
        FROM gold_entities
        WHERE processed_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
        GROUP BY entity_type
        ORDER BY cnt DESC
        LIMIT 5
    ) t;
    
    -- Log statistics
    RAISE NOTICE 'Current Statistics:';
    RAISE NOTICE '  Silver articles: %', silver_count;
    RAISE NOTICE '  Total entities: %', entities_count;
    RAISE NOTICE '  Recent articles (24h): %', recent_articles;
    RAISE NOTICE '  Recent entity types: %', COALESCE(entity_types_summary, 'None');
    
END $$;

COMMIT;