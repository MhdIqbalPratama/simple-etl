BEGIN;

-- Drop existing views to recreate them
DROP VIEW IF EXISTS view_gold CASCADE;
DROP VIEW IF EXISTS view_gold_analytics CASCADE;
DROP VIEW IF EXISTS view_gold_trends CASCADE;
DROP VIEW IF EXISTS view_gold_entities CASCADE;

-- Create main gold view with enhanced analytics
CREATE VIEW view_gold AS
SELECT 
    s.id,
    s.title,
    s.link,
    s.image,
    s.date,
    s.topic,
    s.content,
    s.content_length,
    s.source,
    -- Content categorization
    CASE 
        WHEN s.content_length < 500 THEN 'Short'
        WHEN s.content_length < 1500 THEN 'Medium'
        WHEN s.content_length < 3000 THEN 'Long'
        ELSE 'Very Long'
    END as content_category,
    
    -- Time-based analytics
    EXTRACT(HOUR FROM s.date) as publish_hour,
    EXTRACT(DOW FROM s.date) as publish_day_of_week,
    EXTRACT(WEEK FROM s.date) as publish_week,
    EXTRACT(MONTH FROM s.date) as publish_month,
    EXTRACT(YEAR FROM s.date) as publish_year,
    DATE(s.date) as publish_date,
    
    -- Quality indicators
    CASE WHEN s.title IS NOT NULL AND LENGTH(s.title) > 10 THEN TRUE ELSE FALSE END as has_good_title,
    CASE WHEN s.content_length > 200 THEN TRUE ELSE FALSE END as has_substantial_content,
    CASE WHEN s.image IS NOT NULL AND s.image != '' THEN TRUE ELSE FALSE END as has_image,
    
    -- Topic categorization (enhanced)
    CASE 
        WHEN LOWER(s.topic) LIKE '%politik%' OR LOWER(s.topic) LIKE '%pemerintah%' THEN 'Politik'
        WHEN LOWER(s.topic) LIKE '%ekonom%' OR LOWER(s.topic) LIKE '%bisnis%' THEN 'Ekonomi'
        WHEN LOWER(s.topic) LIKE '%olahraga%' OR LOWER(s.topic) LIKE '%sport%' THEN 'Olahraga'
        WHEN LOWER(s.topic) LIKE '%teknolog%' OR LOWER(s.topic) LIKE '%digital%' THEN 'Teknologi'
        WHEN LOWER(s.topic) LIKE '%kesehatan%' OR LOWER(s.topic) LIKE '%medis%' THEN 'Kesehatan'
        WHEN LOWER(s.topic) LIKE '%pendidikan%' OR LOWER(s.topic) LIKE '%sekolah%' THEN 'Pendidikan'
        WHEN LOWER(s.topic) LIKE '%hukum%' OR LOWER(s.topic) LIKE '%kriminal%' THEN 'Hukum'
        WHEN LOWER(s.topic) LIKE '%internasional%' OR LOWER(s.topic) LIKE '%dunia%' THEN 'Internasional'
        ELSE COALESCE(s.topic, 'Umum')
    END as topic_category,
    
    -- Text analytics
    ARRAY_LENGTH(STRING_TO_ARRAY(s.content, ' '), 1) as word_count,
    ARRAY_LENGTH(STRING_TO_ARRAY(s.content, '.'), 1) as sentence_count,
    
    -- Search text
    CONCAT(s.title, ' ', s.content, ' ', COALESCE(s.topic, '')) as search_text,
    
    s.created_at,
    s.updated_at
FROM silver s
WHERE s.processed = TRUE
  AND s.title IS NOT NULL 
  AND s.content IS NOT NULL
  AND s.date IS NOT NULL
  AND s.content_length > 50; -- Filter out very short articles

-- Create analytics view for dashboard insights
CREATE VIEW view_gold_analytics AS
SELECT 
    topic_category,
    COUNT(*) as article_count,
    AVG(content_length) as avg_content_length,
    AVG(word_count) as avg_word_count,
    
    -- Time-based metrics
    COUNT(CASE WHEN publish_date = CURRENT_DATE THEN 1 END) as today_count,
    COUNT(CASE WHEN publish_date >= CURRENT_DATE - INTERVAL '7 days' THEN 1 END) as week_count,
    COUNT(CASE WHEN publish_date >= CURRENT_DATE - INTERVAL '30 days' THEN 1 END) as month_count,
    
    -- Quality metrics
    COUNT(CASE WHEN has_good_title THEN 1 END) as good_title_count,
    COUNT(CASE WHEN has_substantial_content THEN 1 END) as substantial_content_count,
    COUNT(CASE WHEN has_image THEN 1 END) as with_image_count,
    
    -- Engagement metrics (proxy)
    ROUND(AVG(content_length) / 100.0, 2) as readability_score,
    
    MIN(publish_date) as earliest_article,
    MAX(publish_date) as latest_article
FROM view_gold
GROUP BY topic_category
ORDER BY article_count DESC;

-- Create trends view for time-series analysis
CREATE VIEW view_gold_trends AS
SELECT 
    publish_date,
    topic_category,
    COUNT(*) as daily_count,
    AVG(content_length) as avg_length,
    STRING_AGG(DISTINCT LEFT(title, 50), '; ') as sample_titles,
    
    -- Weekly aggregates
    DATE_TRUNC('week', publish_date)::DATE as week_start,
    EXTRACT(DOW FROM publish_date) as day_of_week,
    EXTRACT(
        HOUR FROM TO_TIMESTAMP(AVG(EXTRACT(EPOCH FROM date)))
    ) as avg_publish_hour
    -- EXTRACT(HOUR FROM AVG(EXTRACT(EPOCH FROM date))) as avg_publish_hour
FROM view_gold
WHERE publish_date >= CURRENT_DATE - INTERVAL '90 days' -- Last 3 months
GROUP BY publish_date, topic_category, DATE_TRUNC('week', publish_date)::DATE
ORDER BY publish_date DESC, daily_count DESC;

-- Create NER entities view (placeholder for Python processing)
-- This will be populated by the NER processing pipeline
CREATE TABLE IF NOT EXISTS gold_entities (
    id SERIAL PRIMARY KEY,
    article_id VARCHAR(64) NOT NULL,
    entity_text TEXT NOT NULL,
    entity_type VARCHAR(20) NOT NULL, -- PER, ORG, LAW, NOR
    confidence_score FLOAT DEFAULT 0.0,
    start_position INTEGER,
    end_position INTEGER,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (article_id) REFERENCES silver(id) ON DELETE CASCADE
);

-- Create indexes for the entities table
CREATE INDEX IF NOT EXISTS idx_gold_entities_article_id ON gold_entities (article_id);
CREATE INDEX IF NOT EXISTS idx_gold_entities_type ON gold_entities (entity_type);
CREATE INDEX IF NOT EXISTS idx_gold_entities_text ON gold_entities (entity_text);
CREATE INDEX IF NOT EXISTS idx_gold_entities_processed_at ON gold_entities (processed_at);

-- Create view for entity insights
CREATE VIEW view_gold_entities AS
SELECT 
    ge.entity_type,
    ge.entity_text,
    COUNT(*) as mention_count,
    COUNT(DISTINCT ge.article_id) as article_count,
    AVG(ge.confidence_score) as avg_confidence,
    
    -- Article context
    STRING_AGG(DISTINCT g.topic_category, ', ') as topics_mentioned,
    MAX(g.publish_date) as latest_mention,
    MIN(g.publish_date) as first_mention,
    
    -- Popular articles mentioning this entity
    STRING_AGG(
        DISTINCT CASE 
            WHEN g.content_length > 1000 THEN LEFT(g.title, 100) 
            ELSE NULL 
        END, 
        '; '
    ) as popular_articles
    
FROM gold_entities ge
JOIN view_gold g ON ge.article_id = g.id
WHERE ge.entity_type IN ('PER', 'ORG', 'LAW', 'NOR') -- Focus on people, orgs, laws, political orgs
  AND ge.confidence_score > 0.7 -- High confidence only
  AND g.publish_date >= CURRENT_DATE - INTERVAL '30 days' -- Recent mentions
GROUP BY ge.entity_type, ge.entity_text
HAVING COUNT(*) >= 2 -- Must be mentioned at least twice
ORDER BY mention_count DESC, avg_confidence DESC;

-- Create function to get entity stats
CREATE OR REPLACE FUNCTION get_entity_stats(days_back INTEGER DEFAULT 7)
RETURNS TABLE(
    entity_type VARCHAR,
    total_entities BIGINT,
    unique_entities BIGINT,
    avg_confidence NUMERIC,
    top_entity TEXT,
    top_entity_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    WITH entity_stats AS (
        SELECT 
            ge.entity_type,
            COUNT(*) as total_count,
            COUNT(DISTINCT ge.entity_text) as unique_count,
            AVG(ge.confidence_score) as avg_conf,
            ge.entity_text,
            COUNT(*) OVER (PARTITION BY ge.entity_type, ge.entity_text) as entity_count,
            ROW_NUMBER() OVER (PARTITION BY ge.entity_type ORDER BY COUNT(*) DESC) as rn
        FROM gold_entities ge
        JOIN silver s ON ge.article_id = s.id
        WHERE s.date >= CURRENT_DATE - INTERVAL '1 day' * days_back
        GROUP BY ge.entity_type, ge.entity_text
    )
    SELECT 
        es.entity_type::VARCHAR,
        MAX(es.total_count) as total_entities,
        MAX(es.unique_count) as unique_entities,
        ROUND(MAX(es.avg_conf)::NUMERIC, 3) as avg_confidence,
        MAX(CASE WHEN es.rn = 1 THEN es.entity_text END)::TEXT as top_entity,
        MAX(CASE WHEN es.rn = 1 THEN es.entity_count END) as top_entity_count
    FROM entity_stats es
    GROUP BY es.entity_type
    ORDER BY es.entity_type;
END;
$$ LANGUAGE plpgsql;

-- Create indexes for gold views (on base silver table)
CREATE INDEX IF NOT EXISTS idx_silver_date_desc ON silver (date DESC) WHERE processed = TRUE;
CREATE INDEX IF NOT EXISTS idx_silver_topic_processed ON silver (topic) WHERE processed = TRUE;
CREATE INDEX IF NOT EXISTS idx_silver_content_length ON silver (content_length) WHERE processed = TRUE;

-- Update table statistics
ANALYZE silver;
ANALYZE gold_entities;

-- Log completion
DO $$
BEGIN
    RAISE NOTICE 'Gold layer views created successfully';
    RAISE NOTICE 'Views created: view_gold, view_gold_analytics, view_gold_trends, view_gold_entities';
    RAISE NOTICE 'Entity table created: gold_entities';
    RAISE NOTICE 'Ready for NER processing pipeline';
END $$;

COMMIT;