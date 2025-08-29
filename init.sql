-- ================================
-- bronze_lv LAYER SETUP
-- ================================

-- Create bronze_lv table
CREATE TABLE IF NOT EXISTS bronze_lv (
    id VARCHAR(64) PRIMARY KEY,
    title TEXT,
    link TEXT UNIQUE NOT NULL,
    image TEXT,
    date_raw TEXT,
    topic TEXT,
    content TEXT,
    source VARCHAR(50) DEFAULT 'cnn_indonesia',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE
);

-- Create indexes for bronze_lv
CREATE INDEX IF NOT EXISTS idx_bronze_lv_processed ON bronze_lv (processed);
CREATE INDEX IF NOT EXISTS idx_bronze_lv_created_at ON bronze_lv (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_bronze_lv_link ON bronze_lv (link);



-- Create silver_lv table (simplified, no tsvector)
CREATE TABLE IF NOT EXISTS silver_lv (
    id VARCHAR(64) PRIMARY KEY,
    title TEXT NOT NULL,
    link TEXT UNIQUE NOT NULL,
    image TEXT,
    date TIMESTAMP WITH TIME ZONE,
    topic VARCHAR(100),
    content TEXT NOT NULL,
    content_length INTEGER,
    source VARCHAR(50) DEFAULT 'cnn_indonesia',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE
);

-- Create indexes for silver_lv
CREATE INDEX IF NOT EXISTS idx_silver_lv_processed ON silver_lv (processed);
CREATE INDEX IF NOT EXISTS idx_silver_lv_topic ON silver_lv (topic);
CREATE INDEX IF NOT EXISTS idx_silver_lv_date ON silver_lv (date DESC);
CREATE INDEX IF NOT EXISTS idx_silver_lv_link ON silver_lv (link);
CREATE INDEX IF NOT EXISTS idx_silver_lv_content_length ON silver_lv (content_length);

-- ================================
-- GOLD VIEWS FOR ANALYTICS
-- ================================

-- Main gold view with enhanced analytics
CREATE OR REPLACE VIEW view_gold_lv AS
SELECT 
    id,
    title,
    link,
    image,
    date,
    topic,
    content,
    content_length,
    source,
    -- Content categorization
    CASE 
        WHEN content_length < 500 THEN 'Short'
        WHEN content_length < 1500 THEN 'Medium'
        ELSE 'Long'
    END as content_category,
    -- Time-based analytics
    EXTRACT(HOUR FROM date) as publish_hour,
    EXTRACT(DOW FROM date) as publish_day_of_week,
    EXTRACT(WEEK FROM date) as publish_week,
    EXTRACT(MONTH FROM date) as publish_month,
    EXTRACT(YEAR FROM date) as publish_year,
    DATE(date) as publish_date,
    -- Quality indicators
    CASE WHEN title IS NOT NULL AND LENGTH(title) > 10 THEN TRUE ELSE FALSE END as has_good_title,
    CASE WHEN content_length > 200 THEN TRUE ELSE FALSE END as has_substantial_content,
    CASE WHEN image IS NOT NULL AND image != '' AND image != 'No image' THEN TRUE ELSE FALSE END as has_image,
    -- Timestamps
    created_at,
    updated_at
FROM silve  r_lv 
WHERE processed = TRUE
  AND title IS NOT NULL 
  AND content IS NOT NULL
  AND date IS NOT NULL;

-- Daily analytics view
CREATE OR REPLACE VIEW view_daily_analytics AS
SELECT 
    publish_date,
    COUNT(*) as total_articles,
    COUNT(DISTINCT topic) as unique_topics,
    AVG(content_length) as avg_content_length,
    COUNT(CASE WHEN content_category = 'Short' THEN 1 END) as short_articles,
    COUNT(CASE WHEN content_category = 'Medium' THEN 1 END) as medium_articles,
    COUNT(CASE WHEN content_category = 'Long' THEN 1 END) as long_articles,
    COUNT(CASE WHEN has_image = TRUE THEN 1 END) as articles_with_image
FROM view_gold_lv
GROUP BY publish_date
ORDER BY publish_date DESC;

-- Topic analytics view
CREATE OR REPLACE VIEW view_topic_analytics AS
SELECT 
    topic,
    COUNT(*) as total_articles,
    AVG(content_length) as avg_content_length,
    MIN(date) as first_published,
    MAX(date) as last_published,
    COUNT(CASE WHEN content_category = 'Long' THEN 1 END) as long_articles,
    COUNT(CASE WHEN has_image = TRUE THEN 1 END) as articles_with_image,
    ROUND(AVG(content_length), 2) as avg_length_rounded
FROM view_gold_lv
WHERE topic IS NOT NULL
GROUP BY topic
ORDER BY total_articles DESC;

-- Hourly publishing pattern view
CREATE OR REPLACE VIEW view_publishing_patterns AS
SELECT 
    publish_hour,
    COUNT(*) as total_articles,
    COUNT(DISTINCT topic) as unique_topics,
    AVG(content_length) as avg_content_length,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM view_gold_lv
GROUP BY publish_hour
ORDER BY publish_hour;

-- Weekly trends view
CREATE OR REPLACE VIEW view_weekly_trends AS
SELECT 
    publish_year,
    publish_week,
    MIN(publish_date) as week_start,
    MAX(publish_date) as week_end,
    COUNT(*) as total_articles,
    COUNT(DISTINCT topic) as unique_topics,
    AVG(content_length) as avg_content_length
FROM view_gold_lv
GROUP BY publish_year, publish_week
ORDER BY publish_year DESC, publish_week DESC;


-- Stored procedure for batch insert to bronze_lv
CREATE OR REPLACE FUNCTION sp_insert_bronze_lv(
    p_articles JSONB
) RETURNS TABLE (
    inserted_count INTEGER,
    updated_count INTEGER,
    error_count INTEGER
) AS $$
DECLARE 
    article JSONB;
    inserted_cnt INTEGER := 0;
    updated_cnt INTEGER := 0;
    error_cnt INTEGER := 0;
    existing_record BOOLEAN;
BEGIN
    -- Loop through articles array
    FOR article IN SELECT jsonb_array_elements(p_articles)
    LOOP
        BEGIN
            -- Check if record exists
            SELECT EXISTS(
                SELECT 1 FROM bronze_lv WHERE link = article->>'link'
            ) INTO existing_record;
            
            -- Insert or update
            INSERT INTO bronze_lv (
                id, title, link, image, date_raw, topic, content, source
            ) VALUES (
                article->>'id',
                article->>'title',
                article->>'link',
                article->>'image',
                article->>'date',
                article->>'topic',
                article->>'content',
                COALESCE(article->>'source', 'cnn_indonesia')
            )
            ON CONFLICT (link) DO UPDATE SET
                title = EXCLUDED.title,
                image = EXCLUDED.image,
                date_raw = EXCLUDED.date_raw,
                topic = EXCLUDED.topic,
                content = EXCLUDED.content,
                source = EXCLUDED.source,
                created_at = CURRENT_TIMESTAMP,
                processed = FALSE;
            
            IF existing_record THEN
                updated_cnt := updated_cnt + 1;
            ELSE
                inserted_cnt := inserted_cnt + 1;
            END IF;
            
        EXCEPTION WHEN OTHERS THEN
            error_cnt := error_cnt + 1;
            RAISE NOTICE 'Error processing article with link %: %', article->>'link', SQLERRM;
        END;
    END LOOP;
    
    RETURN QUERY SELECT inserted_cnt, updated_cnt, error_cnt;
END;
$$ LANGUAGE plpgsql;

-- Stored procedure for batch upsert to silver_lv
CREATE OR REPLACE FUNCTION sp_upsert_silver_lv(
    p_articles JSONB
) RETURNS TABLE (
    inserted_count INTEGER,
    updated_count INTEGER,
    error_count INTEGER
) AS $$
DECLARE 
    article JSONB;
    inserted_cnt INTEGER := 0;
    updated_cnt INTEGER := 0;
    error_cnt INTEGER := 0;
    existing_record BOOLEAN;
    parsed_date TIMESTAMP WITH TIME ZONE;
BEGIN
    -- Loop through articles array
    FOR article IN SELECT jsonb_array_elements(p_articles)
    LOOP
        BEGIN
            -- Parse date from string if needed
            IF article->>'date' IS NOT NULL AND article->>'date' != '' THEN
                BEGIN
                    parsed_date := (article->>'date')::TIMESTAMP WITH TIME ZONE;
                EXCEPTION WHEN OTHERS THEN
                    parsed_date := NULL;
                    RAISE NOTICE 'Could not parse date for article %: %', article->>'link', article->>'date';
                END;
            ELSE
                parsed_date := NULL;
            END IF;
            
            -- Check if record exists
            SELECT EXISTS(
                SELECT 1 FROM silver_lv WHERE link = article->>'link'
            ) INTO existing_record;
            
            -- Insert or update
            INSERT INTO silver_lv (
                id, title, link, image, date, topic, content, content_length, source
            ) VALUES (
                article->>'id',
                article->>'title',
                article->>'link',
                article->>'image',
                parsed_date,
                article->>'topic',
                article->>'content',
                COALESCE(LENGTH(article->>'content'), 0),
                COALESCE(article->>'source', 'cnn_indonesia')
            )
            ON CONFLICT (link) DO UPDATE SET
                title = EXCLUDED.title,
                image = EXCLUDED.image,
                date = EXCLUDED.date,
                topic = EXCLUDED.topic,
                content = EXCLUDED.content,
                content_length = EXCLUDED.content_length,
                source = EXCLUDED.source,
                updated_at = CURRENT_TIMESTAMP,
                processed = TRUE;
            
            IF existing_record THEN
                updated_cnt := updated_cnt + 1;
            ELSE
                inserted_cnt := inserted_cnt + 1;
            END IF;
            
        EXCEPTION WHEN OTHERS THEN
            error_cnt := error_cnt + 1;
            RAISE NOTICE 'Error processing article with link %: %', article->>'link', SQLERRM;
        END;
    END LOOP;
    
    RETURN QUERY SELECT inserted_cnt, updated_cnt, error_cnt;
END;
$$ LANGUAGE plpgsql;



-- Get bronze_lv statistics
CREATE OR REPLACE FUNCTION get_bronze_lv_stats()
RETURNS TABLE (
    total_records BIGINT,
    processed_records BIGINT,
    pending_records BIGINT,
    latest_created TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed_records,
        COUNT(CASE WHEN processed = FALSE THEN 1 END) as pending_records,
        MAX(created_at) as latest_created
    FROM bronze_lv;
END;
$$ LANGUAGE plpgsql;

-- Get silver_lv statistics
CREATE OR REPLACE FUNCTION get_silver_lv_stats()
RETURNS TABLE (
    total_records BIGINT,
    processed_records BIGINT,
    avg_content_length NUMERIC,
    latest_updated TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed_records,
        ROUND(AVG(content_length), 2) as avg_content_length,
        MAX(updated_at) as latest_updated
    FROM silver_lv;
END;
$$ LANGUAGE plpgsql;

