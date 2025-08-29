-- File: airflow_home/dags/sql/process_bronze_to_silver.sql
-- Process bronze data to silver table in batches with enhanced content cleaning

BEGIN;

-- Create or replace the stored procedure for processing bronze to silver
CREATE OR REPLACE FUNCTION process_bronze_to_silver_batch()
RETURNS TABLE(
    processed_count INTEGER,
    error_count INTEGER,
    processing_summary TEXT
) AS $$
DECLARE
    v_processed_count INTEGER := 0;
    v_error_count INTEGER := 0;
    v_batch_size INTEGER := 100;
    v_total_unprocessed INTEGER;
    v_batch_start INTEGER := 0;
    rec RECORD;
BEGIN
    -- Get total unprocessed records
    SELECT COUNT(*) INTO v_total_unprocessed 
    FROM bronze 
    WHERE processed = FALSE;
    
    RAISE NOTICE 'Starting batch processing of % unprocessed bronze records', v_total_unprocessed;
    
    -- Process in batches
    WHILE v_batch_start < v_total_unprocessed LOOP
        -- Process current batch
        FOR rec IN (
            SELECT 
                b.id,
                COALESCE(TRIM(b.title), '') as title,
                b.link,
                b.image,
                CASE 
                    WHEN b.date_raw IS NOT NULL AND b.date_raw != '' THEN
                        CASE 
                            -- Try to parse various date formats
                            WHEN b.date_raw ~ '^\d{4}-\d{2}-\d{2}' THEN b.date_raw::TIMESTAMP
                            WHEN b.date_raw ~ '^\d{2}/\d{2}/\d{4}' THEN TO_TIMESTAMP(b.date_raw, 'DD/MM/YYYY')
                            WHEN b.date_raw ~ '^\d{2}-\d{2}-\d{4}' THEN TO_TIMESTAMP(b.date_raw, 'DD-MM-YYYY')
                            ELSE CURRENT_TIMESTAMP
                        END
                    ELSE CURRENT_TIMESTAMP
                END as parsed_date,
                b.topic,
                -- Enhanced content cleaning to match Python function
                CASE 
                    WHEN b.content IS NOT NULL THEN
                        TRIM(
                            -- Step 8: Remove leading location pattern like "Jakarta, CNN Indonesia -- "
                            REGEXP_REPLACE(
                                -- Step 7: Normalize spaces (collapse multiple spaces/tabs)
                                REGEXP_REPLACE(
                                    -- Step 6: Normalize excessive newlines (convert 3+ to just 2)
                                    REGEXP_REPLACE(
                                        -- Step 5: Remove video embed tags [Gambas:Video ...]
                                        REGEXP_REPLACE(
                                            -- Step 4b: Remove CNN credit notes
                                            REGEXP_REPLACE(
                                                -- Step 4a: Remove photo/credit notes like (ANTARA FOTO/...)
                                                REGEXP_REPLACE(
                                                    -- Step 3: Remove "Lihat Juga" sections
                                                    REGEXP_REPLACE(
                                                        -- Step 2: Remove "Pilihan Redaksi" sections
                                                        REGEXP_REPLACE(
                                                            -- Step 1: Remove advertisements and scroll prompts
                                                            REGEXP_REPLACE(
                                                                b.content,
                                                                'ADVERTISEMENT.*?SCROLL TO CONTINUE WITH CONTENT', 
                                                                '', 
                                                                'gis'
                                                            ),
                                                            'Pilihan Redaksi.*?(?=[A-Z0-9])', 
                                                            '', 
                                                            'gs'
                                                        ),
                                                        'Lihat Juga\s*:.*?(?=\n|$)', 
                                                        '', 
                                                        'gm'
                                                    ),
                                                    '\([^)]*FOTO[^)]*\)', 
                                                    '', 
                                                    'gi'
                                                ),
                                                '\([^)]*CNN[^)]*\)', 
                                                '', 
                                                'gi'
                                            ),
                                            '\[Gambas:.*?\]', 
                                            '', 
                                            'g'
                                        ),
                                        '\n{3,}', 
                                        E'\n\n', 
                                        'g'
                                    ),
                                    '\s+', 
                                    ' ', 
                                    'g'
                                ),
                                '^[A-Za-z\s,]+CNN Indonesia\s*--\s*', 
                                '', 
                                ''
                            )
                        )
                    ELSE ''
                END as cleaned_content,
                b.source
            FROM bronze b
            WHERE processed = FALSE
            ORDER BY created_at DESC
            LIMIT v_batch_size OFFSET v_batch_start
        ) LOOP
            BEGIN
                -- Insert or update silver table
                INSERT INTO silver (
                    id, 
                    title, 
                    link, 
                    image, 
                    date, 
                    topic, 
                    content, 
                    content_length,
                    source,
                    processed
                ) VALUES (
                    rec.id,
                    rec.title,
                    rec.link,
                    rec.image,
                    rec.parsed_date,
                    rec.topic,
                    rec.cleaned_content,
                    LENGTH(rec.cleaned_content),
                    rec.source,
                    TRUE
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
                
                -- Mark bronze record as processed
                UPDATE bronze 
                SET processed = TRUE 
                WHERE id = rec.id;
                
                v_processed_count := v_processed_count + 1;
                
            EXCEPTION WHEN OTHERS THEN
                RAISE WARNING 'Error processing record %: %', rec.id, SQLERRM;
                v_error_count := v_error_count + 1;
                
                -- Mark bronze record as processed even if there was an error
                -- to prevent reprocessing the same problematic record
                UPDATE bronze 
                SET processed = TRUE 
                WHERE id = rec.id;
            END;
        END LOOP;
        
        v_batch_start := v_batch_start + v_batch_size;
        
        -- Log progress
        IF v_batch_start % (v_batch_size * 5) = 0 THEN
            RAISE NOTICE 'Processed % records so far...', v_processed_count;
        END IF;
    END LOOP;
    
    -- Return results
    RETURN QUERY SELECT 
        v_processed_count,
        v_error_count,
        FORMAT('Processed %s records, %s errors, from %s total unprocessed', 
               v_processed_count, v_error_count, v_total_unprocessed);
    
    RAISE NOTICE 'Bronze to Silver processing completed. Processed: %, Errors: %', 
                 v_processed_count, v_error_count;
    
END;
$$ LANGUAGE plpgsql;

-- Execute the batch processing
DO $$
DECLARE
    result RECORD;
BEGIN
    -- Call the processing function
    FOR result IN SELECT * FROM process_bronze_to_silver_batch() LOOP
        RAISE NOTICE 'Processing Summary: %', result.processing_summary;
    END LOOP;
    
    -- Update statistics
    ANALYZE bronze;
    ANALYZE silver;
    
END $$;

COMMIT;