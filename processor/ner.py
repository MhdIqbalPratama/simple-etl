import sys
import os
import logging
from typing import List, Dict, Any, Tuple
import re
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services.staging_pg import PostgresService
from dotenv import load_dotenv


try:
    from transformers import pipeline
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False
    logging.warning("Transformers library not available. Install with: pip install transformers torch")

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ner_processor')

class NERProcessor:
    """Named Entity Recognition processor using NusaBert model"""
    
    def __init__(self):
        self.pg_service = PostgresService()
        self.ner_pipeline = None
        
        # Target entity types we're interested in
        self.target_entities = {
            'PER': 'Person',           # People
            'ORG': 'Organization',     # Organizations  
            'LAW': 'Law',             # Laws/Regulations
            'NOR': 'Political_Org'     # Political Organizations
        }
        
        self.stats = {
            'articles_processed': 0,
            'entities_extracted': 0,
            'errors': 0,
            'start_time': datetime.now()
        }
    
    def setup_ner_pipeline(self) -> bool:
        """Initialize the NER pipeline"""
        if not TRANSFORMERS_AVAILABLE:
            logger.error("Transformers library is required for NER processing")
            return False
        
        try:
            logger.info("Loading NusaBert NER model...")
            self.ner_pipeline = pipeline(
                "ner", 
                model="cahya/NusaBert-ner-v1.3",
                grouped_entities=True,
                device=-1  # Use CPU, set to 0 for GPU
            )
            logger.info("NER pipeline loaded successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load NER pipeline: {e}")
            return False
    
    def clean_text_for_ner(self, text: str) -> str:
        """Clean text before NER processing"""
        if not text:
            return ""
        text = text.lower()
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        
        # Remove email addresses
        text = re.sub(r'\S+@\S+', '', text)
        
        # Remove excessive punctuation
        text = re.sub(r'[^\w\s.,!?;:()\-"\']', ' ', text)
        
        # Limit length for processing efficiency
        if len(text) > 5000:  # Limit to ~5000 characters
            text = text[:5000] + "..."
        
        return text.strip()
    
    def _is_valid_entity(self, text: str, label: str) -> bool:
        """Validate if extracted entity is meaningful (only for PER, ORG, LAW, NOR)"""
        false_positives = {
            'PER': ['cnn', 'indonesia', 'jakarta', 'foto', 'video', 'berita', 'news'],
            'ORG': ['cnn', 'foto', 'video', 'berita', 'news'],
            'LAW': ['cnn', 'foto', 'video', 'berita'],
            'NOR': ['cnn', 'foto', 'video', 'berita', 'news']
        }

        text_lower = text.lower().strip()

        # Filter false positives
        if label in false_positives and text_lower in false_positives[label]:
            return False

        # Additional rules by entity type
        if label == 'PER':
            # Person names should be at least 3 characters and not all uppercase (unless long)
            if len(text) < 3 or (text.isupper() and len(text) < 5):
                return False

        elif label == 'ORG':
            # Organizations should be at least 3 chars
            if len(text) < 3:
                return False

        elif label == 'LAW':
            # Law references should have at least "UU", "Peraturan", "Undang"
            keywords = ['uu', 'undang', 'peraturan', 'pasal']
            if len(text) < 2 or not any(k in text_lower for k in keywords):
                return False

        elif label == 'NOR':
            # Political orgs should be at least 3 chars
            if len(text) < 3:
                return False

        return True

    
    def extract_entities(self, text: str) -> List[Dict[str, Any]]:
        """Extract named entities from text"""
        if not self.ner_pipeline or not text:
            return []
        
        try:
            # Clean text
            cleaned_text = self.clean_text_for_ner(text)
            if not cleaned_text:
                return []
            
            # Run NER
            ner_results = self.ner_pipeline(cleaned_text)
            
            # Filter and format results
            entities = []
            for entity in ner_results:
                entity_type = entity.get('entity_group', '').upper()
                
                # Only keep target entity types
                if entity_type in self.target_entities:
                    entity_text = entity.get('word', '').strip()
                    if self._is_valid_entity(entity_text, entity_type):
                        entities.append({
                            'entity_text': entity.get('word', '').strip(),
                            'entity_type': entity_type,
                            'confidence_score': round(entity.get('score', 0.0), 4),
                            'start_position': entity.get('start', 0),
                            'end_position': entity.get('end', 0)
                        })
            
            # Remove duplicates and low-confidence entities
            filtered_entities = self.filter_entities(entities)
            
            logger.debug(f"Extracted {len(filtered_entities)} entities from text of length {len(cleaned_text)}")
            return filtered_entities
            
        except Exception as e:
            logger.error(f"Error in entity extraction: {e}")
            return []
    
    def filter_entities(self, entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter entities based on confidence and remove duplicates"""
        if not entities:
            return []
        
        # Filter by confidence threshold
        min_confidence = 0.5
        high_conf_entities = [e for e in entities if e['confidence_score'] >= min_confidence]
        
        # Remove very short entities (likely noise)
        filtered_entities = [e for e in high_conf_entities if len(e['entity_text']) >= 2]
        
        # Deduplicate by text and type, keeping highest confidence
        entity_map = {}
        for entity in filtered_entities:
            key = (entity['entity_text'].lower(), entity['entity_type'])
            if key not in entity_map or entity['confidence_score'] > entity_map[key]['confidence_score']:
                entity_map[key] = entity
        
        # Sort by confidence score (highest first)
        result = list(entity_map.values())
        result.sort(key=lambda x: x['confidence_score'], reverse=True)
        
        return result
    
    def process_article(self, article: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process single article for entity extraction"""
        try:
            article_id = article.get('id')
            if not article_id:
                logger.warning("Article missing ID, skipping")
                return []
            
            # Combine title and content for NER
            title = article.get('title', '')
            content = article.get('content', '')
            full_text = f"{title}. {content}" if title else content
            
            if not full_text or len(full_text) < 50:
                logger.debug(f"Article {article_id} has insufficient text for NER")
                return []
            
            # Extract entities
            entities = self.extract_entities(full_text)
            
            # Add article ID to each entity
            for entity in entities:
                entity['article_id'] = article_id
            
            logger.debug(f"Processed article {article_id}: {len(entities)} entities found")
            return entities
            
        except Exception as e:
            logger.error(f"Error processing article {article.get('id', 'unknown')}: {e}")
            self.stats['errors'] += 1
            return []
    
    def process_batch(self, articles: List[Dict[str, Any]]) -> int:
        """Process batch of articles"""
        if not articles:
            return 0
        
        all_entities = []
        processed_count = 0
        
        logger.info(f"Processing batch of {len(articles)} articles...")
        
        for article in articles:
            try:
                entities = self.process_article(article)
                all_entities.extend(entities)
                processed_count += 1
                
                # Log progress for large batches
                if processed_count % 10 == 0:
                    logger.debug(f"Processed {processed_count}/{len(articles)} articles...")
                    
            except Exception as e:
                logger.error(f"Error in batch processing: {e}")
                continue
        
        # Insert entities to database
        if all_entities:
            inserted_count = self.pg_service.insert_entities(all_entities)
            self.stats['entities_extracted'] += inserted_count
            logger.info(f"Inserted {inserted_count} entities from {processed_count} articles")
        
        self.stats['articles_processed'] += processed_count
        return processed_count
    
    def process_unprocessed_articles(self, batch_size: int = 20) -> Dict[str, int]:
        """Process articles that haven't been processed for NER yet"""
        if not self.setup_ner_pipeline():
            return {'processed': 0, 'entities': 0}
        
        if not self.pg_service.connect():
            logger.error("Failed to connect to PostgreSQL")
            return {'processed': 0, 'entities': 0}
        
        try:
            logger.info("Starting NER processing...")
            
            total_processed = 0
            batch_num = 1
            
            while True:
                # Get batch of unprocessed articles
                articles = self.pg_service.get_unprocessed_articles_for_ner(batch_size)
                
                if not articles:
                    logger.info("No more articles to process")
                    break
                
                logger.info(f"Processing batch {batch_num} ({len(articles)} articles)...")
                
                # Process the batch
                processed_count = self.process_batch(articles)
                total_processed += processed_count
                
                batch_num += 1
                
                # Safety limit to prevent infinite loops
                if batch_num > 100:
                    logger.warning("Reached batch limit, stopping")
                    break
            
            result = {
                'processed': total_processed,
                'entities': self.stats['entities_extracted']
            }
            
            self.print_stats()
            return result
            
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
            return {'processed': 0, 'entities': 0}
        finally:
            self.pg_service.close()
    
    def print_stats(self):
        """Print processing statistics"""
        runtime = datetime.now() - self.stats['start_time']
        
        logger.info("NER Processing Statistics:")
        logger.info(f"   Runtime: {runtime}")
        logger.info(f"   Articles Processed: {self.stats['articles_processed']}")
        logger.info(f"   Entities Extracted: {self.stats['entities_extracted']}")
        logger.info(f"   Errors: {self.stats['errors']}")
        
        if self.stats['articles_processed'] > 0:
            avg_entities = self.stats['entities_extracted'] / self.stats['articles_processed']
            logger.info(f"   Average Entities per Article: {avg_entities:.2f}")

def run_ner_processing():
    """Main function to run NER processing"""
    processor = NERProcessor()
    result = processor.process_unprocessed_articles(batch_size=20)
    
    print(f"NER Processing completed:")
    print(f"  Articles processed: {result['processed']}")
    print(f"  Entities extracted: {result['entities']}")
    
    return result

if __name__ == "__main__":
    run_ner_processing()