from elasticsearch import Elasticsearch, helpers
import json
import logging
from typing import List, Dict, Any, Optional
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
logger = logging.getLogger(__name__)

class ElasticsearchService:
    """Enhanced Elasticsearch service for news article indexing and search"""
    
    def __init__(self):
        self.index_name = os.getenv('ES_INDEX_NAME')
        
        # Initialize Elasticsearch client
        if os.getenv('ES_CLOUD_URL') and os.getenv('ES_API_KEY'):
            # Cloud Elasticsearch
            self.es = Elasticsearch(
                os.getenv('ES_CLOUD_URL'),
                api_key=os.getenv('ES_API_KEY'),
                request_timeout=30,
                retry_on_timeout=True,
                max_retries=3
            )
            logger.info("Using Elasticsearch Cloud")
        else:
            # Local Elasticsearch
            host = os.getenv('ELASTICSEARCH_HOST', 'localhost:9200')
            self.es = Elasticsearch(
                [f"http://{host}"],
                request_timeout=30,
                retry_on_timeout=True,
                max_retries=3
            )
            logger.info(f"Using local Elasticsearch: {host}")
    
    def setup_index(self) -> bool:
        """Create index with optimized mappings for news articles"""
        try:
            # Define comprehensive mapping with proper date field
            mapping = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "analysis": {
                        "analyzer": {
                            "indonesian_analyzer": {
                                "type": "custom",
                                "tokenizer": "standard",
                                "filter": [
                                    "lowercase",
                                    "stop"
                                ]
                            }
                        }
                    }
                },
                "mappings": {
                    "properties": {
                        "id": {
                            "type": "keyword"
                        },
                        "title": {
                            "type": "text",
                            "analyzer": "indonesian_analyzer",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "content": {
                            "type": "text",
                            "analyzer": "indonesian_analyzer"
                        },
                        "link": {
                            "type": "keyword"
                        },
                        "image": {
                            "type": "keyword",
                            "index": False
                        },
                        "date": {
                            "type": "date",
                            "format": "strict_date_optional_time||epoch_millis||yyyy-MM-dd||yyyy-MM-dd HH:mm:ss"
                        },
                        "topic": {
                            "type": "keyword"
                        },
                        "source": {
                            "type": "keyword"
                        },
                        "processed_at": {
                            "type": "date"
                        },
                        "content_length": {
                            "type": "integer"
                        }
                    }
                }
            }
            
            # Create index if it doesn't exist
            if not self.es.indices.exists(index=self.index_name):
                self.es.indices.create(index=self.index_name, body=mapping)
                logger.info(f"✅ Index '{self.index_name}' created successfully")
            else:
                logger.info(f"✅ Index '{self.index_name}' already exists")
                
                # Check if we need to update mapping for existing index
                self._check_and_update_mapping()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to setup Elasticsearch index: {e}")
            return False
    
    def _check_and_update_mapping(self):
        """Check current mapping and suggest fixes if needed"""
        try:
            mapping = self.es.indices.get_mapping(index=self.index_name)
            current_mapping = mapping[self.index_name]['mappings']['properties']
            
            # Check if date field is properly mapped
            if 'date' in current_mapping:
                date_type = current_mapping['date'].get('type', 'text')
                if date_type != 'date':
                    logger.warning(f"⚠️ Date field is mapped as '{date_type}' instead of 'date'. Consider reindexing for better performance.")
            
        except Exception as e:
            logger.error(f"Failed to check mapping: {e}")
    
    def bulk_index(self, articles: List[Dict[str, Any]]) -> int:
        """Bulk index articles to Elasticsearch"""
        if not articles:
            logger.warning("No articles to index")
            return 0
        
        try:
            actions = []
            current_time = datetime.now().isoformat()
            
            for article in articles:
                # Prepare document for indexing
                doc = {
                    "_index": self.index_name,
                    "_id": article.get("id"),
                    "_source": {
                        "id": article.get("id"),
                        "title": article.get("title"),
                        "content": article.get("content"),
                        "link": article.get("link"),
                        "image": article.get("image"),
                        "date": article.get("date"),
                        "topic": article.get("topic"),
                        "source": article.get("source", "cnn_indonesia"),
                        "processed_at": current_time,
                        "content_length": len(article.get("content", ""))
                    }
                }
                actions.append(doc)
            
            # Perform bulk indexing
            success_count, failed_items = helpers.bulk(
                self.es,
                actions,
                index=self.index_name,
                refresh=True,
                request_timeout=60,
                chunk_size=100
            )
            
            if failed_items:
                logger.warning(f"⚠️ {len(failed_items)} documents failed to index")
                for failed_item in failed_items[:3]:  # Log first 3 failures
                    logger.warning(f"Failed: {failed_item}")
            
            logger.info(f"✅ Indexed {success_count}/{len(articles)} articles to Elasticsearch")
            return success_count
            
        except Exception as e:
            logger.error(f"❌ Bulk indexing failed: {e}")
            return 0
    
    def search_articles(self, query: str, size: int = 20, 
                       topic_filter: Optional[str] = None,
                       date_range: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        """Search articles with advanced filtering"""
        try:
            # Build query
            search_body = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "multi_match": {
                                    "query": query,
                                    "fields": ["title^3", "content^1"],
                                    "type": "best_fields",
                                    "fuzziness": "AUTO"
                                }
                            }
                        ],
                        "filter": []
                    }
                },
                "highlight": {
                    "fields": {
                        "title": {},
                        "content": {
                            "fragment_size": 150,
                            "number_of_fragments": 3
                        }
                    }
                },
                "sort": [
                    {"_score": {"order": "desc"}},
                    {"date": {"order": "desc", "unmapped_type": "date"}}
                ],
                "size": size
            }
            
            # Add topic filter
            if topic_filter:
                search_body["query"]["bool"]["filter"].append({
                    "term": {"topic": topic_filter}
                })
            
            # Add date range filter (only if date field is properly mapped)
            if date_range and self._is_date_field_available():
                search_body["query"]["bool"]["filter"].append({
                    "range": {"date": date_range}
                })
            
            response = self.es.search(index=self.index_name, body=search_body)
            
            # Process results
            results = []
            for hit in response['hits']['hits']:
                result = {
                    "id": hit["_source"]["id"],
                    "title": hit["_source"]["title"],
                    "content": hit["_source"]["content"],
                    "link": hit["_source"]["link"],
                    "topic": hit["_source"].get("topic"),
                    "date": hit["_source"].get("date"),
                    "score": hit["_score"],
                    "highlight": hit.get("highlight", {})
                }
                results.append(result)
            
            total_hits = response['hits']['total']['value']
            logger.info(f"🔍 Search '{query}' returned {len(results)}/{total_hits} results")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Search failed: {e}")
            return []
    
    def _is_date_field_available(self) -> bool:
        """Check if date field is properly mapped for aggregations"""
        try:
            mapping = self.es.indices.get_mapping(index=self.index_name)
            current_mapping = mapping[self.index_name]['mappings']['properties']
            
            if 'date' in current_mapping:
                date_type = current_mapping['date'].get('type', 'text')
                return date_type == 'date'
            
            return False
        except:
            return False
    
    def get_topic_aggregation(self) -> List[Dict[str, Any]]:
        """Get article count by topic"""
        try:
            search_body = {
                "size": 0,
                "aggs": {
                    "topics": {
                        "terms": {
                            "field": "topic",
                            "size": 20,
                            "order": {"_count": "desc"}
                        }
                    }
                }
            }
            
            response = self.es.search(index=self.index_name, body=search_body)
            
            topics = []
            for bucket in response['aggregations']['topics']['buckets']:
                topics.append({
                    "topic": bucket["key"],
                    "count": bucket["doc_count"]
                })
            
            return topics
            
        except Exception as e:
            logger.error(f"❌ Topic aggregation failed: {e}")
            return []
    
    def get_date_histogram(self, interval: str = "day") -> List[Dict[str, Any]]:
        """Get article count over time - with fallback if date field is not available"""
        try:
            # First check if date field is properly mapped
            if not self._is_date_field_available():
                logger.warning("⚠️ Date field not properly mapped for aggregations. Returning empty histogram.")
                return []
            
            search_body = {
                "size": 0,
                "aggs": {
                    "articles_over_time": {
                        "date_histogram": {
                            "field": "date",
                            "calendar_interval": interval,
                            "order": {"_key": "desc"}
                        }
                    }
                }
            }
            
            response = self.es.search(index=self.index_name, body=search_body)
            
            histogram = []
            for bucket in response['aggregations']['articles_over_time']['buckets']:
                histogram.append({
                    "date": bucket["key_as_string"],
                    "count": bucket["doc_count"]
                })
            
            return histogram
            
        except Exception as e:
            logger.error(f"❌ Date histogram failed: {e}")
            # Return mock data or empty list for graceful degradation
            return []
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive index statistics with graceful error handling"""
        try:
            # Basic count
            count_response = self.es.count(index=self.index_name)
            total_docs = count_response['count']
            
            # Topic distribution
            topics = self.get_topic_aggregation()
            
            # Initialize stats with basic info
            stats = {
                "total_documents": total_docs,
                "topics": topics,
                "index_name": self.index_name,
                "date_histogram_available": self._is_date_field_available()
            }
            
            # Try to get date and content stats only if fields are properly mapped
            try:
                if self._is_date_field_available():
                    search_body = {
                        "size": 0,
                        "aggs": {
                            "date_stats": {
                                "stats": {
                                    "field": "date"
                                }
                            }
                        }
                    }
                    
                    response = self.es.search(index=self.index_name, body=search_body)
                    stats["date_range"] = response['aggregations']['date_stats']
                else:
                    stats["date_range"] = {"min": None, "max": None, "count": 0}
                    
            except Exception as date_error:
                logger.warning(f"⚠️ Could not get date statistics: {date_error}")
                stats["date_range"] = {"error": "Date field not properly mapped"}
            
            # Try to get content length stats
            try:
                search_body = {
                    "size": 0,
                    "aggs": {
                        "content_length_stats": {
                            "stats": {
                                "field": "content_length"
                            }
                        }
                    }
                }
                
                response = self.es.search(index=self.index_name, body=search_body)
                stats["content_stats"] = response['aggregations']['content_length_stats']
                
            except Exception as content_error:
                logger.warning(f"⚠️ Could not get content statistics: {content_error}")
                # Calculate content length from actual documents as fallback
                try:
                    sample_response = self.es.search(
                        index=self.index_name, 
                        body={"size": 100, "_source": ["content"]},
                        timeout="10s"
                    )
                    
                    lengths = []
                    for hit in sample_response['hits']['hits']:
                        content = hit["_source"].get("content", "")
                        lengths.append(len(content))
                    
                    if lengths:
                        stats["content_stats"] = {
                            "avg": sum(lengths) / len(lengths),
                            "min": min(lengths),
                            "max": max(lengths),
                            "count": len(lengths),
                            "sum": sum(lengths)
                        }
                    else:
                        stats["content_stats"] = {"avg": 0, "min": 0, "max": 0, "count": 0}
                        
                except Exception:
                    stats["content_stats"] = {"error": "Content length not available"}
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Failed to get statistics: {e}")
            return {
                "total_documents": 0, 
                "topics": [], 
                "error": str(e),
                "date_histogram_available": False
            }
    
    def health_check(self) -> Dict[str, Any]:
        """Check Elasticsearch cluster health"""
        try:
            # Cluster health
            health = self.es.cluster.health()
            
            # Index info
            index_exists = self.es.indices.exists(index=self.index_name)
            
            # Document count
            doc_count = 0
            if index_exists:
                count_response = self.es.count(index=self.index_name)
                doc_count = count_response['count']
            
            # Check date field mapping
            date_field_ok = False
            mapping_issues = []
            
            if index_exists:
                try:
                    date_field_ok = self._is_date_field_available()
                    if not date_field_ok:
                        mapping_issues.append("Date field not properly mapped for aggregations")
                except:
                    mapping_issues.append("Cannot check date field mapping")
            
            return {
                "cluster_status": health.get("status", "unknown"),
                "index_exists": index_exists,
                "document_count": doc_count,
                "cluster_name": health.get("cluster_name", "unknown"),
                "date_field_available": date_field_ok,
                "mapping_issues": mapping_issues
            }
            
        except Exception as e:
            logger.error(f"❌ Elasticsearch health check failed: {e}")
            return {
                "cluster_status": "error",
                "index_exists": False,
                "document_count": 0,
                "error": str(e),
                "date_field_available": False,
                "mapping_issues": ["Cannot connect to Elasticsearch"]
            }
    
    def fix_date_mapping(self) -> bool:
        """Attempt to fix date mapping issues by reindexing"""
        try:
            logger.info("🔧 Attempting to fix date mapping...")
            
            # Create new index with correct mapping
            temp_index = f"{self.index_name}_temp"
            
            # Delete temp index if exists
            if self.es.indices.exists(index=temp_index):
                self.es.indices.delete(index=temp_index)
            
            # Create temp index with correct mapping
            mapping = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "content": {"type": "text"},
                        "link": {"type": "keyword"},
                        "image": {"type": "keyword", "index": False},
                        "date": {"type": "date", "format": "strict_date_optional_time||epoch_millis||yyyy-MM-dd||yyyy-MM-dd HH:mm:ss"},
                        "topic": {"type": "keyword"},
                        "source": {"type": "keyword"},
                        "processed_at": {"type": "date"},
                        "content_length": {"type": "integer"}
                    }
                }
            }
            
            self.es.indices.create(index=temp_index, body=mapping)
            
            # Reindex data
            reindex_body = {
                "source": {"index": self.index_name},
                "dest": {"index": temp_index}
            }
            
            self.es.reindex(body=reindex_body, wait_for_completion=True, timeout="5m")
            
            # Delete old index and create alias
            self.es.indices.delete(index=self.index_name)
            self.es.indices.create(index=self.index_name, body=mapping)
            
            # Reindex from temp to main
            reindex_body["source"]["index"] = temp_index
            reindex_body["dest"]["index"] = self.index_name
            
            self.es.reindex(body=reindex_body, wait_for_completion=True, timeout="5m")
            
            # Clean up temp index
            self.es.indices.delete(index=temp_index)
            
            logger.info("✅ Date mapping fixed successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to fix date mapping: {e}")
            return False
    
    def close(self):
        """Close Elasticsearch connection"""
        try:
            if hasattr(self.es, 'transport'):
                self.es.transport.close()
            logger.info("✅ Elasticsearch connection closed")
        except Exception as e:
            logger.error(f"❌ Error closing Elasticsearch connection: {e}")