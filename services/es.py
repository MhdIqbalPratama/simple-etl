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
            # Define comprehensive mapping
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
                            "format": "strict_date_optional_time||epoch_millis"
                        },
                        "topic": {
                            "type": "keyword"
                        },
                        "source": {
                            "type": "keyword"
                        },
                        "processed_at": {
                            "type": "date"
                        }
                    }
                }
            }
            
            # Create index if it doesn't exist
            if not self.es.indices.exists(index=self.index_name):
                self.es.indices.create(index=self.index_name, body=mapping)
                logger.info(f"Index '{self.index_name}' created successfully")
            else:
                logger.info(f"Index '{self.index_name}' already exists")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup Elasticsearch index: {e}")
            return False
    
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
                        "processed_at": current_time
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
            
            logger.info(f"Indexed {success_count}/{len(articles)} articles to Elasticsearch")
            return success_count
            
        except Exception as e:
            logger.error(f"Bulk indexing failed: {e}")
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
                    {"date": {"order": "desc"}}
                ],
                "size": size
            }
            
            # Add topic filter
            if topic_filter:
                search_body["query"]["bool"]["filter"].append({
                    "term": {"topic": topic_filter}
                })
            
            # Add date range filter
            if date_range:
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
                    "topic": hit["_source"]["topic"],
                    "date": hit["_source"]["date"],
                    "score": hit["_score"],
                    "highlight": hit.get("highlight", {})
                }
                results.append(result)
            
            total_hits = response['hits']['total']['value']
            logger.info(f"🔍 Search '{query}' returned {len(results)}/{total_hits} results")
            
            return results
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []
    
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
            logger.error(f"Topic aggregation failed: {e}")
            return []
    
    def get_date_histogram(self, interval: str = "day") -> List[Dict[str, Any]]:
        """Get article count over time"""
        try:
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
            logger.error(f"Date histogram failed: {e}")
            return []
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive index statistics"""
        try:
            # Basic count
            count_response = self.es.count(index=self.index_name)
            total_docs = count_response['count']
            
            # Topic distribution
            topics = self.get_topic_aggregation()
            
            # Date range
            search_body = {
                "size": 0,
                "aggs": {
                    "date_stats": {
                        "stats": {
                            "field": "date"
                        }
                    },
                    "content_length_stats": {
                        "stats": {
                            "field": "content_length"
                        }
                    }
                }
            }
            
            response = self.es.search(index=self.index_name, body=search_body)
            
            stats = {
                "total_documents": total_docs,
                "topics": topics,
                "date_range": response['aggregations']['date_stats'],
                "content_stats": response['aggregations']['content_length_stats'],
                "index_name": self.index_name
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            return {"total_documents": 0, "topics": [], "error": str(e)}
    
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
            
            return {
                "cluster_status": health.get("status", "unknown"),
                "index_exists": index_exists,
                "document_count": doc_count,
                "cluster_name": health.get("cluster_name", "unknown")
            }
            
        except Exception as e:
            logger.error(f"Elasticsearch health check failed: {e}")
            return {
                "cluster_status": "error",
                "index_exists": False,
                "document_count": 0,
                "error": str(e)
            }
    
    def close(self):
        """Close Elasticsearch connection"""
        try:
            if hasattr(self.es, 'transport'):
                self.es.transport.close()
            logger.info("Elasticsearch connection closed")
        except Exception as e:
            logger.error(f"Error closing Elasticsearch connection: {e}")