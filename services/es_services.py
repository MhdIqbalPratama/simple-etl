from elasticsearch import Elasticsearch, helpers
import json
from dotenv import load_dotenv
import os


class ESService:
    def __init__(self):

        load_dotenv()

        self.cloud = os.getenv('ES_CLOUD_URL')
        self.api_key = os.getenv('ES_API_KEY')

        self.es = Elasticsearch(
            self.cloud,
            api_key=self.api_key
        )

        self.index_name = "news_index"
    
    def setup_index(self):
        #Create index with mappings
        mapping = {
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "title": {"type": "text", "analyzer": "standard"},
                    "content": {"type": "text", "analyzer": "standard"},
                    "link": {"type": "keyword"},
                    "image": {"type": "keyword"},
                    "date": {"type": "date"},
                    "topic": {"type": "keyword"}
                }
            }
        } 
    
        try:
            if not self.es.indices.exists(index=self.index_name):
                self.es.indices.create(index=self.index_name, body=mapping)
                print(f"Index '{self.index_name}' created successfully.")
            else:
                print(f"Index '{self.index_name}' already exists.")
            
        except Exception as e:
            print(f"An error occurred while setting up the index: {e}")
    
    def save_news(self, news_data):
        # Save news data to Elasticsearch
        doc = {
            "id": news_data.get("id"),
            "title": news_data.get("title"),
            "content": news_data.get("content"),
            "link": news_data.get("link"),
            "image": news_data.get("image"),
            "date": news_data.get("date")
        }

        try:
            result = self.es.index(index=self.index_name, id=doc["id"], body=doc)
            return True
        except Exception as e:
            print(f"An error occurred while saving news data: {e}")
            return False
        
    def save_bulk(self, news_list):
        if not news_list:
            print("No news data to save.")
            return False
        
        action = []
        for news_data in news_list:
            doc = {
                "_index": self.index_name,
                "_id": news_data.get("id"),
                "_source": {
                    "title": news_data.get("title"),
                    "content": news_data.get("content"),
                    "link": news_data.get("link"),
                    "image": news_data.get("image"),
                    "date": news_data.get("date"),
                    "topic": news_data.get("topic")
                }
            }
            action.append(doc) 
            try:
                success_count, failed_count = helpers.bulk(self.es, action, index=self.index_name, refresh=True)
                print(f"Successfully indexed {success_count} documents.")

                return success_count
            
            except Exception as e:
                print(f"An error occurred during bulk indexing: {e}")
                return False
    
    def search_news(self, query, size=10):
        """Search for news articles in Elasticsearch based on a query."""
        search_body = {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["title^2", "content", "search_text"]
                }
            },
            "sort": [{"date": {"order": "desc"}}],
            "size": size
        }

        try:
            response = self.es.search(index=self.index_name, body=search_body)
            return response['hits']['hits']
        except Exception as e:
            print(f"An error occurred while searching for news: {e}")
            return []