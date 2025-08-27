import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import re
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from services.staging_pg import PostgresService
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Indonesian stopwords
INDONESIAN_STOPWORDS = {
    'yang', 'dan', 'di', 'ke', 'dari', 'dalam', 'untuk', 'pada', 'dengan', 'ini', 'itu', 'adalah', 'akan', 
    'telah', 'sudah', 'dapat', 'bisa', 'juga', 'tidak', 'atau', 'serta', 'oleh', 'sebagai', 'karena', 
    'saat', 'ketika', 'sebelum', 'sesudah', 'antara', 'namun', 'tetapi', 'jika', 'maka', 'bila', 'kita', 
    'kami', 'mereka', 'dia', 'ia', 'nya', 'mu', 'ku', 'anda', 'saya', 'kamu', 'beliau', 'para', 'semua', 
    'setiap', 'masing', 'beberapa', 'banyak', 'sedikit', 'lebih', 'kurang', 'paling', 'sangat', 'amat', 
    'begitu', 'sekali', 'lagi', 'masih', 'sedang', 'tengah', 'baru', 'lama', 'dulu', 'nanti', 'sekarang', 
    'hari', 'waktu', 'tahun', 'bulan', 'minggu', 'jam', 'menit', 'detik', 'pagi', 'siang', 'sore', 
    'malam', 'kemarin', 'besok', 'lusa', 'tadi', 'nanti', 'sebentar', 'lalu', 'kemudian', 'akhirnya',
    'ada', 'tak', 'pun', 'lah', 'kah', 'tah', 'pula', 'saja', 'hanya', 'cuma', 'pun', 'dong', 'kok', 
    'sih', 'deh', 'yah', 'nih', 'tuh', 'wah', 'aduh', 'astaga', 'alamak', 'ayo', 'mari', 'silakan',
    'kata', 'ucap', 'tutur', 'sebut', 'bilang', 'ungkap', 'jelas', 'terang', 'nyata'
}

class NewsDatabase:
    def __init__(self):
        self.connection_params = {
            "host": os.getenv("PG_HOST"),
            "database": os.getenv("PG_DATABASE"), 
            "user": os.getenv("PG_USER"),
            "password": os.getenv("PG_PASSWORD"),
            "port": int(os.getenv("PG_PORT", 5432))
        }
        self.search = PostgresService()
    
    def get_connection(_self):
        try:
            conn = psycopg2.connect(**_self.connection_params)
            return conn
        except Exception as e:
            st.error(f"Database connection failed: {e}")
            return None
    
    def get_statistics(self):
        conn = self.get_connection()
        if not conn:
            return {}
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}
                
                # All stats in one query for better performance
                cursor.execute("""
                    WITH bronze_stats AS (
                        SELECT 
                            COUNT(*) as total,
                            COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed,
                            COUNT(CASE WHEN processed = FALSE THEN 1 END) as pending
                        FROM bronze
                    ),
                    silver_stats AS (
                        SELECT 
                            COUNT(*) as total,
                            ROUND(AVG(content_length)::numeric, 2) as avg_content_length,
                            COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed
                        FROM silver
                    ),
                    gold_stats AS (
                        SELECT COUNT(*) as total FROM view_gold
                    ),
                    topic_stats AS (
                        SELECT topic, COUNT(*) as count
                        FROM view_gold
                        WHERE topic IS NOT NULL
                        GROUP BY topic
                        ORDER BY count DESC
                        LIMIT 10
                    ),
                    hourly_stats AS (
                        SELECT 
                            publish_hour,
                            COUNT(*) as article_count
                        FROM view_gold
                        GROUP BY publish_hour
                        ORDER BY publish_hour
                    )
                    SELECT 
                        (SELECT row_to_json(bronze_stats.*) FROM bronze_stats) as bronze,
                        (SELECT row_to_json(silver_stats.*) FROM silver_stats) as silver,
                        (SELECT row_to_json(gold_stats.*) FROM gold_stats) as gold,
                        (SELECT json_agg(topic_stats.*) FROM topic_stats) as topics,
                        (SELECT json_agg(hourly_stats.*) FROM hourly_stats) as hourly
                """)
                
                result = cursor.fetchone()
                if result:
                    stats['bronze'] = result['bronze']
                    stats['silver'] = result['silver'] 
                    stats['gold'] = result['gold']
                    stats['topics'] = result['topics'] or []
                    stats['hourly'] = result['hourly'] or []
                
                return stats
                
        except Exception as e:
            st.error(f"Error getting statistics: {e}")
            return {}
        finally:
            conn.close()
    
    def search_news(self, query: str):
        return self.search.full_text_search(query)

def clean_text_for_wordcloud(text):
    if not text:
        return ""
    
    text = text.lower()
    text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
    text = re.sub(r'[^a-zA-Z\s]', ' ', text)
    
    words = text.split()
    filtered_words = [word for word in words if len(word) > 2 and word not in INDONESIAN_STOPWORDS]
    
    return ' '.join(filtered_words)

def create_wordcloud(text, max_words=50):
    if not text:
        return None
    
    cleaned_text = clean_text_for_wordcloud(text)
    if not cleaned_text:
        return None
    
    wordcloud = WordCloud(
        width=600, 
        height=300,
        background_color='white',
        max_words=max_words,
        colormap='viridis',
        relative_scaling=0.5
    ).generate(cleaned_text)
    
    return wordcloud

def main():
    st.set_page_config(
        page_title="CNN Indonesia News Dashboard",
        page_icon="📰",
        layout="wide"
    )
    
    # Custom CSS for compact layout
    st.markdown("""
    <style>
    .main > div {
        padding-top: 1rem;
    }
    .stMetric > div > div > div > div {
        font-size: 0.9rem;
    }
    .stExpander > div > div > div {
        padding: 0.5rem;
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.title("📰 CNN Indonesia News Dashboard")
    
    # Initialize database
    db = NewsDatabase()
    
    # Load statistics
    with st.spinner("Loading dashboard..."):
        stats = db.get_statistics()
    
    if not stats:
        st.error("Unable to load database statistics.")
        return
    
    # TOP SECTION: Key Metrics
    st.subheader("📊 Database Overview")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        bronze_total = stats.get('bronze', {}).get('total', 0)
        st.metric("🥉 Bronze", f"{bronze_total:,}")
    
    with col2:
        silver_total = stats.get('silver', {}).get('total', 0)
        st.metric("🥈 Silver", f"{silver_total:,}")
    
    with col3:
        gold_total = stats.get('gold', {}).get('total', 0)
        st.metric("🥇 Gold", f"{gold_total:,}")
    
    
    
    st.divider()
    
    # MIDDLE SECTION: Search & WordCloud
    st.subheader("Search News & Word Analysis")
    
    col1, col2 = st.columns([2, 3])
    
    with col1:
        search_query = st.text_input("Enter search terms:", placeholder="e.g. ekonomi, politik")
        # search_limit = st.selectbox("Results:", [5, 10, 20], index=1)
        
        search_results = []
        if search_query:
            with st.spinner("Searching..."):
                search_results = db.search_news(search_query)
            
            if search_results:
                st.success(f"Found {len(search_results)} articles")
                
                # Show top 3 results compactly
                for i, article in enumerate(search_results, 1):
                    with st.expander(f"{i}. {article['title'][:60]}..."):
                        st.write(f"**Topic:** {article['topic'] or 'N/A'}")
                        st.write(f"**Date:** {article['date']}")
                        content_preview = article['content'][:200] + "..." if len(article['content']) > 200 else article['content']
                        st.write(content_preview)
                        if article['link']:
                            st.markdown(f"[Read Full Article]({article['link']})")
                        st.write(f"**Relevance:** {article['rank']:.4f}")
            else:
                st.info("No articles found.")
    
    with col2:
        st.write("**Word Cloud from Search Results**")
        if search_results:
            all_content = ' '.join([article['content'] for article in search_results if article['content']])
            
            if all_content:
                wordcloud = create_wordcloud(all_content)
                if wordcloud:
                    fig, ax = plt.subplots(figsize=(8, 4))
                    ax.imshow(wordcloud, interpolation='bilinear')
                    ax.axis('off')
                    st.pyplot(fig)
                else:
                    st.info("Unable to generate word cloud")
            else:
                st.info("No content available for word cloud")
        else:
            st.info("Search for articles to see word cloud")
    
    st.divider()
    
    # BOTTOM SECTION: Analytics Charts
    st.subheader("Analytics & Insights")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.write("**Top Topics**")
        if stats.get('topics'):
            topics_df = pd.DataFrame(stats['topics'])
            fig = px.bar(
                topics_df.head(8),
                x='count',
                y='topic',
                orientation='h',
                height=300
            )
            fig.update_layout(
                margin=dict(l=0, r=0, t=20, b=0),
                yaxis={'categoryorder': 'total ascending'},
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No topic data")
    
    with col2:
        st.write("**Hourly Patterns**")
        if stats.get('hourly'):
            hourly_df = pd.DataFrame(stats['hourly'])
            fig = px.line(
                hourly_df,
                x='publish_hour',
                y='article_count',
                markers=True,
                height=300
            )
            fig.update_layout(
                margin=dict(l=0, r=0, t=20, b=0),
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No hourly data")
    
    with col3:
        st.write("**Processing Status**")
        bronze_stats = stats.get('bronze', {})
        
        # Processing pipeline status
        processed = bronze_stats.get('processed', 0)
        pending = bronze_stats.get('pending', 0)
        total = processed + pending
        
        if total > 0:
            fig = go.Figure(data=[
                go.Pie(
                    labels=['Processed', 'Pending'],
                    values=[processed, pending],
                    hole=0.4
                )
            ])
            fig.update_layout(
                height=300,
                margin=dict(l=0, r=0, t=20, b=0),
                showlegend=True
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No processing data")
    
    # FOOTER: Quick Stats
    st.divider()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        silver_processed = stats.get('silver', {}).get('processed', 0)
        st.metric("Silver Processed", f"{silver_processed:,}")
    
    with col2:
        data_quality = (gold_total / max(silver_total, 1)) * 100
        st.metric("Data Quality", f"{data_quality:.1f}%")
    
    with col3:
        st.metric("Last Updated", datetime.now().strftime("%H:%M:%S"))
    
    # Auto-refresh option in sidebar
    with st.sidebar:
        st.header("🔧 Controls")
        if st.button("Refresh Data"):
            st.rerun()
        
        auto_refresh = st.checkbox("Auto Refresh (5min)")
        if auto_refresh:
            st.rerun()

if __name__ == "__main__":
    main()