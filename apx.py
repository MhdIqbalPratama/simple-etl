import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import re
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
from services.staging_pg import PostgresService
from services.es import ElasticsearchService # Added ES import
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
        # Initialize Elasticsearch service
        self.es = ElasticsearchService()
    
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
    
    # NEW: Elasticsearch-based insights
    def get_es_statistics(self):
        """Get comprehensive Elasticsearch statistics"""
        try:
            return self.es.get_statistics()
        except Exception as e:
            st.error(f"Error getting ES statistics: {e}")
            return {}
    
    def get_es_health(self):
        """Get Elasticsearch health status"""
        try:
            return self.es.health_check()
        except Exception as e:
            return {"cluster_status": "error", "error": str(e)}
    
    def get_date_histogram(self, interval="day"):
        """Get article distribution over time"""
        try:
            return self.es.get_date_histogram(interval)
        except Exception as e:
            st.error(f"Error getting date histogram: {e}")
            return []
    
    def search_es_articles(self, query, size=10, topic_filter=None):
        """Search articles using Elasticsearch"""
        try:
            return self.es.search_articles(query, size, topic_filter)
        except Exception as e:
            st.error(f"Error searching ES articles: {e}")
            return []

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

def display_search_performance_insights(db):
    """New section: Search & Performance Insights"""
    st.subheader("🔍 Search & Performance Insights")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**🏥 System Health**")
        es_health = db.get_es_health()
        
        # Health status indicators
        status_color = {
            "green": "🟢",
            "yellow": "🟡", 
            "red": "🔴",
            "error": "💥"
        }
        
        status_icon = status_color.get(es_health.get("cluster_status", "error"), "❓")
        st.write(f"{status_icon} **Cluster Status:** {es_health.get('cluster_status', 'Unknown')}")
        st.write(f"📊 **ES Documents:** {es_health.get('document_count', 0):,}")
        st.write(f"📁 **Index Exists:** {'✅' if es_health.get('index_exists', False) else '❌'}")
        
        # Add ES vs DB comparison
        es_stats = db.get_es_statistics()
        pg_stats = db.get_statistics()
        
        es_count = es_stats.get('total_documents', 0)
        pg_gold_count = pg_stats.get('gold', {}).get('total', 0)
        
        if pg_gold_count > 0:
            sync_percentage = (es_count / pg_gold_count) * 100
            st.metric("📊 ES-DB Sync", f"{sync_percentage:.1f}%", 
                     f"{es_count - pg_gold_count:+,} docs")
    
    with col2:
        st.write("**📈 Content Distribution Trends**")
        date_histogram = db.get_date_histogram("day")
        
        if date_histogram:
            df_hist = pd.DataFrame(date_histogram)
            df_hist['date'] = pd.to_datetime(df_hist['date'])
            df_hist = df_hist.sort_values('date').tail(14)  # Last 2 weeks
            
            fig = px.area(
                df_hist, 
                x='date', 
                y='count',
                title="Article Volume (Last 14 Days)",
                height=250
            )
            fig.update_layout(
                margin=dict(l=0, r=0, t=30, b=0),
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No temporal data available")

def display_advanced_search_section(db):
    """Enhanced search section with ES capabilities"""
    st.subheader("🔍 Advanced Search & Analysis")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.write("**🎯 Search Configuration**")
        search_query = st.text_input("Search Query:", placeholder="ekonomi, politik, teknologi")
        
        # Advanced filters
        with st.expander("🔧 Advanced Filters"):
            # Topic filter
            es_stats = db.get_es_statistics()
            topics = [topic['topic'] for topic in es_stats.get('topics', [])]
            topic_filter = st.selectbox("Filter by Topic:", ["All"] + topics)
            topic_filter = None if topic_filter == "All" else topic_filter
            
            # Result limit
            search_limit = st.selectbox("Max Results:", [5, 10, 20, 50], index=1)
        
        search_results = []
        if search_query:
            with st.spinner("🔍 Searching Elasticsearch..."):
                search_results = db.search_es_articles(
                    search_query, 
                    size=search_limit, 
                    topic_filter=topic_filter
                )
            
            if search_results:
                st.success(f"✅ Found {len(search_results)} articles")
                
                # Search quality metrics
                avg_score = sum(r.get('score', 0) for r in search_results) / len(search_results)
                st.metric("📊 Avg Relevance", f"{avg_score:.2f}")
                
                # Display results compactly
                for i, article in enumerate(search_results[:5], 1):
                    score_bar = "🟢" if article.get('score', 0) > 10 else "🟡" if article.get('score', 0) > 5 else "🟠"
                    with st.expander(f"{score_bar} {i}. {article['title'][:50]}... ({article.get('score', 0):.1f})"):
                        st.write(f"**📂 Topic:** {article.get('topic', 'N/A')}")
                        st.write(f"**📅 Date:** {article.get('date', 'N/A')}")
                        
                        # Show highlights if available
                        if article.get('highlight'):
                            if 'title' in article['highlight']:
                                st.write("**🎯 Title Match:** " + " ... ".join(article['highlight']['title']))
                            if 'content' in article['highlight']:
                                st.write("**📝 Content Match:** " + " ... ".join(article['highlight']['content'][:2]))
                        
                        # Content preview
                        content_preview = article['content'][:200] + "..." if len(article['content']) > 200 else article['content']
                        st.write(f"**Content:** {content_preview}")
            else:
                st.warning("❌ No articles found matching your criteria")
    
    with col2:
        st.write("**☁️ Search Results Word Cloud**")
        if search_results:
            # Combine all content from search results
            all_content = ' '.join([
                f"{article['title']} {article['content']}" 
                for article in search_results 
                if article.get('content')
            ])
            
            if all_content:
                wordcloud = create_wordcloud(all_content, max_words=100)
                if wordcloud:
                    fig, ax = plt.subplots(figsize=(10, 5))
                    ax.imshow(wordcloud, interpolation='bilinear')
                    ax.axis('off')
                    st.pyplot(fig)
                    
                    # Word frequency insights
                    word_freq = wordcloud.words_
                    if word_freq:
                        st.write("**🔤 Top Keywords:**")
                        top_words = list(word_freq.items())[:10]
                        
                        # Create a simple bar chart of top words
                        words_df = pd.DataFrame(top_words, columns=['word', 'frequency'])
                        fig_words = px.bar(
                            words_df, 
                            x='frequency', 
                            y='word',
                            orientation='h',
                            height=300,
                            title="Top Keywords in Search Results"
                        )
                        fig_words.update_layout(
                            margin=dict(l=0, r=0, t=30, b=0),
                            yaxis={'categoryorder': 'total ascending'}
                        )
                        st.plotly_chart(fig_words, use_container_width=True)
                else:
                    st.info("Unable to generate word cloud")
            else:
                st.info("No content available for analysis")
        else:
            st.info("🔍 Search for articles to see word cloud and keyword analysis")

def display_enhanced_analytics(db, stats):
    """Enhanced analytics section"""
    st.subheader("📈 Enhanced Analytics & Insights")
    
    # Get ES statistics
    es_stats = db.get_es_statistics()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.write("**📚 Topic Distribution (ES)**")
        es_topics = es_stats.get('topics', [])
        if es_topics:
            topics_df = pd.DataFrame(es_topics[:10])  # Top 10
            
            # Create a donut chart for topics
            fig = px.pie(
                topics_df,
                values='count',
                names='topic',
                hole=0.4,
                height=350,
                title="Article Distribution by Topic"
            )
            fig.update_layout(
                margin=dict(l=20, r=20, t=40, b=20),
                showlegend=True,
                legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="left", x=1.01)
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Topic insights
            total_articles = sum(topic['count'] for topic in es_topics)
            if total_articles > 0:
                dominant_topic = es_topics[0]
                dominance_pct = (dominant_topic['count'] / total_articles) * 100
                st.metric(
                    f"🏆 Dominant Topic", 
                    dominant_topic['topic'],
                    f"{dominance_pct:.1f}% of content"
                )
        else:
            st.info("No topic data available from ES")
    
    with col2:
        st.write("**🕐 Publishing Patterns**")
        hourly_data = stats.get('hourly', [])
        if hourly_data:
            hourly_df = pd.DataFrame(hourly_data)
            
            # Enhanced hourly chart with peak indicators
            fig = go.Figure()
            
            # Add main line
            fig.add_trace(go.Scatter(
                x=hourly_df['publish_hour'],
                y=hourly_df['article_count'],
                mode='lines+markers',
                name='Articles',
                line=dict(color='#2E86AB', width=3),
                marker=dict(size=6)
            ))
            
            # Highlight peak hours
            max_count = hourly_df['article_count'].max()
            peak_threshold = max_count * 0.8
            peak_hours = hourly_df[hourly_df['article_count'] >= peak_threshold]
            
            if not peak_hours.empty:
                fig.add_trace(go.Scatter(
                    x=peak_hours['publish_hour'],
                    y=peak_hours['article_count'],
                    mode='markers',
                    name='Peak Hours',
                    marker=dict(color='red', size=10, symbol='star')
                ))
            
            fig.update_layout(
                title="Publishing Activity by Hour",
                xaxis_title="Hour of Day",
                yaxis_title="Article Count",
                height=350,
                margin=dict(l=0, r=0, t=40, b=0),
                showlegend=True
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Peak hour insights
            if not peak_hours.empty:
                peak_hour = peak_hours.iloc[0]['publish_hour']
                st.metric("⏰ Peak Hour", f"{peak_hour}:00", f"{peak_hours.iloc[0]['article_count']} articles")
        else:
            st.info("No hourly data available")
    
    with col3:
        st.write("**📊 Data Quality & Processing**")
        
        # Multi-layer processing visualization
        bronze_stats = stats.get('bronze', {})
        silver_stats = stats.get('silver', {})
        gold_stats = stats.get('gold', {})
        
        # Create funnel chart
        funnel_data = [
            dict(
                values=[bronze_stats.get('total', 0), silver_stats.get('total', 0), gold_stats.get('total', 0)],
                labels=['Bronze (Raw)', 'Silver (Processed)', 'Gold (Enriched)'],
                name="Data Pipeline"
            )
        ]
        
        fig = go.Figure(go.Funnel(
            y=['Bronze', 'Silver', 'Gold'],
            x=[bronze_stats.get('total', 0), silver_stats.get('total', 0), gold_stats.get('total', 0)],
            textinfo="value+percent initial",
            marker=dict(color=['#CD7F32', '#C0C0C0', '#FFD700']),
        ))
        
        fig.update_layout(
            title="Data Processing Pipeline",
            height=350,
            margin=dict(l=0, r=0, t=40, b=20)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Processing efficiency
        if bronze_stats.get('total', 0) > 0:
            efficiency = (gold_stats.get('total', 0) / bronze_stats.get('total', 0)) * 100
            st.metric("🎯 Pipeline Efficiency", f"{efficiency:.1f}%", "Bronze → Gold")

def main():
    st.set_page_config(
        page_title="CNN Indonesia News Analytics Dashboard",
        page_icon="📰",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Enhanced custom CSS
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
    .success-metric { color: #00C851; }
    .warning-metric { color: #FF8800; }
    .error-metric { color: #FF4444; }
    </style>
    """, unsafe_allow_html=True)
    
    st.title("📰 CNN Indonesia News Analytics Dashboard")
    st.markdown("*Advanced analytics powered by PostgreSQL and Elasticsearch*")
    
    # Initialize database
    db = NewsDatabase()
    
    # Sidebar controls
    with st.sidebar:
        st.header("🔧 Dashboard Controls")
        
        # Refresh controls
        if st.button("🔄 Refresh All Data", type="primary"):
            st.rerun()
        
        # Display options
        st.subheader("📊 Display Options")
        show_es_insights = st.checkbox("Show Elasticsearch Insights", value=True)
        show_advanced_search = st.checkbox("Show Advanced Search", value=True)
        
        # Auto-refresh
        auto_refresh = st.checkbox("Auto Refresh (5min)")
        if auto_refresh:
            st.rerun()
        
        st.divider()
        
        # System status
        st.subheader("🏥 System Status")
        es_health = db.get_es_health()
        pg_connected = db.get_connection() is not None
        
        st.write(f"🐘 PostgreSQL: {'🟢 Connected' if pg_connected else '🔴 Disconnected'}")
        es_status = es_health.get('cluster_status', 'error')
        status_emoji = {"green": "🟢", "yellow": "🟡", "red": "🔴", "error": "🔴"}.get(es_status, "❓")
        st.write(f"🔍 Elasticsearch: {status_emoji} {es_status.title()}")
    
    # Load statistics
    with st.spinner("📊 Loading comprehensive analytics..."):
        stats = db.get_statistics()
    
    if not stats:
        st.error("❌ Unable to load database statistics.")
        return
    
    # TOP SECTION: Enhanced Key Metrics
    st.subheader("📊 System Overview")
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        bronze_total = stats.get('bronze', {}).get('total', 0)
        st.metric("🥉 Bronze", f"{bronze_total:,}")
    
    with col2:
        silver_total = stats.get('silver', {}).get('total', 0)
        st.metric("🥈 Silver", f"{silver_total:,}")
    
    with col3:
        gold_total = stats.get('gold', {}).get('total', 0)
        st.metric("🥇 Gold", f"{gold_total:,}")
    
    with col4:
        es_stats = db.get_es_statistics()
        es_total = es_stats.get('total_documents', 0)
        st.metric("🔍 ES Index", f"{es_total:,}")
    
    with col5:
        avg_length = stats.get('silver', {}).get('avg_content_length', 0)
        st.metric("📝 Avg Length", f"{int(avg_length):,}")
    
    with col6:
        processing_rate = (stats.get('bronze', {}).get('processed', 0) / max(bronze_total, 1)) * 100
        st.metric("⚙️ Processed", f"{processing_rate:.1f}%")
    
    st.divider()
    
    # NEW: Search Performance Insights
    if show_es_insights:
        display_search_performance_insights(db)
        st.divider()
    
    # ENHANCED: Advanced Search Section
    if show_advanced_search:
        display_advanced_search_section(db)
        st.divider()
    
    # ENHANCED: Analytics Charts
    display_enhanced_analytics(db, stats)
    
    # FOOTER: Extended Quick Stats
    st.divider()
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        silver_processed = stats.get('silver', {}).get('processed', 0)
        st.metric("📋 Silver Processed", f"{silver_processed:,}")
    
    with col2:
        data_quality = (gold_total / max(silver_total, 1)) * 100
        st.metric("✨ Data Quality", f"{data_quality:.1f}%")
    
    with col3:
        # ES sync status
        sync_rate = (es_total / max(gold_total, 1)) * 100 if gold_total > 0 else 0
        st.metric("🔄 ES Sync", f"{sync_rate:.1f}%")
    
    with col4:
        st.metric("🕐 Last Updated", datetime.now().strftime("%H:%M:%S"))
    
    # Status footer
    st.markdown("---")
    st.markdown("*Dashboard integrates PostgreSQL data pipeline with Elasticsearch search capabilities for comprehensive news analytics*")

if __name__ == "__main__":
    main()