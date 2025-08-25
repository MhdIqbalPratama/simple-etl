import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Configure Streamlit page
st.set_page_config(
    page_title="Streaming ETL Pipeline Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

class StreamingETLDashboard:
    def __init__(self):
        self.pg_params = {
            "host": os.getenv("PG_HOST"),
            "database": os.getenv("PG_DATABASE"),
            "user": os.getenv("PG_USER"),
            "password": os.getenv("PG_PASSWORD"),
            "port": os.getenv("PG_PORT")
        }
    
    def get_connection(self):
        """Get PostgreSQL connection"""
        try:
            return psycopg2.connect(**self.pg_params)
        except Exception as e:
            st.error(f"Database connection failed: {e}")
            return None
    
    def get_pipeline_stats(self):
        """Get comprehensive pipeline statistics"""
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            # Bronze table stats
            bronze_query = """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed_records,
                    COUNT(CASE WHEN processed = FALSE THEN 1 END) as pending_records,
                    MAX(created_at) as latest_record
                FROM pg_bronze
            """
            
            # Silver table stats
            silver_query = """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed_records,
                    COUNT(CASE WHEN processed = FALSE THEN 1 END) as pending_records,
                    AVG(content_length) as avg_content_length,
                    MAX(created_at) as latest_record
                FROM pg_silver
            """
            
            # Gold view stats
            gold_query = """
                SELECT 
                    COUNT(*) as total_records,
                    AVG(content_length) as avg_content_length,
                    COUNT(CASE WHEN content_category = 'Short' THEN 1 END) as short_articles,
                    COUNT(CASE WHEN content_category = 'Medium' THEN 1 END) as medium_articles,
                    COUNT(CASE WHEN content_category = 'Long' THEN 1 END) as long_articles,
                    COUNT(CASE WHEN has_substantial_content = TRUE THEN 1 END) as quality_articles,
                    MAX(date) as latest_article_date
                FROM vw_gold
            """
            
            with conn.cursor() as cur:
                cur.execute(bronze_query)
                bronze_result = cur.fetchone()
                bronze_stats = {
                    'total': bronze_result[0],
                    'processed': bronze_result[1],
                    'pending': bronze_result[2],
                    'latest': bronze_result[3]
                }
                
                cur.execute(silver_query)
                silver_result = cur.fetchone()
                silver_stats = {
                    'total': silver_result[0],
                    'processed': silver_result[1],
                    'pending': silver_result[2],
                    'avg_content_length': round(silver_result[3] or 0, 2),
                    'latest': silver_result[4]
                }
                
                cur.execute(gold_query)
                gold_result = cur.fetchone()
                gold_stats = {
                    'total': gold_result[0],
                    'avg_content_length': round(gold_result[1] or 0, 2),
                    'short_articles': gold_result[2],
                    'medium_articles': gold_result[3],
                    'long_articles': gold_result[4],
                    'quality_articles': gold_result[5],
                    'latest_article': gold_result[6]
                }
            
            return {
                'bronze': bronze_stats,
                'silver': silver_stats,
                'gold': gold_stats
            }
            
        except Exception as e:
            st.error(f"Error fetching pipeline stats: {e}")
            return None
        finally:
            conn.close()
    
    def get_topic_distribution(self):
        """Get article distribution by topic from gold view"""
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            query = """
                SELECT 
                    topic, 
                    COUNT(*) as count,
                    AVG(content_length) as avg_length,
                    COUNT(CASE WHEN content_category = 'Long' THEN 1 END) as long_articles
                FROM vw_gold
                GROUP BY topic
                ORDER BY count DESC
                LIMIT 10
            """
            
            df = pd.read_sql_query(query, conn)
            return df
            
        except Exception as e:
            st.error(f"Error fetching topic distribution: {e}")
            return None
        finally:
            conn.close()
    
    def get_content_quality_analysis(self):
        """Analyze content quality from gold view"""
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            query = """
                SELECT 
                    content_category,
                    COUNT(*) as count,
                    AVG(content_length) as avg_length,
                    COUNT(CASE WHEN has_good_title = TRUE THEN 1 END) as good_titles,
                    COUNT(CASE WHEN has_substantial_content = TRUE THEN 1 END) as substantial_content
                FROM vw_gold
                GROUP BY content_category
                ORDER BY 
                    CASE content_category 
                        WHEN 'Short' THEN 1 
                        WHEN 'Medium' THEN 2 
                        WHEN 'Long' THEN 3 
                    END
            """
            
            df = pd.read_sql_query(query, conn)
            return df
            
        except Exception as e:
            st.error(f"Error fetching content quality analysis: {e}")
            return None
        finally:
            conn.close()
    
    def get_hourly_publishing_pattern(self):
        """Get hourly publishing patterns from gold view"""
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            query = """
                SELECT 
                    publish_hour,
                    COUNT(*) as article_count,
                    AVG(content_length) as avg_content_length
                FROM vw_gold
                WHERE date >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY publish_hour
                ORDER BY publish_hour
            """
            
            df = pd.read_sql_query(query, conn)
            return df
            
        except Exception as e:
            st.error(f"Error fetching hourly patterns: {e}")
            return None
        finally:
            conn.close()
    
    def get_daily_articles(self):
        """Get daily article counts from gold view"""
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            query = """
                SELECT 
                    publish_date as date,
                    COUNT(*) as count,
                    COUNT(CASE WHEN content_category = 'Long' THEN 1 END) as long_articles,
                    COUNT(CASE WHEN has_substantial_content = TRUE THEN 1 END) as quality_articles
                FROM vw_gold
                WHERE publish_date >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY publish_date
                ORDER BY date DESC
            """
            
            df = pd.read_sql_query(query, conn)
            return df
            
        except Exception as e:
            st.error(f"Error fetching daily articles: {e}")
            return None
        finally:
            conn.close()
    
    def get_processing_pipeline_status(self):
        """Get detailed processing pipeline status"""
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            query = """
                SELECT 
                    'Bronze' as stage,
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed,
                    ROUND(
                        COUNT(CASE WHEN processed = TRUE THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2
                    ) as processing_rate,
                    MAX(created_at) as latest_update
                FROM pg_bronze
                
                UNION ALL
                
                SELECT 
                    'Silver' as stage,
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed,
                    ROUND(
                        COUNT(CASE WHEN processed = TRUE THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2
                    ) as processing_rate,
                    MAX(created_at) as latest_update
                FROM pg_silver
                
                UNION ALL
                
                SELECT 
                    'Gold (View)' as stage,
                    COUNT(*) as total_records,
                    COUNT(*) as processed,
                    100.0 as processing_rate,
                    MAX(created_at) as latest_update
                FROM vw_gold
            """
            
            df = pd.read_sql_query(query, conn)
            return df
            
        except Exception as e:
            st.error(f"Error fetching processing pipeline status: {e}")
            return None
        finally:
            conn.close()
    
    def get_recent_articles_sample(self, limit=5):
        """Get sample of recent articles from gold view"""
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            query = """
                SELECT 
                    title,
                    topic,
                    content_category,
                    content_length,
                    publish_date,
                    has_good_title,
                    has_substantial_content
                FROM vw_gold
                ORDER BY created_at DESC
                LIMIT %s
            """
            
            df = pd.read_sql_query(query, conn, params=[limit])
            return df
            
        except Exception as e:
            st.error(f"Error fetching recent articles: {e}")
            return None
        finally:
            conn.close()

def main():
    st.title("📊 Streaming ETL Pipeline Dashboard")
    st.markdown("### Real-time Bronze → Silver → Gold Data Processing")
    st.markdown("---")
    
    dashboard = StreamingETLDashboard()
    
    # Sidebar
    st.sidebar.header("🔧 Dashboard Controls")
    auto_refresh = st.sidebar.checkbox("Auto Refresh (15s)", value=True)
    refresh_rate = st.sidebar.selectbox("Refresh Rate", [10, 15, 30, 60], index=1)
    
    if st.sidebar.button("🔄 Refresh Now"):
        st.rerun()
    
    # Auto refresh
    if auto_refresh:
        time.sleep(refresh_rate)
        st.rerun()
    
    # Get pipeline statistics
    pipeline_stats = dashboard.get_pipeline_stats()
        
    if pipeline_stats:
        # Main metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="🥉 Bronze Records", 
                value=f"{pipeline_stats['bronze']['total']:,}",
                delta=f"{pipeline_stats['bronze']['pending']} pending"
            )
        
        with col2:
            st.metric(
                label="🥈 Silver Records", 
                value=f"{pipeline_stats['silver']['total']:,}",
                delta=f"{pipeline_stats['silver']['pending']} pending"
            )
        
        with col3:
            st.metric(
                label="🥇 Gold Articles", 
                value=f"{pipeline_stats['gold']['total']:,}",
                delta=f"{pipeline_stats['gold']['quality_articles']} quality"
            )
        
        with col4:
            if pipeline_stats['gold']['total'] > 0:
                quality_rate = (pipeline_stats['gold']['quality_articles'] / 
                              pipeline_stats['gold']['total'] * 100)
                st.metric(
                    label="✨ Quality Rate", 
                    value=f"{quality_rate:.1f}%",
                    delta=f"Avg {pipeline_stats['gold']['avg_content_length']:.0f} chars"
                )
    
    st.markdown("---")
    
    # Content Quality Analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📝 Content Quality Distribution")
        quality_data = dashboard.get_content_quality_analysis()
        
        if quality_data is not None and not quality_data.empty:
            fig = px.bar(
                quality_data,
                x='content_category',
                y='count',
                title="Articles by Content Length Category",
                color='content_category',
                color_discrete_map={
                    'Short': '#ff9999',
                    'Medium': '#66b3ff', 
                    'Long': '#99ff99'
                }
            )
            fig.update_layout(
                xaxis_title="Content Category",
                yaxis_title="Number of Articles"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No quality data available")
    
    with col2:
        st.subheader("🏷️ Topic Distribution")
        topic_data = dashboard.get_topic_distribution()
        
        if topic_data is not None and not topic_data.empty:
            fig = px.bar(
                topic_data,
                x='topic',
                y='count',
                title="Articles by Topic",
                hover_data=['avg_length', 'long_articles']
            )
            fig.update_layout(
                xaxis_title="Topic",
                yaxis_title="Number of Articles"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No topic data available")
    
    # Temporal Analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Daily Article Processing")
        daily_data = dashboard.get_daily_articles()
        
        if daily_data is not None and not daily_data.empty:
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=daily_data['date'],
                y=daily_data['count'],
                mode='lines+markers',
                name='Total Articles',
                line=dict(color='blue', width=3)
            ))
            
            fig.add_trace(go.Scatter(
                x=daily_data['date'],
                y=daily_data['quality_articles'],
                mode='lines+markers',
                name='Quality Articles',
                line=dict(color='green', width=2)
            ))
            
            fig.update_layout(
                title="Daily Article Processing Trend",
                xaxis_title="Date",
                yaxis_title="Number of Articles",
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No daily data available")
    
    with col2:
        st.subheader("🕐 Hourly Publishing Pattern")
        hourly_data = dashboard.get_hourly_publishing_pattern()
        
        if hourly_data is not None and not hourly_data.empty:
            fig = px.bar(
                hourly_data,
                x='publish_hour',
                y='article_count',
                title="Articles Published by Hour (Last 7 Days)",
                hover_data=['avg_content_length']
            )
            fig.update_layout(
                xaxis_title="Hour of Day",
                yaxis_title="Number of Articles"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No hourly pattern data available")
    
    # Processing Pipeline Status
    st.subheader("⚙️ Pipeline Processing Status")
    
    processing_data = dashboard.get_processing_pipeline_status()
    
    if processing_data is not None and not processing_data.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Processing rates
            fig = px.bar(
                processing_data,
                x='stage',
                y='processing_rate',
                title="Processing Rate by Stage (%)",
                color='processing_rate',
                color_continuous_scale='RdYlGn',
                range_color=[0, 100]
            )
            fig.update_layout(
                xaxis_title="Pipeline Stage",
                yaxis_title="Processing Rate (%)",
                yaxis=dict(range=[0, 100])
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Record counts
            fig = px.bar(
                processing_data,
                x='stage',
                y=['total_records', 'processed'],
                title="Records Count by Stage",
                barmode='group'
            )
            fig.update_layout(
                xaxis_title="Pipeline Stage",
                yaxis_title="Number of Records"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # System Health and Recent Data
    st.markdown("---")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("🆕 Recent Articles Sample")
        recent_articles = dashboard.get_recent_articles_sample(10)
        
        if recent_articles is not None and not recent_articles.empty:
            # Format the dataframe for better display
            recent_articles['quality_score'] = recent_articles.apply(
                lambda x: '🟢 High' if x['has_substantial_content'] and x['has_good_title'] 
                         else '🟡 Medium' if x['has_substantial_content'] or x['has_good_title']
                         else '🔴 Low', axis=1
            )
            
            display_df = recent_articles[['title', 'topic', 'content_category', 
                                        'content_length', 'publish_date', 'quality_score']]
            st.dataframe(display_df, use_container_width=True)
        else:
            st.info("No recent articles available")
    
    with col2:
        st.subheader("🏥 System Health")
        
        # Database connection test
        conn = dashboard.get_connection()
        if conn:
            st.success("✅ PostgreSQL: Connected")
            conn.close()
        else:
            st.error("❌ PostgreSQL: Connection Failed")
        
        # # Check for recent data
        # if pipeline_stats:
        #     if pipeline_stats['bronze']['latest']:
        #         latest_time = pipeline_stats['bronze']['latest']
        #         time_diff = datetime.now() - latest_time.replace(tzinfo=None)
                
        #         if time_diff.total_seconds():