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
    page_title="ETL Pipeline Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

class ETLDashboard:
    def __init__(self):
        self.pg_params = {
            "host": os.getenv("PG_HOST", "localhost"),
            "database": os.getenv("PG_DATABASE", "cnn_news_db"),
            "user": os.getenv("PG_USER", "cnn_user"),
            "password": os.getenv("PG_PASSWORD", "cnn_password"),
            "port": os.getenv("PG_PORT", "5432")
        }
    
    def get_connection(self):
        """Get PostgreSQL connection"""
        try:
            return psycopg2.connect(**self.pg_params)
        except Exception as e:
            st.error(f"Database connection failed: {e}")
            return None
    
    def get_pipeline_stats(self):
        """Get pipeline statistics"""
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            # Bronze table stats
            bronze_query = """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed_records,
                    COUNT(CASE WHEN processed = FALSE THEN 1 END) as pending_records
                FROM pg_bronze
            """
            
            # Silver table stats
            silver_query = """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed_records,
                    COUNT(CASE WHEN processed = FALSE THEN 1 END) as pending_records
                FROM pg_silver
            """
            
            # Gold table stats
            gold_query = """
                SELECT 
                    COUNT(*) as total_records,
                    AVG(content_length) as avg_content_length
                FROM pg_gold
            """
            
            # Final table stats
            final_query = """
                SELECT COUNT(*) as total_articles
                FROM articles
            """
            
            with conn.cursor() as cur:
                cur.execute(bronze_query)
                bronze_stats = dict(zip(['total', 'processed', 'pending'], cur.fetchone()))
                
                cur.execute(silver_query)
                silver_stats = dict(zip(['total', 'processed', 'pending'], cur.fetchone()))
                
                cur.execute(gold_query)
                gold_result = cur.fetchone()
                gold_stats = {
                    'total': gold_result[0],
                    'avg_content_length': round(gold_result[1] or 0, 2)
                }
                
                cur.execute(final_query)
                final_stats = {'total': cur.fetchone()[0]}
            
            return {
                'bronze': bronze_stats,
                'silver': silver_stats,
                'gold': gold_stats,
                'final': final_stats
            }
            
        except Exception as e:
            st.error(f"Error fetching pipeline stats: {e}")
            return None
        finally:
            conn.close()
    
    def get_topic_distribution(self):
        """Get article distribution by topic"""
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            query = """
                SELECT topic, COUNT(*) as count
                FROM pg_gold
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
    
    def get_daily_articles(self):
        """Get daily article counts"""
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            query = """
                SELECT 
                    DATE(created_at) as date,
                    COUNT(*) as count
                FROM pg_gold
                WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY DATE(created_at)
                ORDER BY date DESC
            """
            
            df = pd.read_sql_query(query, conn)
            return df
            
        except Exception as e:
            st.error(f"Error fetching daily articles: {e}")
            return None
        finally:
            conn.close()
    
    def get_processing_times(self):
        """Get processing stage statistics"""
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
                        COUNT(CASE WHEN processed = TRUE THEN 1 END) * 100.0 / COUNT(*), 2
                    ) as processing_rate
                FROM pg_bronze
                
                UNION ALL
                
                SELECT 
                    'Silver' as stage,
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed,
                    ROUND(
                        COUNT(CASE WHEN processed = TRUE THEN 1 END) * 100.0 / COUNT(*), 2
                    ) as processing_rate
                FROM pg_silver
                
                UNION ALL
                
                SELECT 
                    'Gold' as stage,
                    COUNT(*) as total_records,
                    COUNT(*) as processed,
                    100.0 as processing_rate
                FROM pg_gold
            """
            
            df = pd.read_sql_query(query, conn)
            return df
            
        except Exception as e:
            st.error(f"Error fetching processing times: {e}")
            return None
        finally:
            conn.close()

def main():
    st.title("📊 ETL Pipeline Monitoring Dashboard")
    st.markdown("---")
    
    dashboard = ETLDashboard()
    
    # Sidebar
    st.sidebar.header("🔧 Controls")
    auto_refresh = st.sidebar.checkbox("Auto Refresh (30s)", value=False)
    
    if st.sidebar.button("🔄 Refresh Now"):
        st.rerun()
    
    # Auto refresh
    if auto_refresh:
        time.sleep(30)
        st.rerun()
    
    # Main dashboard
    col1, col2, col3= st.columns(3)
    
    # Get pipeline statistics
    pipeline_stats = dashboard.get_pipeline_stats()
        
    if pipeline_stats:
        with col1:
            st.metric(
                label="🥉 Bronze Records", 
                value=pipeline_stats['bronze']['total'],
                delta=f"{pipeline_stats['bronze']['pending']} pending"
            )
        
        with col2:
            st.metric(
                label="🥈 Silver Records", 
                value=pipeline_stats['silver']['total'],
                delta=f"{pipeline_stats['silver']['pending']} pending"
            )
        
        with col3:
            st.metric(
                label="🥇 Gold Records", 
                value=pipeline_stats['gold']['total'],
                delta=f"Avg {pipeline_stats['gold']['avg_content_length']} chars"
            )
        
        # with col4:
        #     st.metric(
        #         label="📚 Final Articles", 
        #         value=pipeline_stats['final']['total']
        #     )
    
    st.markdown("---")
    
    # Charts row
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Daily Article Processing")
        daily_data = dashboard.get_daily_articles()
        
        if daily_data is not None and not daily_data.empty:
            fig = px.line(
                daily_data, 
                x='date', 
                y='count',
                title="Articles Processed Per Day",
                markers=True
            )
            fig.update_layout(
                xaxis_title="Date",
                yaxis_title="Number of Articles",
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No daily data available")
    
    with col2:
        st.subheader("🏷️ Topic Distribution")
        topic_data = dashboard.get_topic_distribution()
        
        if topic_data is not None and not topic_data.empty:
            fig = px.bar(
                topic_data,
                x = 'topic', 
                y = 'count',
                title="Articles by Topic"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No topic data available")
    
    # Processing pipeline visualization
    st.subheader("⚙️ Pipeline Processing Status")
    
    processing_data = dashboard.get_processing_times()
    
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
                color_continuous_scale='Viridis'
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
    
    # Processing stages flow
    st.subheader("🔄 Data Flow Overview")
    
    if pipeline_stats:
        # Create a flow diagram
        stages = ['Bronze', 'Silver', 'Gold', 'Final']
        values = [
            pipeline_stats['bronze']['total'],
            pipeline_stats['silver']['total'],
            pipeline_stats['gold']['total']
            # pipeline_stats['final']['total']
        ]
        
        fig = go.Figure(data=go.Scatter(
            x=stages,
            y=values,
            mode='lines+markers+text',
            text=[f"{v:,}" for v in values],
            textposition="top center",
            line=dict(width=3, color='lightblue'),
            marker=dict(size=15, color='darkblue')
        ))
        
        fig.update_layout(
            title="Data Flow Through Pipeline Stages",
            xaxis_title="Pipeline Stage",
            yaxis_title="Number of Records",
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # System health indicators
    st.markdown("---")
    st.subheader("🏥 System Health")
    
    health_col1, health_col2, health_col3 = st.columns(3)
    
    with health_col1:
        # Database connection test
        conn = dashboard.get_connection()
        if conn:
            st.success("✅ PostgreSQL: Connected")
            conn.close()
        else:
            st.error("❌ PostgreSQL: Connection Failed")
    
    with health_col2:
        # Check for recent data
        if pipeline_stats and pipeline_stats['gold']['total'] > 0:
            st.success("✅ Data Pipeline: Active")
        else:
            st.warning("⚠️ Data Pipeline: No recent data")
    
    with health_col3:
        # Processing status
        if pipeline_stats:
            pending_total = (
                pipeline_stats['bronze']['pending'] + 
                pipeline_stats['silver']['pending']
            )
            if pending_total == 0:
                st.success("✅ Processing: Up to date")
            else:
                st.info(f"⏳ Processing: {pending_total} pending")
    
    # Raw data table
    with st.expander("📋 Raw Data Tables"):
        tab1, tab2, tab3 = st.tabs(["Topic Distribution", "Daily Counts", "Processing Status"])
        
        with tab1:
            if topic_data is not None:
                st.dataframe(topic_data, use_container_width=True)
        
        with tab2:
            if daily_data is not None:
                st.dataframe(daily_data, use_container_width=True)
        
        with tab3:
            if processing_data is not None:
                st.dataframe(processing_data, use_container_width=True)
    
    # Footer
    st.markdown("---")
    st.markdown(
        f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"**Dashboard Version:** 1.0"
    )

if __name__ == "__main__":
    main()