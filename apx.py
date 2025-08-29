import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime, timedelta
import json
from typing import List, Dict, Any
from dotenv import load_dotenv

# Import Elasticsearch service for search
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from services.es import ElasticsearchService

load_dotenv()

# Page configuration
st.set_page_config(
    page_title="CNN Indonesia News Analytics",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="expanded"
)

class NewsAnalyticsDashboard:
    """Main dashboard class for news analytics"""
    
    def __init__(self):
        self.pg_connection = None
        self.es_service = ElasticsearchService()
        self.setup_database_connection()
    
    def setup_database_connection(self):
        """Setup PostgreSQL connection"""
        try:
            self.pg_connection = psycopg2.connect(
                host=os.getenv("PG_HOST"),
                database=os.getenv("PG_DATABASE"),
                user=os.getenv("PG_USER"),
                password=os.getenv("PG_PASSWORD"),
                port=int(os.getenv("PG_PORT"))
            )

        except Exception as e:
            st.error(f"Database connection failed: {e}")
    
    def execute_query(self, query: str, params=None) -> List[Dict]:
        """Execute PostgreSQL query and return results"""
        if not self.pg_connection:
            return []
        
        try:
            with self.pg_connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            st.error(f"Query execution failed: {e}")
            return []
    
    def get_overview_metrics(self) -> Dict[str, Any]:
        """Get overview metrics for the dashboard"""
        query = """
        SELECT 
            (SELECT COUNT(*) FROM view_gold) as total_articles,
            (SELECT COUNT(*) FROM view_gold WHERE publish_date = CURRENT_DATE) as today_articles,
            (SELECT COUNT(*) FROM view_gold WHERE publish_date >= CURRENT_DATE - INTERVAL '7 days') as week_articles,
            (SELECT COUNT(DISTINCT topic_category) FROM view_gold) as unique_topics,
            (SELECT COUNT(*) FROM gold_entities) as total_entities,
            (SELECT COUNT(DISTINCT entity_text) FROM gold_entities WHERE entity_type = 'PER') as unique_people,
            (SELECT COUNT(DISTINCT entity_text) FROM gold_entities WHERE entity_type = 'ORG') as unique_orgs,
            (SELECT AVG(content_length) FROM view_gold) as avg_content_length
        """
        
        result = self.execute_query(query)
        return result[0] if result else {}
    
    def get_topic_distribution(self) -> pd.DataFrame:
        """Get article distribution by topic"""
        query = """
        SELECT 
            topic_category,
            COUNT(*) as article_count,
            AVG(content_length) as avg_length,
            COUNT(CASE WHEN publish_date >= CURRENT_DATE - INTERVAL '7 days' THEN 1 END) as recent_count
        FROM view_gold
        GROUP BY topic_category
        ORDER BY article_count DESC
        """
        
        results = self.execute_query(query)
        return pd.DataFrame(results)
    
    def get_time_trends(self, days: int = 30) -> pd.DataFrame:
        """Get article trends over time"""
        query = """
        SELECT 
            publish_date,
            topic_category,
            COUNT(*) as daily_count,
            AVG(content_length) as avg_length
        FROM view_gold
        WHERE publish_date >= CURRENT_DATE - INTERVAL %s
        GROUP BY publish_date, topic_category
        ORDER BY publish_date DESC
        """
        
        results = self.execute_query(query, (f"{days} days",))
        return pd.DataFrame(results)
    
    def get_entity_insights(self, entity_type: str = 'PER', limit: int = 20) -> pd.DataFrame:
        """Get entity insights"""
        query = """
        SELECT 
            entity_text,
            entity_type,
            COUNT(*) as mention_count,
            COUNT(DISTINCT article_id) as article_count,
            AVG(confidence_score) as avg_confidence,
            MAX(ge.processed_at) as latest_mention
        FROM gold_entities ge
        WHERE entity_type = %s
          AND confidence_score > 0.7
          AND processed_at >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY entity_text, entity_type
        HAVING COUNT(*) >= 2
        ORDER BY mention_count DESC, avg_confidence DESC
        LIMIT %s
        """
        
        results = self.execute_query(query, (entity_type, limit))
        return pd.DataFrame(results)
    
    def get_content_analytics(self) -> Dict[str, Any]:
        """Get content quality analytics"""
        query = """
        SELECT 
            content_category,
            COUNT(*) as count,
            AVG(word_count) as avg_words,
            AVG(sentence_count) as avg_sentences,
            COUNT(CASE WHEN has_good_title THEN 1 END) as good_titles,
            COUNT(CASE WHEN has_image THEN 1 END) as with_images
        FROM view_gold
        GROUP BY content_category
        ORDER BY 
            CASE content_category 
                WHEN 'Short' THEN 1 
                WHEN 'Medium' THEN 2 
                WHEN 'Long' THEN 3 
                WHEN 'Very Long' THEN 4 
            END
        """
        
        results = self.execute_query(query)
        return pd.DataFrame(results)

def render_overview_metrics(dashboard):
    """Render overview metrics cards"""
    metrics = dashboard.get_overview_metrics()
    
    if not metrics:
        st.error("Could not load overview metrics")
        return
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Articles",
            value=f"{metrics.get('total_articles', 0):,}",
            delta=f"+{metrics.get('today_articles', 0)} today"
        )
    
    with col2:
        st.metric(
            label="This Week",
            value=f"{metrics.get('week_articles', 0):,}",
            delta=f"{metrics.get('unique_topics', 0)} topics"
        )
    
    with col3:
        st.metric(
            label="Total Entities",
            value=f"{metrics.get('total_entities', 0):,}",
            delta=f"{metrics.get('unique_people', 0)} people"
        )
    
    with col4:
        avg_length = metrics.get('avg_content_length', 0)
        st.metric(
            label="Avg Content Length",
            value=f"{int(avg_length):,} chars" if avg_length else "0",
            delta=f"{metrics.get('unique_orgs', 0)} organizations"
        )

def render_topic_analysis(dashboard):
    """Render topic analysis section"""
    st.subheader("Topic Distribution Analysis")
    
    topic_df = dashboard.get_topic_distribution()
    
    if topic_df.empty:
        st.warning("No topic data available")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Topic distribution pie chart
        fig_pie = px.pie(
            topic_df, 
            values='article_count', 
            names='topic_category',
            title="Articles by Topic Category"
        )
        fig_pie.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        # Topic vs average content length
        fig_bar = px.bar(
            topic_df.sort_values('avg_length', ascending=False),
            x='topic_category',
            y='avg_length',
            title="Average Content Length by Topic",
            color='recent_count',
            color_continuous_scale='viridis'
        )
        fig_bar.update_layout(xaxis_title = " Topic Category",
                              yaxis_title= " Avarage length")
        # fig_bar.update_xaxis(title="Topic Category")
        # fig_bar.update_yaxis(title="Average Length (characters)")
        # fig_bar.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_bar, use_container_width=True)
    
    # Topic details table
    st.subheader("Topic Details")
    topic_df_display = topic_df.copy()
    topic_df_display['avg_length'] = topic_df_display['avg_length'].astype(int)
    topic_df_display = topic_df_display.rename(columns={
        'topic_category': 'Topic',
        'article_count': 'Total Articles',
        'avg_length': 'Avg Length',
        'recent_count': 'Recent (7 days)'
    })
    st.dataframe(topic_df_display, use_container_width=True)

def render_time_trends(dashboard):
    """Render time trends analysis"""
    st.subheader("Publication Trends")
    
    # Time range selector
    col1, col2 = st.columns([3, 1])
    with col2:
        days_back = st.selectbox(
            "Time Range",
            options=[7, 14, 30, 60, 90],
            index=2,
            format_func=lambda x: f"Last {x} days"
        )
    
    trends_df = dashboard.get_time_trends(days=days_back)
    
    if trends_df.empty:
        st.warning("No trend data available")
        return
    
    # Convert date for plotting
    trends_df['publish_date'] = pd.to_datetime(trends_df['publish_date'])
    
    # Daily article count trend
    daily_totals = trends_df.groupby('publish_date')['daily_count'].sum().reset_index()
    
    fig_trend = px.line(
        daily_totals,
        x='publish_date',
        y='daily_count',
        title=f"Daily Article Count (Last {days_back} days)",
        markers=True
    )
    fig_trend.update_layout(xaxis_title = "Date",
                            yaxis_title = "Number of Articles")
    # fig_trend.update_xaxis(title="Date")
    # fig_trend.update_yaxis(title="Number of Articles")
    st.plotly_chart(fig_trend, use_container_width=True)
    
    # Stacked area chart by topic
    fig_stacked = px.area(
        trends_df,
        x='publish_date',
        y='daily_count',
        color='topic_category',
        title="Daily Articles by Topic Category"
    )
    st.plotly_chart(fig_stacked, use_container_width=True)

def render_entity_insights(dashboard):
    """Render entity insights section"""
    st.subheader("👥 Entity Recognition Insights")
    
    # Entity type selector
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        entity_type = st.selectbox(
            "Entity Type",
            options=['PER', 'ORG', 'LAW', 'NOR'],
            format_func=lambda x: {
                'PER': '👤 People',
                'ORG': '🏢 Organizations', 
                'LAW': '⚖️ Laws',
                'NOR': '🏛️ Political Organizations'
            }.get(x, x)
        )
    
    with col2:
        limit = st.number_input("Top N", min_value=10, max_value=50, value=20)
    
    entities_df = dashboard.get_entity_insights(entity_type=entity_type, limit=limit)
    
    if entities_df.empty:
        st.warning(f"No {entity_type} entities found")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Top entities bar chart
        top_entities = entities_df.head(15)
        fig_entities = px.bar(
            top_entities,
            x='mention_count',
            y='entity_text',
            orientation='h',
            title=f"Most Mentioned {entity_type} Entities",
            color='avg_confidence',
            color_continuous_scale='viridis'
        )
        fig_entities.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig_entities, use_container_width=True)
    
    with col2:
        # Confidence vs mentions scatter
        fig_scatter = px.scatter(
            entities_df,
            x='mention_count',
            y='avg_confidence',
            size='article_count',
            hover_data=['entity_text'],
            title="Confidence vs Mention Frequency",
            color='article_count',
            color_continuous_scale='plasma'
        )
        fig_scatter.update_layout(xaxis_title = "Mention Count",
                                  yaxis_title= "Avarage Confidance")
        # fig_scatter.update_xaxis(title="Mention Count")
        # fig_scatter.update_yaxis(title="Average Confidence")
        st.plotly_chart(fig_scatter, use_container_width=True)
    
    # Entities table
    st.subheader(f"Top {entity_type} Entities")
    entities_display = entities_df.copy()
    entities_display['avg_confidence'] = entities_display['avg_confidence'].round(3)
    entities_display['latest_mention'] = pd.to_datetime(entities_display['latest_mention']).dt.strftime('%Y-%m-%d')
    entities_display = entities_display.rename(columns={
        'entity_text': 'Entity',
        'mention_count': 'Mentions',
        'article_count': 'Articles',
        'avg_confidence': 'Confidence',
        'latest_mention': 'Latest'
    })
    st.dataframe(entities_display, use_container_width=True)

def render_search_interface(dashboard):
    """Render search interface using Elasticsearch"""
    st.subheader("🔍 Article Search")
    
    col1, col2, col3 = st.columns([3, 1, 1])
    
    with col1:
        search_query = st.text_input(
            "Search articles",
            placeholder="Enter keywords to search..."
        )
    
    with col2:
        search_size = st.number_input("Results", min_value=5, max_value=50, value=20)
    
    with col3:
        topic_filter = st.selectbox(
            "Topic Filter",
            options=['All'] + ['Politik', 'Ekonomi', 'Olahraga', 'Teknologi', 'Kesehatan', 'Internasional'],
            index=0
        )
    
    if search_query:
        try:
            # Prepare search parameters
            topic_param = None if topic_filter == 'All' else topic_filter
            
            # Perform search
            with st.spinner("Searching articles..."):
                search_results = dashboard.es_service.search_articles(
                    query=search_query,
                    size=search_size,
                    topic_filter=topic_param
                )
            
            if search_results:
                st.success(f"Found {len(search_results)} results")
                
                # Display search results
                for i, article in enumerate(search_results):
                    with st.expander(f"{article.get('title', 'No title')[:100]}..."):
                        col1, col2 = st.columns([3, 1])
                        
                        with col1:
                            st.write(f"**Content Preview:**")
                            content = article.get('content', '')
                            st.write(content[:500] + "..." if len(content) > 500 else content)
                            
                            # Show highlights if available
                            if article.get('highlight'):
                                st.write("**Highlighted matches:**")
                                for field, highlights in article['highlight'].items():
                                    for highlight in highlights[:2]:  # Show max 2 highlights
                                        st.markdown(f"- {highlight}", unsafe_allow_html=True)
                        
                        with col2:
                            st.write(f"**Score:** {article.get('score', 0):.2f}")
                            st.write(f"**Topic:** {article.get('topic', 'N/A')}")
                            st.write(f"**Date:** {article.get('date', 'N/A')}")
                            if article.get('link'):
                                st.markdown(f"[Read Full Article]({article['link']})")
            
            else:
                st.warning("No articles found matching your search criteria")
                
        except Exception as e:
            st.error(f"Search failed: {e}")

def render_content_analytics(dashboard):
    """Render content quality analytics"""
    st.subheader("Content Quality Analytics")
    
    content_df = dashboard.get_content_analytics()
    
    if content_df.empty:
        st.warning("No content analytics data available")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Content category distribution
        fig_content = px.bar(
            content_df,
            x='content_category',
            y='count',
            title="Articles by Content Length Category",
            color='avg_words',
            color_continuous_scale='blues'
        )
        fig_content.update_layout(xaxis_title = "Content Category",
                                  yaxis_title ="Numbers of Article")
        # fig_content.update_xaxis(title="Content Category")
        # fig_content.update_yaxis(title="Number of Articles")
        st.plotly_chart(fig_content, use_container_width=True)
    
    with col2:
        # Quality metrics
        content_df['good_title_rate'] = (content_df['good_titles'] / content_df['count'] * 100).round(1)
        content_df['image_rate'] = (content_df['with_images'] / content_df['count'] * 100).round(1)
        
        fig_quality = px.scatter(
            content_df,
            x='good_title_rate',
            y='image_rate',
            size='count',
            hover_data=['content_category', 'avg_words'],
            title="Content Quality Metrics",
            color='avg_words',
            color_continuous_scale='viridis'
        )
        fig_quality.update_layout(xaxis_title = "Good Title Rate",
                                  yaxis_title = "Image Rate")
        # fig_quality.update_xaxis(title="Good Title Rate (%)")
        # fig_quality.update_yaxis(title="Image Rate (%)")
        st.plotly_chart(fig_quality, use_container_width=True)
    
    # Content analytics table
    display_df = content_df.copy()
    display_df['avg_words'] = display_df['avg_words'].astype(int)
    display_df['avg_sentences'] = display_df['avg_sentences'].astype(int)
    display_df = display_df.rename(columns={
        'content_category': 'Category',
        'count': 'Articles',
        'avg_words': 'Avg Words',
        'avg_sentences': 'Avg Sentences',
        'good_titles': 'Good Titles',
        'with_images': 'With Images'
    })
    st.dataframe(display_df, use_container_width=True)

def main():
    """Main dashboard application"""
    st.title("CNN Indonesia News Analytics Dashboard")
    # st.markdown("Real-time analytics and insights from CNN Indonesia news articles")
    
    # Initialize dashboard
    dashboard = NewsAnalyticsDashboard()
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.radio(
        "Select Page",
        ["Overview", "Topic Analysis", "Time Trends", "Entity Insights", "Search Articles", "Content Analytics"]
    )
    
    # Add refresh button
    if st.sidebar.button("Refresh Data"):
        st.cache_data.clear()
        st.experimental_rerun()
    
    # Render selected page
    if page == "Overview":
        render_overview_metrics(dashboard)
        
        # Show recent highlights
        st.subheader("Quick Insights")
        col1, col2 = st.columns(2)
        
        with col1:
            render_topic_analysis(dashboard)
        
        with col2:
            render_entity_insights(dashboard)
    
    elif page == "Topic Analysis":
        render_topic_analysis(dashboard)
    
    elif page == "Time Trends":
        render_time_trends(dashboard)
    
    elif page == "Entity Insights":
        render_entity_insights(dashboard)
    
    elif page == "Search Articles":
        render_search_interface(dashboard)
    
    elif page == "Content Analytics":
        render_content_analytics(dashboard)
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Last Updated:** " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    st.sidebar.markdown("**Data Source:** CNN Indonesia")

if __name__ == "__main__":
    main()