import os
import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor
import streamlit as st
from datetime import datetime
from dotenv import load_dotenv
import plotly.express as px
from wordcloud import WordCloud
import matplotlib.pyplot as plt

# Load env
load_dotenv()

st.set_page_config(
    page_title="📰 News ETL Dashboard",
    page_icon="📊",
    layout="wide"
)

# --- CSS header ---
st.markdown("""
<style>
    .main-header {
        font-size: 2.2rem;
        text-align: center;
        margin-bottom: 2rem;
        padding: 1rem;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 10px;
    }
</style>
""", unsafe_allow_html=True)


class NewsETLDashboard:
    def __init__(self):
        self.conn_params = {
            "host": os.getenv("PG_HOST"),
            "database": os.getenv("PG_DATABASE"),
            "user": os.getenv("PG_USER"),
            "password": os.getenv("PG_PASSWORD"),
            "port": os.getenv("PG_PORT"),
        }

    def run_query(self, query, params=None, as_df=True):
        try:
            conn = psycopg2.connect(**self.conn_params)
            if as_df:
                return pd.read_sql_query(query, conn, params=params)
            else:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, params or [])
                    return cur.fetchall()
        except Exception as e:
            st.error(f"DB Error: {e}")
            return pd.DataFrame() if as_df else []
        finally:
            if "conn" in locals():
                conn.close()

    def get_pipeline_stats(self):
        query = """
            SELECT 'bronze_total' AS metric, COUNT(*)::int AS value FROM bronze
            UNION ALL
            SELECT 'bronze_processed', COUNT(*)::int FROM bronze WHERE processed = TRUE
            UNION ALL
            SELECT 'silver_total', COUNT(*)::int FROM silver
            UNION ALL
            SELECT 'silver_processed', COUNT(*)::int FROM silver WHERE processed = TRUE
            UNION ALL
            SELECT 'gold_total', COUNT(*)::int FROM view_gold
            UNION ALL
            SELECT 'final_total', COUNT(*)::int FROM articles;
        """
        rows = self.run_query(query, as_df=False)
        return {row["metric"]: row["value"] for row in rows}

    def get_articles_data(self, limit=2000):
        query = """
            SELECT 
                title, topic, date, content_category, content_length,
                publish_hour, publish_day_of_week, publish_date,
                has_good_title, has_substantial_content, search_text
            FROM view_gold
            ORDER BY date DESC
            LIMIT %s;
        """
        return self.run_query(query, params=[limit])

    def search_articles(self, search_term, limit=100):
        query = """
            SELECT 
                title, topic, date, content_category, content_length,
                SUBSTRING(search_text, 1, 500) as content_preview,
                search_text
            FROM view_gold
            WHERE search_text ILIKE %s
            ORDER BY date DESC 
            LIMIT %s;
        """
        return self.run_query(query, params=[f"%{search_term}%", limit])


# --- Main Page ---
def main():
    st.markdown('<div class="main-header">📰 News ETL Pipeline Dashboard</div>', unsafe_allow_html=True)

    dashboard = NewsETLDashboard()

    # Pipeline stats
    stats = dashboard.get_pipeline_stats()
    if stats:
        c1, c2 = st.columns(2)
        c1.metric("🥉 Bronze", stats.get("bronze_total", 0), f"{stats.get('bronze_processed', 0)} processed")
        c2.metric("🥈 Silver", stats.get("silver_total", 0), f"{stats.get('silver_processed', 0)} processed")
        # c3.metric("🥇 Gold", stats.get("gold_total", 0))
        # c4.metric("📚 Final", stats.get("final_total", 0))

    # Article analytics
    st.subheader("📊 Article Analytics")
    df = dashboard.get_articles_data()
    if not df.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(px.pie(df, names="topic", title="Articles by Topic"))
        with col2:
            st.plotly_chart(px.histogram(df, x="publish_hour", title="Publishing Patterns by Hour"))

        col3, col4 = st.columns(2)
        with col3:
            st.plotly_chart(px.histogram(df, x="publish_day_of_week", title="Publishing by Day of Week"))
        with col4:
            st.plotly_chart(px.box(df, x="topic", y="content_length", title="Content Length Distribution by Topic"))

    # Search section
    st.subheader("🔍 Search Articles + WordCloud")
    term = st.text_input("Search keyword:")
    if term:
        results = dashboard.search_articles(term)
        if not results.empty:
            st.write(results[["title", "topic", "date", "content_preview"]])

            # --- WordCloud dari hasil search ---
            text = " ".join(results["search_text"].dropna().tolist())
            if text.strip():
                wc = WordCloud(width=800, height=400, background_color="white").generate(text)
                fig, ax = plt.subplots(figsize=(10, 5))
                ax.imshow(wc, interpolation="bilinear")
                ax.axis("off")
                st.pyplot(fig)
        else:
            st.write(f"news not found with {term} keywords") 


if __name__ == "__main__":
    main()
