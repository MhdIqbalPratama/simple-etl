import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from services.staging_pg import PostgresService  # pake service PG yang udah ada

# Init service
pg_service = PostgresService()

st.set_page_config(page_title="News Search Dashboard", layout="wide")

st.title("📰 News Search & WordCloud Dashboard")

# Input keyword
query = st.text_input("Masukkan kata kunci untuk mencari berita:", "")

if query:
    # Ambil hasil search dari PG
    results = pg_service.full_text_search(query)

    if results:
        df = pd.DataFrame(results)

        st.subheader("🔎 Hasil Pencarian")
        st.dataframe(df[["id", "title", "topic", "content"]])

        # Gabungkan semua content jadi satu string untuk wordcloud
        all_text = " ".join(df["content"])

        if all_text.strip():
            st.subheader("☁️ WordCloud dari Hasil Pencarian")

            wordcloud = WordCloud(width=800, height=400, background_color="white").generate(all_text)

            fig, ax = plt.subplots(figsize=(10, 5))
            ax.imshow(wordcloud, interpolation="bilinear")
            ax.axis("off")
            st.pyplot(fig)
        else:
            st.info("Tidak ada konten untuk membuat wordcloud.")
    else:
        st.warning("Tidak ada hasil untuk kata kunci ini.")
else:
    st.info("Masukkan kata kunci untuk memulai pencarian.")
