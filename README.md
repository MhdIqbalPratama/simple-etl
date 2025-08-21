# ETL Simple - CNN Indonesia News Pipeline

Proyek ini adalah pipeline ETL (Extract, Transform, Load) sederhana untuk mengambil data berita dari situs CNN Indonesia, membersihkan data, dan menyimpannya ke Elasticsearch dan PostgreSQL.

## Fitur Utama

- **Crawling Berita:**  
  Mengambil data berita secara otomatis dan paginasi dari CNN Indonesia.

- **Data Cleaning:**  
  Membersihkan dan menstrukturkan data hasil crawling agar siap disimpan.

- **Penyimpanan Data:**  
  - **Elasticsearch:** Untuk pencarian dan analisis data berita.
  - **PostgreSQL:** Untuk penyimpanan terstruktur dan statistik.

- **Statistik Database:**  
  Menyediakan statistik jumlah artikel, distribusi topik, dan artikel terbaru.

## Struktur Folder

```
etl-simple/
├── crawler/           # Modul crawling berita
├── pipeline/          # Pipeline ETL utama
├── processor/         # Data cleaning
├── services/          # Integrasi Elasticsearch & PostgreSQL
├── main.py            # Entry point aplikasi
├── .env               # Konfigurasi environment
```

## Cara Menjalankan

1. **Clone repository dan install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Siapkan file `.env` dengan konfigurasi berikut:**
   ```
   ES_CLOUD_URL=your_elasticsearch_cloud_id
   ES_API_KEY=your_elasticsearch_api_key
   PG_HOST=localhost
   PG_DATABASE=your_db
   PG_USER=your_user
   PG_PASSWORD=your_password
   PG_PORT=5432
   ```

3. **Jalankan pipeline ETL:**
   ```bash
   python main.py
   ```

## Konfigurasi Elasticsearch

- Index: `news_index`
- Mapping: id, title, content, link, image, date, topic, search_text, content_length

## Konfigurasi PostgreSQL

- Tabel: `articles`, `categories`
- Index: topic, date

## Catatan

- Pastikan koneksi ke Elasticsearch dan PostgreSQL sudah benar.
- Pipeline mendukung crawling multi-halaman (pagination).
- Statistik dapat diakses setelah proses ETL selesai.

