import re
import dateparser
import hashlib

class Cleaner:
    def __init__(self):
        self.months = {
            'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
            'Mei': '05', 'Jun': '06', 'Jul': '07', 'Agu': '08',
            'Sep': '09', 'Okt': '10', 'Nov': '11', 'Des': '12'
        }
    
    def clean_title(self, title):
        """Remove unwanted characters from title"""
        if not title:
            return ""
            
        # Remove newlines and extra spaces
        title = title.replace('\n', ' ').strip()
        title = re.sub(r'\s+', ' ', title)
        
        return title
    
    def clean_content(self, content: str) -> str:
        """Clean article content from CNN Indonesia crawler."""
        if not content:
            return ""
        
        # Remove advertisements and scroll prompts
        content = re.sub(r'ADVERTISEMENT.*?SCROLL TO CONTINUE WITH CONTENT', '', content, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove "Pilihan Redaksi" sections (up to next line or sentence)
        content = re.sub(r'Pilihan Redaksi.*?(?=[A-Z0-9])', '', content, flags=re.DOTALL)
        
        # Remove "Lihat Juga" sections
        content = re.sub(r'Lihat Juga\s*:.*?(?=\n|$)', '', content, flags=re.MULTILINE)
        
        # Remove photo / credit notes like (ANTARA FOTO/...), (CNN Indonesia/...), etc.
        content = re.sub(r'\([^)]*FOTO[^)]*\)', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\([^)]*CNN[^)]*\)', '', content, flags=re.IGNORECASE)
        
        # Remove video embed tags [Gambas:Video ...]
        content = re.sub(r'\[Gambas:.*?\]', '', content)
        
        # Normalize excessive newlines (convert 3+ to just 2)
        content = re.sub(r'\n{3,}', '\n\n', content)
        
        # Normalize spaces (collapse multiple spaces/tabs)
        content = re.sub(r'\s+', ' ', content)
        
        # Remove leading location pattern like "Jakarta, CNN Indonesia -- "
        content = re.sub(r'^[A-Za-z\s,]+CNN Indonesia\s*--\s*', '', content)
        
        return content.strip()
    
    def parse_date(self, date_str):
        """
        Parse string tanggal ke objek datetime menggunakan dateparser.
        Jika gagal, return None.
        """
        try:
            if not date_str or not isinstance(date_str, str):
                return None
            dt = dateparser.parse(date_str)
            return dt
        except Exception as e:
            print(f"Error parsing date: {e}")
            return None
        
    def generate_id(self, link):
        """Create unique ID from article link"""
        return hashlib.md5(link.encode()).hexdigest()
    
    def clean_article(self, article):
        """Clean single article data"""
        cleaned = {
            'id': self.generate_id(article['link']),
            'title': self.clean_title(article['title']),
            'content': self.clean_content(article['content']),
            'link': article['link'],
            'image': article.get('image', ''),
            'date': self.parse_date(article.get('date')),
            'topic': article.get('topic')
        }
        return cleaned
    
    def clean_all_articles(self, articles):
        """Clean list of articles"""
        cleaned_articles = []
        
        for article in articles:
            try:
                cleaned = self.clean_article(article)
                cleaned_articles.append(cleaned)
            except Exception as e:
                print(f"Error cleaning article {article.get('link', 'unknown')}: {e}")
                continue
        
        return cleaned_articles
