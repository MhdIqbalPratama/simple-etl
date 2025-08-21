import re
from datetime import datetime
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
    
    def clean_content(self, content):
        """Clean article content"""
        if not content:
            return ""
        
        # Remove advertisements
        content = re.sub(r'ADVERTISEMENT.*?SCROLL TO CONTINUE', '', content, flags=re.DOTALL)
        
        # Remove "Lihat Juga" sections
        content = re.sub(r'Lihat Juga\s*:.*?(?=\n|$)', '', content, flags=re.MULTILINE)
        
        # Remove video tags
        content = re.sub(r'\[Gambas:.*?\]', '', content)
        
        # Clean extra spaces and newlines
        content = re.sub(r'\n{3,}', '\n\n', content)
        content = re.sub(r'\s+', ' ', content)
        
        # Remove leading location pattern
        content = re.sub(r'^[A-Za-z\s,]+CNN Indonesia\s*--\s*', '', content)
        
        return content.strip()
    
    def parse_date(self, date_str):
        """Parse date - handles both string and datetime formats"""
        if not date_str:
            return datetime.now()
        
        # If it's already a datetime object, return as is
        if isinstance(date_str, datetime):
            return date_str
        
        # If it's a string in YYYY-MM-DD HH:MM:SS format
        if isinstance(date_str, str):
            try:
                # Try parsing ISO format first
                return datetime.fromisoformat(date_str.replace('T', ' ').replace('Z', ''))
            except:
                try:
                    # Try standard datetime format
                    return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                except:
                    # Try Indonesian format if still fails
                    if 'WIB' in date_str:
                        parts = date_str.split()
                        if len(parts) >= 5:
                            day = parts[1]
                            month = self.months.get(parts[2], '01')
                            year = parts[3]
                            time = parts[4]
                            
                            dt_string = f"{year}-{month}-{day.zfill(2)} {time}"
                            return datetime.strptime(dt_string, "%Y-%m-%d %H:%M")
                    pass
        
        return datetime.now()
    
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
            'topic': article.get('topic', 'General')
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
