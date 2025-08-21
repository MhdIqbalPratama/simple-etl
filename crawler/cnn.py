from bs4 import BeautifulSoup, Comment
import aiohttp
import asyncio

class CNNCrawler:
    def __init__(self):
        self.base_url = "https://www.cnnindonesia.com/indeks/2"
    
    async def listing_news(self, session, total_page=10):
        data = []
        for page in range(1, total_page+1):
            print(f"Processing page {page}")
            url = f"{self.base_url}?page={page}"
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        page_content = soup.find("div", {"class": "flex flex-col gap-5"})
                        if page_content:
                            articles = page_content.find_all("article", {"class": "flex-grow"})
                            for item in articles:
                                try:
                                    comment_time = item.find("span", {"class": "text-xs text-cnn_black_light3"})
                                    doc = {
                                        "title": item.find("h2").get_text(strip=True) if item.find("h2") else "No title",
                                        "link": item.find("a")["href"] if item.find("a") else None,
                                        "image": item.find("img")["src"] if item.find("img") else "No image",
                                        "date": comment_time.get_text(strip=True) if comment_time else None,
                                        "topic": item.find("span", {"class": "text-xs text-cnn_red"}).get_text(strip=True) if item.find("span", {"class": "text-xs text-cnn_red"}) else None,
                                        "content": None
                                    }
                                    data.append(doc)
                                except Exception as e:
                                    print(f"Error processing article: {e}")
                        else:
                            print(f"No articles found on page {page}")
                    else:
                        print(f"Failed to fetch page {page}, status: {response.status}")
            except Exception as e:
                print(f"An error occurred while fetching page {page}: {e}")

        print(f"Total data collected: {len(data)}")
        return data

    async def crawl_contents(self, session, article):
        try:
            async with session.get(article["link"]) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    content_tag = soup.find("div", {"class": "detail-text"})
                    article["content"] = content_tag.text.strip() if content_tag else None
                return article
        except Exception as e:
            print(f"An error occurred while crawling contents: {e}")
            return None

    async def crawl_all_contents(self, total_page=10):
        async with aiohttp.ClientSession() as session:
            articles = await self.listing_news(session, total_page)
            if not articles:
                return []
            semaphore = asyncio.Semaphore(10)  # Limit concurrent requests

            async def fetch_content(article):
                async with semaphore:
                    return await self.crawl_contents(session, article)
            
            print("Fetching contents for articles...")
            articles_with_contents = await asyncio.gather(*[fetch_content(article) for article in articles])

            valid_articles = [article for article in articles_with_contents if article and article["content"]]

            print(f"Total articles with contents: {len(valid_articles)}")

            return valid_articles

