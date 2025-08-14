import pandas as pd
import time
import os
from selenium.webdriver.common.by import By
import undetected_chromedriver as uc
import pyarrow
#import fastparquet
import sys

def scrape_reviews_for_book(book_url, max_reviews=20):
    options = uc.ChromeOptions()
    options.add_argument('--headless=new')
    driver = uc.Chrome(options=options)

    driver.get(book_url + "/reviews")
    time.sleep(5)

    reviews = []
    review_elements = driver.find_elements(By.CSS_SELECTOR, 'div.review p span')
    for elem in review_elements:
        text = elem.text.strip()
        if text:
            reviews.append(text)
        if len(reviews) >= max_reviews:
            break

    driver.quit()
    return reviews

def scrape_top_100_reviews(input_path='/home/vaibhavi/spark-ml-venv/Book_Analysis/data/top_100_books_urls.parquet',
                           output_path='/home/vaibhavi/spark-ml-venv/Book_Analysis/data/book_reviews.parquet',
                           max_reviews_per_book=20):
    df_books = pd.read_parquet(input_path)
    all_reviews = []

    for idx, row in df_books.iterrows():
        title = row['title']
        url = row['url']
        print(f"Scraping: {title}")
        try:
            reviews = scrape_reviews_for_book(url, max_reviews=max_reviews_per_book)
            for review in reviews:
                all_reviews.append({'title': title, 'url': url, 'review': review})
        except Exception as e:
            print(f"Failed for {title}: {e}")

    df_reviews = pd.DataFrame(all_reviews)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_reviews.to_parquet(output_path, index=False)
    print(f"Saved scraped reviews to {output_path}")


print("Python Path:", sys.executable)
print("Pyarrow version:", pyarrow.__version__)
df = pd.read_parquet('/home/vaibhavi/spark-ml-venv/Book_Analysis/data/book_reviews.parquet')
