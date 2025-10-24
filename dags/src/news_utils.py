import requests
from bs4 import BeautifulSoup
from collections import Counter
import pandas as pd
import re
import os

# Function to fetch news from a sample website or RSS feed
def fetch_news():
    """
    Fetch news text from a website or API.
    Returns the raw text.
    """
    url = "https://www.bbc.com/news"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    
    paragraphs = soup.find_all("p")
    text = " ".join([p.get_text() for p in paragraphs])
    
    return text

def clean_text(raw_text):
    """
    Clean the text by removing non-alphabet characters and lowercasing.
    """
    text = re.sub(r"[^a-zA-Z\s]", "", raw_text)
    text = text.lower()
    return text

def count_words(cleaned_text):
    """
    Count words and return a dictionary with word counts.
    """
    words = cleaned_text.split()
    word_counts = Counter(words)  
    return word_counts

def save_report(word_counts):
    """
    Save the top 10 words as a CSV report.
    """
    top_words = word_counts.most_common(10)
    
    df = pd.DataFrame(top_words, columns=["word", "count"])
    
    output_dir = "/opt/airflow/working_data"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "top_words.csv")
    df.to_csv(output_path, index=False)
    
    print(f"Report saved to {output_path}")
