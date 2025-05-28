import os
import json
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from tqdm import tqdm
import pandas as pd
from dotenv import load_dotenv
import time
load_dotenv()

# Загрузка ключей из переменных окружения
TAVILY_API_KEY = os.getenv('TAVILY_API_KEY')
OPENROUTER_API_KEY = os.getenv('OPENROUTER_API_KEY')
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
GOOGLE_CX = os.getenv('GOOGLE_CX')

# --- Конфиг ---
NEWS_OUTPUT_FILE = 'news_history.json'

# --- Загрузка контекста из keywords_persons.json ---
def load_context(filename: str = 'keywords_persons.json'):
    with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)
    keywords = [k['keyword'] for k in data.get('keywords', [])]
    persons = data.get('persons', [])
    organizations = data.get('organizations', [])
    return keywords, persons, organizations

# --- Генерация поисковых запросов на основе контекста ---
def generate_contextual_queries(period: str = 'day') -> List[str]:
    keywords, persons, organizations = load_context()
    queries = set()
    # Ключевые слова
    for kw in keywords:
        queries.add(f'{kw} новости')
        queries.add(f'{kw} news')
    # Персоны и их связи
    for p in persons:
        ru = p.get('name_ru')
        en = p.get('name_en')
        org_ids = p.get('organization_ids', [])
        if ru:
            queries.add(f'{ru} искусственный интеллект')
            queries.add(f'{ru} AI')
        if en:
            queries.add(f'{en} artificial intelligence')
            queries.add(f'{en} AI')
        # Связанные организации
        for org_id in org_ids:
            org = next((o for o in organizations if o['id'] == org_id), None)
            if org:
                org_name = org['name']
                if ru:
                    queries.add(f'{ru} {org_name} новости')
                    queries.add(f'{ru} {org_name} AI')
                if en:
                    queries.add(f'{en} {org_name} news')
                    queries.add(f'{en} {org_name} AI')
    # Организации
    for org in organizations:
        org_name = org['name']
        queries.add(f'{org_name} искусственный интеллект')
        queries.add(f'{org_name} AI')
        queries.add(f'{org_name} artificial intelligence')
        queries.add(f'{org_name} news')
    return list(queries)

# --- Tool: Google Custom Search ---
GOOGLE_BLOCKED_UNTIL = 0

def google_search(query: str, num: int = 10, max_retries: int = 2) -> List[Dict] | str:
    global GOOGLE_BLOCKED_UNTIL
    if time.time() < GOOGLE_BLOCKED_UNTIL:
        print("Google временно заблокирован, переключаюсь на Tavily.")
        return 'use_tavily'
    url = f"https://www.googleapis.com/customsearch/v1"
    params = {
        'key': GOOGLE_API_KEY,
        'cx': GOOGLE_CX,
        'q': query,
        'num': num,
        'dateRestrict': 'd1',  # только за последние сутки
    }
    retries = 0
    while retries < max_retries:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            items = response.json().get('items', [])
            results = []
            for item in items:
                results.append({
                    'query': query,
                    'url': item.get('link'),
                    'title': item.get('title'),
                    'snippet': item.get('snippet'),
                    'date': item.get('pagemap', {}).get('metatags', [{}])[0].get('article:published_time', ''),
                    'source': 'google',
                })
            return results
        elif response.status_code == 429:
            wait_time = 2 ** retries
            print(f"Google Search error 429: Too Many Requests. Повтор через {wait_time} сек...")
            time.sleep(wait_time)
            retries += 1
        else:
            print(f"Google Search error: {response.status_code}")
            return []
    print(f"Google Search error 429: превышено число попыток для запроса '{query}', Google заблокирован на 10 минут, переключаюсь на Tavily.")
    GOOGLE_BLOCKED_UNTIL = time.time() + 600  # 10 минут блокировки
    return 'use_tavily'

# --- Tool: Tavily Search ---
def tavily_search(query: str, num: int = 10) -> List[Dict]:
    url = "https://api.tavily.com/search"
    headers = {"Authorization": f"Bearer {TAVILY_API_KEY}"}
    params = {"query": query, "num": num}
    response = requests.post(url, headers=headers, json=params)
    if response.status_code == 200:
        data = response.json()
        results = []
        now = datetime.utcnow()
        for item in data.get('results', []):
            pub_date = item.get('date', '')
            # Фильтрация по дате публикации (если есть)
            if pub_date:
                try:
                    dt = datetime.fromisoformat(pub_date.replace('Z', '+00:00'))
                    if (now - dt).total_seconds() > 86400:
                        continue  # старше 24 часов
                except Exception:
                    pass  # если не удалось распарсить дату, не фильтруем
            results.append({
                'query': query,
                'url': item.get('url'),
                'title': item.get('title'),
                'snippet': item.get('description'),
                'date': pub_date,
                'source': 'tavily',
            })
        return results
    else:
        print(f"Tavily Search error: {response.status_code}")
        return []

# --- Сохранение новостей ---
def save_news(news: List[Dict], filename: str = NEWS_OUTPUT_FILE):
    """Сохраняет новости в JSON-файл (добавляет к истории)."""
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            history = json.load(f)
    else:
        history = []
    history.extend(news)
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

def save_news_csv(news: List[Dict], filename: str = 'news_history.csv'):
    """Сохраняет новости в CSV-файл."""
    df = pd.DataFrame(news)
    if os.path.exists(filename):
        try:
            df_old = pd.read_csv(filename)
            df = pd.concat([df_old, df], ignore_index=True)
        except pd.errors.EmptyDataError:
            pass  # файл пустой, просто перезаписываем
    df.to_csv(filename, index=False)

# --- Основной запуск ---
def main():
    queries = generate_contextual_queries(period='day')
    all_news = []
    for query in tqdm(queries, desc='Сбор новостей'):
        news_google = google_search(query, num=10)
        if news_google == 'use_tavily':
            news_tavily = tavily_search(query, num=10)
            all_news.extend(news_tavily)
        else:
            all_news.extend(news_google)
            news_tavily = tavily_search(query, num=10)
            all_news.extend(news_tavily)
    save_news(all_news)
    save_news_csv(all_news)
    print(f'Собрано новостей: {len(all_news)}')

if __name__ == '__main__':
    main() 