import requests
import json
from pprint import pprint
import praw
from datetime import datetime
import pytz

CLIENT_ID = '' ## Reddit API client ID
SECRET_KEY = '' ## Reddit API secret key
NOTION_TOKEN = '' ## Notion API token
DATABASE_ID = '' ## Notion database 
REDDIT_USERNAME = '' ## Reddit username
REDDIT_PASSWORD = '' ## Reddit password
USER_AGENT = '' ## User agent


notion_headers = {
    'Authorization': f'Bearer {NOTION_TOKEN}',
    'Content-Type': 'application/json',
    'Notion-Version': '2022-06-28'
}

def time_extractor(utc_ts):

    utc_dt = datetime.fromtimestamp(utc_ts)
    utc_dt = pytz.utc.localize(utc_dt)

    ist = pytz.timezone('Asia/Kolkata')
    ist_dt = utc_dt.astimezone(ist)

    date_str = ist_dt.strftime('%Y-%m-%d')       
    day_str = ist_dt.strftime('%A')              
    time_str = ist_dt.strftime('%H:%M')

    return date_str, day_str, time_str

def get_reddit_posts():
    reddit = praw.Reddit(
        client_id=CLIENT_ID,
        client_secret=SECRET_KEY,
        user_agent= USER_AGENT,
        username= REDDIT_USERNAME,
        password= REDDIT_PASSWORD
    )
    
    user = reddit.redditor('')  ## Reddit username
    reddit_posts = []
    for submission in user.submissions.new(limit=None):
        date, day, time = time_extractor(submission.created_utc)
        reddit_posts.append({
            'title': submission.title,
            'subreddit': submission.subreddit.display_name,
            'score': submission.score,
            'comments': submission.num_comments,
            'url': f'reddit.com{submission.permalink}',
            'date': date,
            'time': time,
            'day': day
        })

    return reddit_posts

def get_existing_pages():
    existing = {}
    url = f'https://api.notion.com/v1/databases/{DATABASE_ID}/query'
    payload = {}
    has_more = True
    next_cursor = None

    while has_more:
        if next_cursor:
            payload['start_cursor'] = next_cursor

        response = requests.post(url, headers=notion_headers, json=payload)
        if response.status_code != 200:
            print(f"Failed to query database: {response.status_code}")
            break

        data = response.json()
        for page in data['results']:
            properties = page.get('properties', {})
            url_property = properties.get('URL', {})
            url_value = url_property.get('url')
            if url_value:
                existing[url_value] = page['id']

        has_more = data.get('has_more', False)
        next_cursor = data.get('next_cursor')
    
    return existing


def upsert_reddit_posts(reddit_posts):
    existing_pages = get_existing_pages()
    for post in reddit_posts:
        url = post['url']
        name = post['title']
        page_data = {
            'properties': {
                'Name': {
                    'title': [{'text': {'content': post['title']}}]
                },
                'Subreddit': {
                    'rich_text': [{'text': {'content': post['subreddit']}}]
                },
                'URL': {
                    'url': post['url']
                },
                'Score': {
                    'number': post['score']
                },
                'Comments': {
                    'number': post['comments']
                },
                'Date': {
                    'date': {
                        'start': post['date']  
                            }
                        },
                'Day': {
                    'rich_text': [
                        {
                            'text': {
                                'content': post['day']
                            }
                        }
                    ]
                },
                'Time': {
                    'rich_text': [
                        {
                            'text': {
                                'content': post['time']
                            }
                        }
                    ]
                }
                }
                }
        
        if url in existing_pages:
            page_id = existing_pages[url]
            update_url = f'https://api.notion.com/v1/pages/{page_id}'
            requests.patch(update_url, headers=notion_headers, json=page_data)
            print(f"Updated page: {name}")

        else:
            page_data['parent'] = {'database_id': DATABASE_ID}
            create_url = 'https://api.notion.com/v1/pages'
            requests.post(create_url, headers=notion_headers, json=page_data)
            print(f"Created new page: {name}")


if __name__ == "__main__":
    reddit_posts = get_reddit_posts()
    upsert_reddit_posts(reddit_posts)
    print("All posts have been processed.")
