import aylien_news_api
from aylien_news_api.rest import ApiException
import time
import json
from datetime import datetime, timedelta
import os
from pathlib import Path
import pandas as pd

AYLIEN_TIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
STORIES_FOLDER = 'stories'

# Get credentials from an external json file
with open('credentials.json') as f:
    credentials = json.load(f)
API_ID, API_KEY = credentials['API_ID'], credentials['API_KEY']


def api_connection():
    configuration = aylien_news_api.Configuration()
    configuration.api_key['X-AYLIEN-NewsAPI-Application-ID'] = API_ID
    configuration.api_key['X-AYLIEN-NewsAPI-Application-Key'] = API_KEY

    # Create an instance of the API class
    api_instance_ = aylien_news_api.DefaultApi(
        aylien_news_api.ApiClient(configuration))
    print("Aylien News API version " + aylien_news_api.__version__)
    print(f"API connection: {api_instance_}")
    return [api_instance_, ApiException]


def count_stories(name):
    folder = Path() / STORIES_FOLDER / name
    files = os.listdir(folder)
    return [len(readjsonl(folder / file)) for file in files]


def filter_stories_by_entities(name, patterns):
    folder = Path() / STORIES_FOLDER / name
    files = os.listdir(folder)
    patterns = set(p.lower() for p in patterns)
    all_stories = []
    for file in files:
        stories = readjsonl(folder / file)
        has_entity = [len(patterns.intersection(set(e['text'].lower() for e in
                                                    s['entities']['title'] +
                                                    s['entities']['body']))) > 0
                      for s in stories]
        all_stories.append([s for s, k in zip(stories, has_entity) if k > 0])
    return all_stories


def filter_stories_by_keywords(name, patterns):
    folder = Path() / STORIES_FOLDER / name
    files = os.listdir(folder)
    patterns = set(p.lower() for p in patterns)
    all_stories = []
    for file in files:
        stories = readjsonl(folder / file)
        has_keyword = [
            len(patterns.intersection((k.lower() for k in s['keywords']))) > 0
            for s in stories]
        all_stories.append([s for s, k in zip(stories, has_keyword) if k > 0])
    return all_stories


def get_stories(**params):
    """Fetch stories given some parameters."""
    all_stories = []
    new_stories = None

    while new_stories is None or (
            len(new_stories) > 0 and len(all_stories)):
        try:
            api_response = api_instance.list_stories(**params)
            new_stories = api_response.stories
            params['cursor'] = api_response.next_page_cursor

            all_stories += new_stories
            print("Fetched %d new stories. Total story count so far: %d" %
                  (len(new_stories), len(all_stories)))
        except ApiException as e:
            if e.status == 429:
                print('Usage limit exceeded. Waiting for 60 seconds...')
                time.sleep(60)
                continue
            else:
                print(e.status)

    return [s.to_dict() for s in all_stories]


def get_intervals(start_date, end_date):
    start_date = str2date(start_date)
    end_date = str2date(end_date)
    return [(to_date(start_date + timedelta(days=d)),
             to_date(start_date + timedelta(days=d + 1)))
            for d in range((end_date - start_date).days)]


def fetch_stories(name, intervals, override=False, **params):
    def f(x):
        # Format date for filename
        return x.split('T')[0].replace('-', '')

    make_folder(Path(STORIES_FOLDER) / name, override=override)

    filenames_list = []
    for start_date, end_date in intervals:
        filename = '_'.join([name, f(start_date)]) + '.jsonl'
        stories_ = get_stories(published_at_start=start_date,
                               published_at_end=end_date,
                               **params)
        if len(stories_) > 0:
            writejsonl(stories_, path=Path() / STORIES_FOLDER / name / filename)
            filenames_list.append(filename)
    return filenames_list


def make_folder(folder, override=False):
    """Make directory and necessary intermediate directories"""
    if not os.path.exists(folder) or override:
        Path(folder).mkdir(parents=True, exist_ok=override)


def readjsonl(path):
    with open(path) as f:
        text = f.read()
    return [json.loads(line) for line in text.split('\n')]


def str2date(string):
    return datetime.strptime(string, '%Y-%m-%d')


def to_date(date_):
    if not isinstance(date_, datetime):
        date_ = str2date(date_)
    return date_.strftime(AYLIEN_TIME_FORMAT)


def writejsonl(lines, path, verbose=False, **kwargs):
    def dump_date(obj):
        if isinstance(obj, datetime):
            return obj.strftime(AYLIEN_TIME_FORMAT)

    lines_ = [json.dumps(line, default=dump_date, **kwargs) for line in lines]
    with open(path, 'w') as f:
        f.write('\n'.join(lines_))
    if verbose:
        print('Number of lines:', len(lines_))
        print('Saved in:', os.path.abspath(path))
    return lines


if __name__ == '__main__':
    api_instance, api_exception = api_connection()
    run_name = 'newyorktimes'
    patterns_ = ['Wells Fargo', 'Wells Fargo & Company', 'Wells Fargo & Co',
                 'Wells Fargo Bank, N.A.']
    base_query = {'source_id': [202, 12579, 13215, 1833], 'per_page': 100}

    intervals = get_intervals('2020-06-01', '2020-08-01')
    filenames = fetch_stories(run_name, intervals, override=True,
                              **base_query)

    keywords = filter_stories_by_keywords(run_name, patterns_)
    entities = filter_stories_by_entities(run_name, patterns_)

    keywords_ids = [[s['id'] for s in stories] for stories in keywords]
    entities_ids = [[s['id'] for s in stories] for stories in entities]
    unique_ids = [list(set(k).union(e)) for k, e in
                  zip(keywords_ids, entities_ids)]

    days = [i[0].split('T')[0] for i in intervals]
    stories_counts = count_stories(run_name)
    keywords_counts = [len(stories) for stories in keywords]
    entities_counts = [len(stories) for stories in entities]
    unique_counts = [len(stories) for stories in unique_ids]

    df = pd.DataFrame(list(
        zip(days, stories_counts, keywords_counts, entities_counts,
            unique_counts)),
        columns=['date', 'volume of stories', 'volume using keyword',
                 'volume using entities',
                 'volume using keyword and entities'])

    df.to_csv(Path() / (run_name + '.csv'), index=False)
