import tweepy
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import json
import sqlite3
import pandas as pd
import numpy as np


def write_json(dct, file_str):
    # file_str = 'auth_keys.json'
    with open(file_str, 'w') as outfile:
        json.dump(dct, outfile)



def read_keys_json(file_str):
    dct = {}
    # file_str = 'auth_keys.json'
    with open(file_str) as readfile:
        dct = json.load(readfile)
    return dct



def get_api():
    api_keys = read_keys_json('auth_keys.json')
    auth = tweepy.OAuthHandler(api_keys['api_key'], api_keys['secret_api_key'])
    auth.set_access_token(api_keys['access_token'], api_keys['access_token_secret'])
    return tweepy.API(auth)



def get_entry_str(tweet, s1, s2):
    try:
        return ' '.join([s[s2] for s in tweet.entities[s1]])
    except KeyError:
        return ' '


def flatten_tweet_entities(tweet):
    dct = {}
    dct['entities_hashtags'] = get_entry_str(tweet, 'hashtags', 'text')
    dct['entities_user_mentions_ids'] = get_entry_str(tweet, 'user_mentions','id_str')
    dct['entities_urls'] = get_entry_str(tweet, 'urls', 'url')
    dct['entities_media_urls'] = get_entry_str(tweet, 'media', 'url')
    return dct



def tweet_to_dct(tweet):
    if not hasattr(tweet, '_json') and (type(tweet) == type({}) ):
        print('ALREADY_FLAT, NO JSON')
        return tweet

    flat_tweet = {}
    fields = ['created_at', 'id', 'full_text', 'source', 'in_reply_to_status_id', 'in_reply_to_user_id',
              'in_reply_to_screen_name', 'geo', 'coordinates', 'place', 'is_quote_status', 'retweet_count',
              'favorite_count', 'lang', 'possibly_sensitive', 'quoted_status_id']

    for f in fields:
        try:
            flat_tweet[f] = tweet._json[f]
        except KeyError:
            flat_tweet[f] = ' '

    flat_tweet['user_id'] = tweet._json['user']['id']
    flat_tweet['user_screen_name'] = tweet._json['user']['screen_name']
    flat_tweet['user_name'] = tweet._json['user']['name']

    try:
        flat_tweet['retweeted_status'] = tweet.retweeted_status.id
    except AttributeError:
        flat_tweet['retweeted_status'] = ' '

    entities = flatten_tweet_entities(tweet)
    for k in entities.keys():
        flat_tweet[k] = entities[k]

    return flat_tweet





def save_df(df, file_str):
    try:
        df.to_csv(file_str, index_label='index')
    except KeyError:
        df['index'] = df['index.1']
        df.drop(columns=['index.1'], inplace=True)
        df.to_csv(file_str)



def csv_reader(file_str):
    df = pd.read_csv(file_str, dtype= {'id': np.int64}, index_col='index')
    if 'index.1' in df.keys():
        df['index'] = df['index.1']
        df.drop(columns='index.1', inplace=True)
    return df


def tweet_dataframe(lst):
    # get keys from a single processed flattened tweet
    dct = tweet_to_dct(lst[0])

    # now get list of entries for each key in dct to dct
    flat_tweets = [tweet_to_dct(t) for t in lst]

    for k in dct.keys():
        dct[k] = [t[k] for t in flat_tweets]

    return pd.DataFrame.from_dict(dct)



def get_list_str(lst):
    return ' '.join([str(x) for x in lst])


def save_to_csv(df, file_str):
    save_df(csv_reader(file_str).append(df, ignore_index=True).drop_duplicates(subset=['id'], keep='last'), file_str)




def user_to_dct(user):
    dct = {}
    atrs = ['id', 'name', 'screen_name', 'location', 'profile_location', 'description', 'url', 'protected',
            'followers_count', 'friends_count', 'listed_count', 'created_at', 'favourites_count', 'utc_offset',
            'time_zone', 'geo_enabled', 'verified', 'statuses_count', 'lang', 'contributors_enabled', 'is_translator',
            'is_translation_enabled', 'profile_background_color', 'profile_background_image_url', 'profile_background_tile',
            'profile_image_url', 'profile_banner_url', 'profile_link_color', 'profile_sidebar_border_color',
            'profile_sidebar_fill_color', 'profile_text_color', 'profile_use_background_image', 'has_extended_profile',
            'default_profile', 'default_profile_image', 'translator_type']
    for atr in atrs:
        try:
            dct[atr] = user[atr]
        except KeyError:
            dct[atr] = ''
    dct['known_friends'] = ''
    dct['known_followers'] = ''
    return dct



def user_dataframe(lst):
    dct = user_to_dct(lst[0])
    users = [user_to_dct(u) for u in lst]
    for k in dct.keys():
        dct[k] = [u[k] for u in users]
    return pd.DataFrame.from_dict(dct).drop_duplicates()



def get_text(status):
    end = [status.display_text_range[1]]
    if len(status.entities['urls']) > 0:
        end.append(status.entities['urls'][0]['indices'][0])
    if 'media' in status.entities.keys():
        end.append(status.entities['media'][0]['indices'][0])
    return status.full_text[:min(end)]



def get_cleaned_text(status):
    if hasattr(status, 'retweeted_status'):
        status = status.retweeted_status
    text = status.full_text
    remove = []
    for e in status.entities.keys():
        for a in status.entities[e]:
            try:
                # [remove.append(i) for i in range(a['indices'][0], a['indices'][1])]
                remove.extend(range(a['indices'][0], a['indices'][1]))
            except AttributeError:
                pass
    text = ''.join([text[i] if i not in remove else '' for i in range(len(text))])
    return text



def show_tweets(tweet_list, api):
    i = 0
    for t in tweet_list:
        print(f'[{i}] id={t.id}, created: {t.created_at}')
        i += 1
        if hasattr(t, 'retweeted_status'):
            print(f'RETWEETED FROM: {t.retweeted_status.user.name} (@{t.retweeted_status.user.screen_name})')
            t = t.retweeted_status

        print(get_text(t))

        if t.is_quote_status:
            print("\t>>>>>-----------------------------------<<<<<")
            if not hasattr(t, 'quoted_status'):
                try:
                    tmp = api.get_status(id=t.quoted_status_id, tweet_mode='extended')
                    print(f'\t QUOTING TWEET FROM {tmp.author.name} (@{tmp.author.screen_name})')
                    print('\t', get_text(tmp).replace('\n', '\n\t '))
                except tweepy.error.TweepError:
                    print('\tQUOTE TWEET NOT AVAILABLE')
            else:
                print(f'\t QUOTING TWEET FROM {t.quoted_status.author.name} (@{t.quoted_status.author.screen_name})')
                print('\t', get_text(t.quoted_status).replace('\n', '\n\t '))
            print("\t>>>>>-----------------------------------<<<<<")

        print(' ===============================================')



def get_replies(status, api):
    replies = []
    author = status.author.screen_name
    id = status.id
    for reply in tweepy.Cursor(api.search, q=f'to:{author}', since_id=id, tweet_mode='extended').items():
        try:
            if not hasattr(reply, 'in_reply_to_status_id'):
                continue
            elif reply.in_reply_to_status_id == id:
               replies.append(reply)
        except:
            print('failed')
            return []
    return replies



def show_tweet_text_only(tweet_list):
    for t in tweet_list:
        print(get_cleaned_text(t))
        print('-----------------------------------------')



def display_status(status):
    for i in status.__dict__.keys():
        print(f'{i}: {status.__dict__[i]}')



def sentiment(tweet):
    analyzer = SentimentIntensityAnalyzer()
    return analyzer.polarity_scores(get_cleaned_text(tweet))



def run_sentiment(tweet_list):
    for t in tweet_list:
        print(get_text(t))
        print(sentiment(t))
        print('^^^^^^^^^^^^^^^^^^^^^^^^')



def process_users(user_list):
    file_str = 'users.csv'
    users = user_dataframe(user_list)
    save_to_csv(users, file_str)



def process_tweets(tweet_list):
    file_str = 'tweets.csv'
    users = [t._json['user'] for t in tweet_list]
    process_users(users)
    tweets = tweet_dataframe(tweet_list)
    save_to_csv(tweets, file_str)



def get_timeline(user_handle, api, count=50, page=1):
    t = [x for x in api.user_timeline(screen_name=user_handle, count=count, tweet_mode='extended', page=page)]
    process_tweets(t)
    return t


# def createTweetDb():
#     conn = sqlite3.connect('test.db')
#     c = conn.cursor()
#     k = ['created_at', 'id', 'full_text', 'display_text_range', 'entities', 'source', 'in_reply_to_status_id', 'in_reply_to_user_id', 'in_reply_to_screen_name', 'user', 'geo', 'coordinates', 'place', 'is_quote_status', 'retweet_count', 'favorite_count', 'lang', 'retweeted_status', 'possibly_sensitive', 'quoted_status_id', 'quoted_status_permalink', 'quoted_status']
#     c.execute("""CREATE TABLE tweets (created_at DATETIME, id INT PRIMARY KEY,  full_text TEXT, entities BLOB, source  TINYTEXT, in_reply_to_status_id TINYTEXT, in_reply_to_user_id TINYTEXT, user TINYTEXT, geo TINYTEXT, coordinates TINYTEXT, place TEXT, is_quote_status BOOL, retweet_count INT, favorite_count INT, lang TINYTEXT, retweeted_status BOOL, possibly_sensitive BOOL, quoted_status_id TINYTEXT )""")
#
#
#
# def insert_into_db(tweet, curs):
#     db_file = 'test.db'
#     conn = sqlite3.connect(db_file)
#     c = conn.cursor()
#     fields = ['created_at', 'id', 'full_text', 'entities', 'source', 'in_reply_to_status_id', 'in_reply_to_user_id', 'in_reply_to_screen_name', 'user', 'geo', 'coordinates', 'place', 'is_quote_status', 'retweet_count', 'favorite_count', 'lang', 'retweeted_status', 'possibly_sensitive', 'quoted_status_id']
#     data_dct = {}
#
#     for key in fields:
#         try:
#             data_dct[key] = tweet._json[key]
#         except KeyError:
#             data_dct[key] = 'null'
#     curs.execute('INSERT INTO tweets VALUES (:created_at, :id,  :full_text, :entities, :source, :in_reply_to_status_id, :in_reply_to_user_id, :in_reply_to_screen_name, :user, :geo, :coordinates, :place, :is_quote_status, :retweet_count, :favorite_count, :lang, :retweeted_status, :possibly_sensitive, :quoted_status_id )', data_dct)




api = get_api()
# tweets = api.user_timeline(screen_name='@realdonaldtrump', count=50, tweet_mode='extended', page=1)
tweets = get_timeline('realdonaldtrump', api)
print('length: ', len(tweets))
print()
show_tweets(tweets, api)
tweet_df = tweet_dataframe(tweets)
user_df = user_dataframe([t._json['user'] for t in tweets])


save_to_csv(tweet_df,'tweets.csv')
save_to_csv(user_df, 'users.csv')

user_df = csv_reader('users.csv')
df = csv_reader('tweets.csv')
