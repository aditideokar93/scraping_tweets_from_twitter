import os
import pandas as pd
import tweepy
from collections import Counter

# Twitter credentials for the app
consumer_key = ''
consumer_secret = ''
access_key = ''
access_secret = ''

# pass twitter credentials to tweepy
auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
# auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth,wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

# file location to store the extracted tweets
keyword_csv = r"C:\Users\Home\PycharmProjects\Forex_tweet_extraction\Hashtags_maximum_trial2.csv"
useraccount_csv= r"C:\Users\Home\PycharmProjects\Forex_tweet_extraction\Useraccounts_maximum_trial2.csv"

# columns of the csv file
COLS = ['id', 'created_at', 'source', 'original_text', 'lang',
        'favorite_count', 'retweet_count', 'original_author', 'possibly_sensitive', 'hashtags',
        'user_mentions', 'media_link','media_type','place']

# set two date variables for date range
start_date = '2019-10-01'
end_date = '2019-10-15'

count_key_list=[]
count_useraccount_list=[]
# method write_tweets()
def write_tweets(keywords_string, file):
    total_tweetCount=0
    tweetCount=0
    insertion_count = 0
    # If the file exists, then read the existing data from the CSV file.
    if os.stat(file).st_size == 2:
        df = pd.DataFrame(columns=COLS)
    else:
        df = pd.read_csv(file, header=0)

    try:
        print("keyword_String:",keywords_string)
        # page attribute in tweepy.cursor and iteration
        for page in tweepy.Cursor(api.search, q=keywords_string,
                                  count=100, include_rts=False, since=start_date,until=end_date,tweet_mode='extended').pages():
            for status in page:
                new_entry = []
                res_status = status._json

                ## check whether the tweet is in english or skip to the next tweet
                if res_status['lang'] != 'en':
                    continue

                # when run the code, below code replaces the retweets amount and
                # no of favourites that are changed since last download.
                if res_status['created_at'] in df['created_at'].values:
                    i = df.loc[df['created_at'] == res_status['created_at']].index[0]
                    if res_status['favorite_count'] != df.at[i, 'favorite_count'] or \
                            res_status['retweet_count'] != df.at[i, 'retweet_count']:
                        df.at[i, 'favorite_count'] = res_status['favorite_count']
                        df.at[i, 'retweet_count'] = res_status['retweet_count']
                    continue

                # append values to new entry
                new_entry += [res_status['id'], res_status['created_at'],
                              res_status['source'], res_status['full_text'],
                              res_status['lang'],
                              res_status['favorite_count'], res_status['retweet_count']]

                print(res_status['full_text'])
                # to append original author of the tweet
                new_entry.append(res_status['user']['screen_name'])

                try:
                    is_sensitive = res_status['possibly_sensitive']
                except KeyError:
                    is_sensitive = None
                new_entry.append(is_sensitive)

                # hashtagas and mentions are saved and separated using comma
                hashtags = ", ".join([hashtag_item['text'] for hashtag_item in res_status['entities']['hashtags']])
                new_entry.append(hashtags)
                hashtags_list = hashtags.split(',')
                for x in hashtags_list:
                    count_key_list.append(x)
                mentions = ", ".join([mention['screen_name'] for mention in res_status['entities']['user_mentions']])
                new_entry.append(mentions)

                # get link to the media attached to the tweet
                #Note: Media type will always indicate ‘photo’ even in cases of a video and GIF being attached to Tweet.
                try:
                    media_link = res_status['entities']['media'][0]['media_url_https']
                    media_type=res_status['entities']['media'][0]['type']
                except:
                    media_link = None
                    media_type=None
                new_entry.append(media_link)
                new_entry.append(media_type)

                # get location of the tweet if possible
                try:
                    location = res_status['user']['location']
                except TypeError:
                    location = None
                new_entry.append(location)

                # create a dataframe using the new entry list
                single_tweet_df = pd.DataFrame([new_entry], columns=COLS)
                df = df.append(single_tweet_df, ignore_index=True)
                total_tweetCount += len(single_tweet_df)
                tweetCount+=1
                print("Downloaded {0} tweets".format(total_tweetCount))
                #Write the tweets in CSV files in the batches of 1000 so that we do not lose the entire data if the extraction is interrupted
                if tweetCount>1000:
                    csvFile = open(file, 'a+', encoding='utf-8')
                    df.to_csv(csvFile, mode='a+', columns=COLS, index=False, encoding="utf-8")
                    insertion_count+=1
                    #Calculate the number of occurrence of each hashtag passed in query string
                    occurence_hashtags = dict(Counter(count_key_list))
                    with open("hashtag_occurrence_trial2.txt", 'a+',encoding='utf-8') as fp:
                        fp.write("insertion count:"+str(insertion_count)+'\n'+str(occurence_hashtags)+'\n\n')
                    print("hashtags count written")
                    # Calculate the number of occurrence of each user account passed in query string
                    occurence_useraccount = dict(Counter(df['original_author'].values))
                    with open("useraccount_occurrence_trial2.txt", 'a+',encoding='utf-8') as fp:
                        fp.write("insertion count:"+str(insertion_count)+'\n'+str(occurence_useraccount)+'\n\n')
                    print("useraccount count written")
                    print(tweetCount)
                    tweetCount=0
    except tweepy.TweepError as e:
        # Just exit if any error
        print("some error : " + str(e))

#the keywords are divided into 4 parts because a single query string can contain only 30 keywords
# 48 user accounts, 38 keywords
forex_keywords1 = '#forex OR #trading OR #forextrader OR #forextrading OR #forexsignals OR #trader OR #cryptocurrency OR #exchangerate OR #ea OR #currencyexchange OR #currency OR #forexfactory OR #foreignexchange OR #currencyconversion OR #currencyexchangerates OR #fxcm OR #iforex OR #metatrader OR #onlinetrading OR #currencycalculator OR #stocktrading OR #money OR #usd OR #stocks OR #usdcad OR #scalping OR #dollar OR #forexmarket'
forex_keywords2='#eur OR #gbp OR #aud OR #nzd OR #xau OR #cad OR #chf OR #us30 or #ger30'
user_accounts1='forexcrunch OR Schuldensuehner OR GoForexApp OR LizAnnSonders OR NorthmanTrader OR bySamRo OR ErikFossing OR KLCapital OR CiovaccoCapital OR JLyonsFundMGMT OR TDANSherrod OR FXstreetNews OR 50Pips OR waltervannelli OR FXflow OR FaithMight OR PipCzar OR JohnKicklighter OR RagheeHorner OR 4xguy OR JoelKruger OR alaidi OR piptrain OR ChrisLoriFX OR priceactionkim OR fatbeetrader OR ForexKong OR fxjoshwilson OR RenaTrader OR kathylienfx OR SaraEisen'
user_accounts2='Cigolo OR AdmiralMarkets OR aulafx OR BabyPips OR Brenda_Kelly  OR Caseystubbs OR CharmerCharts OR Chrislorifx OR CVecchioFX OR DavidJSong OR DRodriguezFX OR edjmoya OR EdMatts OR FerroTV OR Financemagnates OR flacqua OR DorganG'


write_tweets(forex_keywords1, keyword_csv)
write_tweets(forex_keywords2, keyword_csv)
write_tweets(user_accounts1, useraccount_csv)
write_tweets(user_accounts2, useraccount_csv)