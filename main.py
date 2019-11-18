from dotenv import load_dotenv
from os import environ
from pymongo import MongoClient
from tweepy import StreamListener, OAuthHandler, API, Stream

load_dotenv()

mongo_host = environ['MONGO_HOST']
client = MongoClient(mongo_host)
db = client.NoSQLProject
consumer_key = environ['CONSUMER_KEY']
consumer_secret = environ['CONSUMER_SECRET']
access_token = environ['ACCESS_TOKEN']
access_token_secret = environ['ACCESS_TOKEN_SECRET']


class CustomStreamListener(StreamListener):
    def on_status(self, status):
        username = status.author.screen_name
        tweet = status.text
        retweets = status.retweet_count
        favorites = status.favorite_count
        followers = status.user.followers_count
        background_color = status.user.profile_background_color
        creation_date = status.user.created_at
        location = status.user.location
        verified = status.user.verified
        geo_enabled = status.user.geo_enabled
        if(hasattr(status, 'possibly_sensitive')):
            possibly_sensitive = status.possibly_sensitive
        else:
            possibly_sensitive = 'Null'

        tweet_object = {
            "Username": username,
            "Tweet": tweet,
            "Retweets": retweets,
            "Favorites": favorites,
            "Followers": followers,
            "Background-Color": background_color,
            "Date-Created": creation_date,
            "Location": location,
            "Verified-Status": verified,
            "Geo-Enabled": geo_enabled,
            "Possibly-Sensitive": possibly_sensitive
        }
        db.Tweets.insert(tweet_object)

    def on_error(self, status):
        print(f'Error No: {status}')


if __name__ == "__main__":
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = API(auth)
    keywordsStream = Stream(auth=api.auth, listener=CustomStreamListener())
    bayAreaStream = Stream(auth=api.auth, listener=CustomStreamListener())
    tokioStream = Stream(auth=api.auth, listener=CustomStreamListener())
    cdmxStream = Stream(auth=api.auth, listener=CustomStreamListener())

    keywordsStream.filter(track=['#Something', '#AnotherThing', '#MoreThings', '#Hi'])

    bayAreaStream.filter(locations=[-122.53, 37.48, -121.94, 37.89])
    tokioStream.filter(locations=[139.72, 35.66, 139.78, 35.71])
    cdmxStream.filter(locations=[-99.30, 19.21, -98.85, 19.54])

    keywordsStream.filter(track=['Keyword1', 'Keyword2', 'Keyword3', 'Keyword4'])
