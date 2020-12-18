from collections import defaultdict, OrderedDict
import datetime
from urllib.parse import urlparse

import praw

from .core import Reddit, RedditSubmission


class Game:
    def __init__(self, gameId, name, url=None):
        """
        A class encapsulating a game
        :param gameId: A unique id for games
        :param name: The name of the game
        :param url: The URL of the game
        """
        self.gameId = gameId
        self.name = name
        url_parsed = urlparse(url)
        simple_url = '{}{}'.format(url_parsed.netloc, url_parsed.path)
        if simple_url[-1] == '/':
            simple_url = simple_url[:-1]
        self.simple_url = simple_url
        self.url = url

    def __repr__(self):
        return '{name} ({url})'.format(name=self.name, url=self.url)


class RedditAggregationData:
    def __init__(self, gameId, date, submissions):
        """
        A data structure to contain aggregated data for games

        :param gameId: Unique identifier of a game
        :param date: A datetime.date object capturing the date of submissions being aggregated
        :param gameData: A dictionary containing number of submissions, comments, upvote and downvotes for
        submissions for a game on a given date. Note this should alread be aggregated
        """
        types = ('submissions', 'comments', 'upvotes', 'downvotes')
        gameData = dict(zip(*(types, list(map(sum, zip(*[(1,
                                                          submission.comments,
                                                          submission.upvotes,
                                                          submission.downvotes) for submission in submissions]))))))

        self.gameId = gameId
        self.date = str(date)
        self.comments = gameData['comments']
        self.submissions = gameData['submissions']
        self.upvotes = gameData['upvotes']
        self.downvotes = gameData['downvotes']
        self.date_of_request = str(datetime.date.today())

    def __repr__(self):
        return '{id}/{date}: (submissions={submissions},comments={comments},upvotes={upvotes},downvotes={downvotes}'.format(
            id=self.gameId,
            comments=self.comments,
            submissions=self.submissions,
            upvotes=self.upvotes,
            downvotes=self.downvotes,
            date=self.date
        )


def FetchRedditDataForGames(subreddits, games, credentials_path):
    """
    The main function to get data for games
    :param subreddits: a list of string
    :param games: a list of Game objects
    :param credentials_path: a path to a json file containing reddit credentials
    :return:
    """
    subreddits = '+'.join(map(clean_subreddit_name, subreddits))
    reddit = Reddit(credentials_path)
    aggregate_data_by_date = defaultdict(list)
    for game in games:
        list_of_submissions_by_date = defaultdict(list)
        for submission in reddit.search_for_multiple_terms([game.name, game.simple_url], subreddits):
            list_of_submissions_by_date[submission.created].append(submission)

        for d, submissions in list_of_submissions_by_date.items():
            aggregate_data_by_date[str(d)].append(RedditAggregationData(game.gameId, d, submissions))

    od = OrderedDict()
    for k, v in sorted(aggregate_data_by_date.items(), key=lambda x: x[0]):
        od[k] = v
    return od


def clean_subreddit_name(x):
    """
    Remove 'r/' from beginning of a subreddit name
    :param x:
    :return:
    """
    x = x.strip()
    x = x[2:] if x.startswith('r/') else x
    return x


def FetchRedditSubmissionsInSubreddits(subreddits, collection_period, credentials_path, limit=None):
    """
    Fetches submissions from a list of subreddits

    :param subreddits: List of subreddit names
    :param collection_period: Can be one of 'D', 'W', 'M','Y'
    :param credentials_path: Path to a file containing reddit credentials
    :param limit: The limit of submissions per subreddit
    :return: A list of RedditSubmission objects
    """
    assert collection_period in ('D', 'W', 'M', 'Y'), 'Collection period has to be one of D,W,M or Y'

    days = {'D': 1, 'W': 7, 'M': 30, 'Y': 365}.get(collection_period)
    filter_date = str(datetime.date.today() - datetime.timedelta(days=days))

    reddit = Reddit(credentials_path)

    results = []
    seen = set()

    for subreddit_name in subreddits:
        subreddit_name = clean_subreddit_name(subreddit_name)
        subreddit = reddit.reddit.subreddit(subreddit_name)

        for submission in subreddit.new(limit=limit):
            if submission.id not in seen:
                seen.add(submission.id)
                submission = RedditSubmission(submission)
                if submission.created > filter_date:
                    results.append(submission)

    return results


def FetchRedditSubmissionsByUsers(users, collection_period, credentials_path, limit=None):
    """
    Fetches submissions from a list of users

    :param users: List of user names
    :param collection_period: Can be one of 'D', 'W', 'M', 'Y'
    :param credentials_path: Path to reddit credentials file
    :param limit: The limit of submissions per user
    :return: A list of RedditSubmission objects
    """
    assert collection_period in ('D', 'W', 'M', 'Y'), 'Collection period has to be one of D,W,M or Y'

    days = {'D': 1, 'W': 7, 'M': 30, 'Y': 365}.get(collection_period)
    filter_date = str(datetime.date.today() - datetime.timedelta(days=days))

    reddit = Reddit(credentials_path)

    results = []
    seen = set()

    for user in users:
        redditor = reddit.reddit.redditor(user)
        for submission in redditor.new(limit=limit):
            if isinstance(submission,praw.models.reddit.comment.Comment):
                continue
            if submission.id not in seen:
                seen.add(submission.id)
                submission = RedditSubmission(submission)
                if submission.created > filter_date:
                    results.append(submission)

    return results
