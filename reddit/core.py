import json
import praw
import logging
import datetime

TIME_FILTER_DEFAULT = 'year'

class Reddit:

    def __init__(self, credentials_path, debug_level='ERROR'):
        """
        :param credentials_path: The path to a json file containing reddit credentials
        """
        self.credentials_path = credentials_path
        credentials = json.load(open(self.credentials_path))

        debug_level = getattr(logging, debug_level, 'ERROR')
        handler = logging.StreamHandler()
        handler.setLevel(debug_level)

        for logger_name in ("praw", "prawcore"):
            logger = logging.getLogger(logger_name)
            logger.setLevel(debug_level)
            logger.addHandler(handler)

        self.reddit = praw.Reddit(
            client_id=credentials['client_id'],
            client_secret=credentials['client_secret'],
            user_agent=credentials['user_agent'],
        )

    def _search(self, query, subreddit_name, sort, time_filter=TIME_FILTER_DEFAULT, limit=None):
        """
        Does a query on the exact search query

        :param query:
        :param subreddit_name:
        :param sort:
        :param time_filter:
        :param limit:
        :return:
        """
        return self.reddit.subreddit(subreddit_name).search(query=query,
                                                            sort=sort,
                                                            time_filter=time_filter,
                                                            limit=limit)

    def search(self, query, subreddits, time_filter=TIME_FILTER_DEFAULT, limit=None, search_sort=None):
        """
        A wrapper on a simple search _search that goes over the various
        search sorts  if the first search yields more than 250 results.
        Otherwise it just returns the results of the first search. The reason
        for this is because reddit sorts the results before returning
        the top 250. So if the number of results is less than 250 there is no
        need to search with other sorts as the same results will be returned.

        :param query: search query
        :param subreddits: list of subreddits
        :param time_filter:
        :param limit:
        :param search_sort: Either None or one of 'relevance', 'hot', 'top', 'new', 'comments'
        :return: yields praw's submission objects
        """
        search_query = '"{}"'.format(query.lower())

        def query_in_submission(q, s):
            """
            Check if lowered case query is in submission.title or submission.selftext. The reason this is necessary is
            because reddit search on lemmatized terms.

            :param q: the query we searched for
            :param s: a PRAW.submission object
            :return:
            """
            q = q.lower()
            in_title = q in s.title.lower()
            in_text = q in s.selftext.lower()
            return in_title or in_text, {'in_title': in_title, 'in_text': in_text}

        if search_sort is None:
            search_sorts = ['relevance', 'hot', 'top', 'new', 'comments']
        else:
            search_sorts = [search_sort]

        seen_ids = set()
        if isinstance(subreddits, str):
            subreddits = [subreddits]
        for subreddit_name in subreddits:
            sort = search_sorts[0]
            for submission in self._search(search_query, subreddit_name, sort, time_filter, limit):
                if submission.id not in seen_ids:
                    seen_ids.add(submission.id)
                    found, _ = query_in_submission(query, submission)
                    if found:
                        yield RedditSubmission(submission)
            if len(seen_ids) >= 250:
                for sort in search_sorts[1:]:
                    for submission in self._search(search_query, subreddit_name, sort, time_filter, limit):
                        if submission.id not in seen_ids:
                            seen_ids.add(submission.id)
                            found, _ = query_in_submission(query, submission)
                            if found:
                                yield RedditSubmission(submission)

    def search_for_multiple_terms(self, search_terms, subreddit_name, search_sort=None):
        """
        This is a wrapper around search where we can pass in multiple terms. These terms
        can be combined using "OR" in reddit search but this way of doing it allows us to bypass
        the limit set by reddit by searching for each term independentally.

        :param search_terms: A list of terms to search
        :param subreddit_name: A single subreddit or multiple subreddits joined by '+' symbol
        :param search_sort: Either None or one of 'relevance', 'hot', 'top', 'new', 'comments'
        :return:
        """
        s = set()

        for query in search_terms:
            if not query:
                continue
            for i, submission in enumerate(self.search(query,
                                                       subreddit_name,
                                                       search_sort=search_sort)):
                if submission.id in s:
                    continue
                s.add(submission.id)

                yield submission


class RedditSubmission:

    def __init__(self, submission):
        """
        A wrapper around praw.submission object for now. However, this wrapping ensures that we are not tied to the PRAW library
        :param submission:
        """
        self.redditUsername = submission.author
        self.subreddit = submission.subreddit
        self.created = str(datetime.date.fromtimestamp(submission.created_utc))
        self.title = submission.title
        self.text = submission.selftext
        self.url = submission.permalink
        self.external_url = submission.url
        self.upvotes = submission.score
        self.comments = submission.num_comments
        self.id = submission.id

        if submission.upvote_ratio == 0:
            # This is not strictly correct but we have no way of knowing downvotes when upvote_ratio
            # is zero so we say downvotes is zero
            self.downvotes = 0
        else:
            self.downvotes = int(submission.score / submission.upvote_ratio) - submission.score

    def __hash__(self):
        return hash(self.id)

    def __repr__(self):
        return '{author}|{subreddit}|{created}|{title}'.format(author=self.redditUsername,
                                                               subreddit=self.subreddit,
                                                               created=self.created,
                                                               title=self.title
                                                               )
