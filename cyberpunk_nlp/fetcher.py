"""
Code copied from https://gist.github.com/int8/6684f968b252314cc8b5b87296ea2367
"""

import os
import time
import urllib.parse

import requests
import json
from slugify import slugify
import logging

from nltk.tokenize import sent_tokenize
import pandas as pd

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
CYBERPUNK_GAME_ID = 1091500


class SteamReviewFetcher(object):
    REVIEWS_PER_PAGE = 100

    BASE_URL = 'https://store.steampowered.com/appreviews/{0}?json=1&' \
               '&language={1}&' \
               'filter=updated&review_type=all&purchase_type=all&cursor={2}' \
               '&num_per_page=' + str(REVIEWS_PER_PAGE)

    def __init__(self, game_id, output_dir, language='english', delay_s=1):
        self.language = language
        self.output_dir = output_dir
        self.delay_s = delay_s
        self.game_id = game_id

    def _get_reviews_response(self, cursor='*'):
        logger.info(
            f"hitting "
            f"{self.BASE_URL.format(self.game_id, self.language, cursor)}"
        )
        return requests.get(
            self.BASE_URL.format(self.game_id, self.language, cursor))

    def get_total_nr_of_reviews(self):
        response = self._get_reviews_response(cursor='*')
        if response.ok:
            v = response.json()
            return int(v['query_summary']['total_reviews'])
        raise RuntimeError(f"Got {response} instead of 200 OK")

    def collect_reviews(self):
        cursor = '*'
        n = self.get_total_nr_of_reviews()
        for i in range(n // self.REVIEWS_PER_PAGE):
            destination_file_path = self.get_filepath(cursor)

            if os.path.exists(destination_file_path):
                logger.info(
                    f"data for cursor={cursor} already exists - "
                    f"moving to the next cursor"
                )
                cursor = self.get_cursor_from_file(cursor)
                continue

            response = self._get_reviews_response(cursor)
            if response.ok:
                v = response.json()
                cursor = urllib.parse.quote(v['cursor'])
                if v['success'] == 1:
                    with open(destination_file_path, "w") as f:
                        json.dump(fp=f, obj=v)
                    logger.info(
                        f"data for cursor={cursor} successfully fetched"
                    )
                else:
                    logger.error(f"could not fetch data for cursor={cursor}")
            else:
                logger.error(f"could not fetch data for cursor={cursor}")

            time.sleep(self.delay_s)

    def extract_sentences_dataframe(self):
        sentences_and_metadata = []
        for filename in os.listdir(self.output_dir):
            filepath = os.path.join(self.output_dir, filename)
            with open(filepath, "r") as f:
                data = json.load(fp=f)
                for review in data['reviews']:
                    if review['received_for_free']:
                        continue
                    timestamp = review['timestamp_updated']
                    author_steam_id = review['author']['steamid']
                    author_num_games_owned = review['author']['num_games_owned']
                    author_num_reviews = review['author']['num_reviews']
                    author_playtime_forever = review['author'][
                        'playtime_forever'
                    ]
                    author_playtime_last_two_weeks = review['author'][
                        'playtime_last_two_weeks'
                    ]
                    recommendation_id = review['recommendationid']
                    voted_up = review['voted_up']
                    sentences = sent_tokenize(review['review'])
                    for sentence in sentences:
                        sentences_and_metadata.append(
                            {
                                'recommendation_id': recommendation_id,
                                'author_steam_id': author_steam_id,
                                'author_num_games_owned': author_num_games_owned,
                                'author_num_reviews': author_num_reviews,
                                'author_playtime_forever': author_playtime_forever,
                                'author_playtime_last_two_weeks': author_playtime_last_two_weeks,
                                'voted_up': voted_up,
                                'sentence': sentence,
                                'timestamp': timestamp
                            }
                        )
        return pd.DataFrame(
            sentences_and_metadata
        )

    def get_filepath(self, cursor):
        return os.path.join(
            self.output_dir,
            f"cursor_{slugify(cursor)}.json"
        )

    def get_cursor_from_file(self, cursor):

        find_cursor_in = self.get_filepath(cursor)
        with open(find_cursor_in, "r") as f:
            v = json.load(f)
            cursor = urllib.parse.quote(v['cursor'])
            return cursor