#!/usr/bin/env python3
import os
import json
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from google_play_scraper import app
import requests


REQUIRED_CONFIG_KEYS = ['api_host', 'country_code', 'categories_to_scrape', 'collections_to_scrape']
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)

def load_schema(name):
    return utils.load_json(get_abs_path("schemas/{}.json".format(name)))

def tap_data(app_id_list):
    keys_to_drop = ['messages', 'screenshots', 'comments', 'histogram']
    for app_id in app_id_list:
        app_result = app(app_id,lang='en', country='uk')
        for k in keys_to_drop:
            if k in app_result: del app_result[k]
        yield app_result

def scroll(host, collection, category, country_code):
    base_url = "http://{}/api/apps/?collection={}&category={}&country={}"
    r = dict(requests.get(base_url.format(host, collection, category, country_code)).json())
    while 'next' in r:
        yield [a['appId'] for a in r['results']]
        r = requests.get(r['next'])
    return


def app_list(config):
    category_l = ['APPLICATION','ANDROID_WEAR','ART_AND_DESIGN','AUTO_AND_VEHICLES','BEAUTY','BOOKS_AND_REFERENCE','BUSINESS','COMICS','COMMUNICATION','DATING','EDUCATION','ENTERTAINMENT','EVENTS','FINANCE','FOOD_AND_DRINK','HEALTH_AND_FITNESS','HOUSE_AND_HOME','LIBRARIES_AND_DEMO','LIFESTYLE','MAPS_AND_NAVIGATION','MEDICAL','MUSIC_AND_AUDIO','NEWS_AND_MAGAZINES','PARENTING','PERSONALIZATION','PHOTOGRAPHY','PRODUCTIVITY','SHOPPING','SOCIAL','SPORTS','TOOLS','TRAVEL_AND_LOCAL','VIDEO_PLAYERS','WEATHER','GAME','GAME_ACTION','GAME_ADVENTURE','GAME_ARCADE','GAME_BOARD','GAME_CARD','GAME_CASINO','GAME_CASUAL','GAME_EDUCATIONAL','GAME_MUSIC','GAME_PUZZLE','GAME_RACING','GAME_ROLE_PLAYING','GAME_SIMULATION','GAME_SPORTS','GAME_STRATEGY','GAME_TRIVIA','GAME_WORD','FAMILY','FAMILY_ACTION','FAMILY_BRAINGAMES','FAMILY_CREATE','FAMILY_EDUCATION','FAMILY_MUSICVIDEO','FAMILY_PRETEND']
    collection_l = ['topselling_free','topselling_paid','topgrossing','movers_shakers','topselling_free_games','topselling_paid_games','topselling_grossing_games','topselling_new_free','topselling_new_paid','topselling_new_free_games','topselling_new_paid_games']
    n_category = config['categories_to_scrape']
    n_collection = config['collections_to_scrape']
    full_app_list = []
    for country_code in config['country_code']:
        for category in category_l[:n_category]:
            for collection in collection_l[:n_collection]:
                for app_list in scroll(config['api_host'], collection, category, country_code):
                    full_app_list.extend(app_list)
    return full_app_list

def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    selected_stream_id = ['app']
    stream = catalog.get_stream('app')
    LOGGER.info("Syncing stream:" + stream.tap_stream_id)

    bookmark_column = stream.replication_key
    is_sorted = True  # TODO: indicate whether data is sorted ascending on bookmark value


    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=load_schema('app'),
        key_properties=stream.key_properties,
    )

    # TODO: delete and replace this inline function with your own data retrieval process:
    print(config)
    app_ids = app_list(config)

    for row in tap_data(app_ids):
        # TODO: place type conversions or transformations here
        # write one or more rows to the stream:
        singer.write_record(stream.tap_stream_id, row)
        if bookmark_column:
            if is_sorted:
                # update bookmark to latest value
                singer.write_state({stream.tap_stream_id: row[bookmark_column]})
            else:
                # if data unsorted, save max value until end of writes
                max_bookmark = max(max_bookmark, row[bookmark_column])
    if bookmark_column and not is_sorted:
        singer.write_state({stream.tap_stream_id: max_bookmark})
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
