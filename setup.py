#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-google-play",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_google_play"],
    install_requires=[
        "singer-python",
        "requests",
        "google_play_scraper"
    ],
    entry_points="""
    [console_scripts]
    tap-google-play=tap_google_play:main
    """,
    packages=["tap_google_play"],
    package_data = {
        "schemas": ["tap_google_play/schemas/*.json"]
    },
    include_package_data=True,
)
