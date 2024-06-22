# This module is runned once to get the data from Genius website and save it to a csv file.


import logging
import time

import pandas as pd
from bs4 import BeautifulSoup
import requests

logging.basicConfig(level=logging.INFO)

def get_genius_tag(url: str, number_tags: int = 1) -> list[str]:
    """
    WEBSCRAPING IS INVOLVED, IT'S NOT VERY LEGAL AND THE FUNCTION MIGHT BREAK IN THE FUTURE.
    get_genius_tag: Get the tags of a song from the Genius website. 
    :param url: The URL of the song on Genius.
    :param number_tags: The number of tags to get.
    """
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    finder = soup.find("div", class_="SongTags__Title-xixwg3-0 ceKRFE")
    tags = []
    if finder is None:
        return tags
    
    tag_finder = finder
    
    for i in range(number_tags):
        tag_finder = tag_finder.find_next("a")
        tags.append(tag_finder.text)
    
    return list(filter(None, tags))

if __name__ == "__main__":
    # importing raw data
    song_data = pd.read_csv("./data/all_songs_data_raw.csv")
    
    # creating unique ID for each song
    song_data = song_data.reset_index().rename(columns={'index': 'id'})

    # renaming columns
    song_data = song_data.rename(columns={'Song Ttile': 'title',
                                        'Artist': 'artist',
                                        'Album': 'album',
                                        'Album URL': 'album_url',
                                        'Featured Artists': 'featured_artists',
                                        'Lyrics': 'lyrics',
                                        'Media': 'media',
                                        'Rank': 'rank',
                                        'Release Date': 'release_date',
                                        'Writers': 'writers',
                                        'Year': 'year',
                                        'Song URL': 'song_url'})
    
    # scrapping tags from Genius with help of get_genius_tag function
    # iterating through each row of the data
    for index, row in song_data.iterrows():

        # setting a sleep time after bacth of querries to avoid getting blocked and to avoid overloading the server 
        # I am not sure if this is necessary as the total load remains low and the implementation is quite slow, but I am doing it anyway
        if index % 50 == 0:
            logging.info(f"Processing {index}th row")
            time.sleep(10)
            
        # if the tags are already present, skip the row
        if not pd.isnull(row["tags"]):
            continue
    
        url = row["Song URL"]
        
        # if the url is not present, skip the row
        if pd.isnull(url):
            song_data.loc[index, "tags"] = None
            continue
        
        # getting the tags
        tags = get_genius_tag(url, 1)

        # if the tags are not present, set it to an empty list
        if not tags:
            song_data.loc[index, "tags"] = None
            
        # else set the tags
        else:
            song_data.loc[index, "tags"] = tags[0]
        
    # saving the data to a parquet file
    song_data.to_csv("./data/all_songs_data_labeled.csv")