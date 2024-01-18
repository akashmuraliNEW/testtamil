from requests_html import HTMLSession
from dataclasses import dataclass
from typing import List
from datetime import datetime
from pymongo import MongoClient
from time import sleep
import feedparser
from telegram import Bot
from telegram.ext import ParseMode



@dataclass
class Movie:
    """
    Represents movie information.
    
    Attributes:
        name (str): The name of the movie.
        release_datetime (datetime): The release date and time of the movie.
        poster_url (str): The URL of the movie poster.
        screenshots (List[str]): List of screenshot URLs.
        torrents (List[Torrent]): List of torrent data.
    """
    name: str
    release_datetime: datetime
    poster_url: str
    screenshots: List[str]
    torrents: List['Torrent']

    def __str__(self):
        return f"Movie: {self.name} (Released on: {self.release_datetime})"

@dataclass
class Torrent:
    """
    Represents torrent data.
    
    Attributes:
        file_name (str): The name of the torrent file.
        torrent_link (str): The URL to download the torrent file.
        magnet_link (str): The magnet link for the torrent.
    """
    file_name: str
    torrent_link: str
    magnet_link: str

    def __str__(self):
        return f"Torrent File: {self.file_name}"

def scrape_from_url(url: str) -> Movie:
    """
    Scrape movie information from a given URL.
    
    Args:
        url (str): The URL of the movie page to scrape.
        
    Returns:
        Movie: A Movie object containing scraped information.
    """

    session = HTMLSession()
    response = session.get(url)
    page = response.html

    # Scrape data (same as your existing code)
    name = page.find("h3")[0].text
    release_datetime_str = page.find("time")[0].attrs["datetime"]
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    release_datetime = datetime.strptime(release_datetime_str, date_format)
    img_tags = page.find("img.ipsImage")
    pics = [img.attrs["src"] for img in img_tags if img.attrs["src"].lower().split(".")[-1] in ("jpg", "jpeg", "png")]
    poster_url = pics[0] if pics else ""
    screenshots = pics[1:]
    magnet_links = [a.attrs["href"] for a in page.find("a.skyblue-button")]
    torrent_links = [a.attrs["href"] for a in page.find("a[data-fileext='torrent']")]
    file_names = [span.text.strip() for span in page.find('span[style="color:#0000ff;"]')]

    # Create Torrent objects
    torrents = [Torrent(file_name, torrent_link, magnet_link) for file_name, torrent_link, magnet_link in zip(file_names, torrent_links, magnet_links)]

    # Create and return a Movie object
    movie = Movie(name, release_datetime, poster_url, screenshots, torrents)
    return movie

MONGODB_CONNECTION_STRING = "mongodb+srv://spotify:spotify@cluster0.tmcsezs.mongodb.net/?retryWrites=true&w=majority"
DATABASE_NAME = "movie_links_db"
COLLECTION_NAME = "movie_links"

def initialize_database():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client[DATABASE_NAME]
    db[COLLECTION_NAME].create_index("link", unique=True)
    client.close()

def load_previous_movie_links():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client[DATABASE_NAME]
    previous_links = {doc["link"] for doc in db[COLLECTION_NAME].find()}
    client.close()
    return previous_links

def save_movie_link(link):
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client[DATABASE_NAME]
    db[COLLECTION_NAME].insert_one({"link": link})
    client.close()

def process_new_movie_data(movie_data: Movie, bot_token: str, chat_id: str):
    bot = Bot(token=bot_token)

    # Customize the message format based on your needs
    message = f"*New Movie Release*\n\n{movie_data}\n\n[View Details]({movie_data.poster_url})"
    
    # Send the message to the Telegram channel
    bot.send_message(chat_id=chat_id, text=message, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)

if __name__ == "__main__":
    # Example usage:
    rss_url = "https://www.1tamilmv.world/index.php?/forums/forum/11-web-hd-itunes-hd-bluray.xml"
    telegram_bot_token = "1991263740:AAG4ujt3Y5PivwFMIvZg2Ysfk2cSw_p3q4c"
    telegram_channel_id = "-1001587722715"

    # Initialize the database and load previous movie links
    initialize_database()
    previous_movie_links = load_previous_movie_links()

    while True:
        feed = feedparser.parse(rss_url)

        for entry in feed.entries:
            movie_url = entry.link

            # Check if it's a new movie link
            if movie_url not in previous_movie_links:
                movie_data = scrape_from_url(movie_url)

                # Process the new movie data and send to Telegram channel
                process_new_movie_data(movie_data, telegram_bot_token, telegram_channel_id)

                # Save the link to the database
                save_movie_link(movie_url)

                # Update the set of processed links
                previous_movie_links.add(movie_url)

        # Sleep for a while before checking for updates again (adjust as needed)
        sleep(3600)  # Sleep for 1 hour
