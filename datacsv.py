import requests
import pandas as pd
import time

API_KEY = open("api_key.txt").read().strip()
BASE_URL = "https://app.ticketmaster.com/discovery/v2/events.json"

all_events = []

# Load page 0 to find page count
params = {
    "apikey": API_KEY,
    "classificationName": "Music",
    "countryCode": "US",
    "page": 0
}

resp = requests.get(BASE_URL, params=params)
data = resp.json()

if "page" not in data:
    print("Error loading first page. Check API key / URL.")
    exit()

total_pages = data["page"]["totalPages"]
print("Total pages:", total_pages)

# Fetch only 10 pages for testing
pages_to_fetch = min(10, total_pages)

for page in range(pages_to_fetch):
    params["page"] = page
    resp = requests.get(BASE_URL, params=params)

    if resp.status_code != 200:
        print(f"Error {resp.status_code} on page {page}, skipping...")
        continue

    data = resp.json()

    if "_embedded" not in data:
        break

    events = data["_embedded"]["events"]

    for ev in events:

        venue = ev.get("_embedded", {}).get("venues", [{}])[0]
        attractions = ev.get("_embedded", {}).get("attractions", [])

        # artist popularity approximations
        if attractions:
            artist = attractions[0]
            artist_name = artist.get("name")
            artist_num_links = len(artist.get("externalLinks", {}))
            artist_num_images = len(artist.get("images", []))
        else:
            artist_name = None
            artist_num_links = 0
            artist_num_images = 0

        # price ranges
        priceRanges = ev.get("priceRanges", [{}])
        price_min = priceRanges[0].get("min")
        price_max = priceRanges[0].get("max")

        row = {
            # Event info
            "event_id": ev.get("id"),
            "event_name": ev.get("name"),
            "event_url": ev.get("url"),
            "status_code": ev.get("dates", {}).get("status", {}).get("code"),
            "start_date": ev.get("dates", {}).get("start", {}).get("localDate"),

            # Classification
            "segment": ev.get("classifications", [{}])[0].get("segment", {}).get("name"),
            "genre": ev.get("classifications", [{}])[0].get("genre", {}).get("name"),
            "subgenre": ev.get("classifications", [{}])[0].get("subGenre", {}).get("name"),

            # Venue
            "venue_name": venue.get("name"),
            "venue_city": venue.get("city", {}).get("name"),
            "venue_state": venue.get("state", {}).get("stateCode"),
            "venue_country": venue.get("country", {}).get("countryCode"),
            "venue_lat": venue.get("location", {}).get("latitude"),
            "venue_lon": venue.get("location", {}).get("longitude"),
            "venue_upcoming_events": venue.get("upcomingEvents", {}).get("ticketmaster"),
            "venue_market_name": venue.get("markets", [{}])[0].get("name"),

            # Promoter
            "promoter_name": ev.get("promoter", {}).get("name"),

            # Artist
            "artist_name": artist_name,
            "artist_num_links": artist_num_links,
            "artist_num_images": artist_num_images,

            # Pricing
            "price_min": price_min,
            "price_max": price_max,

            # Flags
            "offers_exist": 1 if ev.get("offers") else 0,
            "seatmap_exists": 1 if ev.get("seatmap") else 0
        }

        all_events.append(row)

    print(f"Page {page+1}/{pages_to_fetch} — Total events: {len(all_events)}")
    time.sleep(0.2)

df = pd.DataFrame(all_events)
df.to_csv("ticketmaster_music_events_detailed.csv", index=False)

print("\nCSV saved as ticketmaster_music_events_detailed.csv")
print("Total rows:", len(df))
