import re
import csv
from bs4 import BeautifulSoup

# === Load the file ===
with open("data/raw_data/Mon activité/YouTube/MonActivité.html", "r", encoding="utf-8") as f:
    html = f.read()

# === Parse HTML ===
soup = BeautifulSoup(html, "html.parser")

# === Prepare CSV output ===
output = []
header = ["Date", "Time", "Action", "Title", "Channel", "URL"]

# === Extract all video activity blocks ===
entries = soup.find_all("div", class_="outer-cell")

for entry in entries:
    content = entry.get_text(separator="\n", strip=True)
    
    # Extract YouTube URL and title
    link_tag = entry.find("a", href=re.compile(r"^https://www\.youtube\.com/watch\?v="))
    if not link_tag:
        continue

    url = link_tag['href']
    title = link_tag.text.strip()

    # Try to extract channel
    channel_tag = link_tag.find_next("a")
    if channel_tag and channel_tag['href'].startswith("https://www.youtube.com/channel/"):
        channel = channel_tag.text.strip()
    else:
        # fallback
        channel = "Unknown"

    # Determine action type
    if "Vous avez regardé" in content:
        action = "Watched"
    elif "Vous avez recherché" in content:
        action = "Searched"
    else:
        action = "Unknown"

    # Extract date and time
    datetime_match = re.search(r"(\d{1,2} \w+ 2025), (\d{2}:\d{2}:\d{2}) CEST", content)
    if datetime_match:
        date_str = datetime_match.group(1)
        time_str = datetime_match.group(2)
    else:
        date_str = ""
        time_str = ""

    output.append([date_str, time_str, action, title, channel, url])

with open("youtube_history.csv", "w", newline='', encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(header)
    writer.writerows(output)

print("Done. Saved to youtube_history.csv")
