import json
import pandas as pd
from datetime import datetime

# Load JSON file
with open('data/raw_data/Historique.json', 'r') as file:
    data = json.load(file)

# Extract browser history
history = data.get("Browser History", [])

# Convert time_usec to human-readable datetime
def convert_time_usec(usec):
    # time_usec is in microseconds since epoch
    return datetime.utcfromtimestamp(usec / 1e6)

# Create DataFrame
df = pd.DataFrame(history)

# Add readable time column
df['datetime'] = df['time_usec'].apply(convert_time_usec)

# Reorder columns
columns = ['datetime', 'title', 'url', 'page_transition_qualifier', 'favicon_url', 'client_id']
df = df[columns]

# Save to CSV
df.to_csv('chrome_history_parsed.csv', index=False)

print("✅ Chrome history exported to chrome_history_parsed.csv")
