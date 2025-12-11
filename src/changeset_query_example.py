
# Example: Query OSM Changesets for London
import requests
from datetime import datetime

# London bounding box
bbox = (-0.6, 51.3, 0.3, 51.7)  # west, south, east, north

# Date range
start_date = "2020-01-01"
end_date = "2024-01-01"

# OSM Changeset API
url = "https://api.openstreetmap.org/api/0.6/changesets"
params = {
    'bbox': f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}",
    'time': f"{start_date}T00:00:00Z,{end_date}T23:59:59Z",
    'closed': 'true'
}

response = requests.get(url, params=params)
# Parse XML response to get changeset data
