import json
import time

# Sample data simulating Facebook Ads
facebook_ads_data = [
    '{"ad_id": "AD101", "campaign_id": "CMP001", "impressions": 100, "clicks": 10, "spend": 50.0}',
    '{"ad_id": "AD102", "campaign_id": "CMP002", "impressions": 200, "clicks": 30, "spend": 75.0}',
    '{"ad_id": "AD103", "campaign_id": "CMP003", "impressions": 150, "clicks": 15, "spend": 60.0}',
]

# Process each "streamed" record
for record_str in facebook_ads_data:
    # Simulate streaming delay
    time.sleep(1)

    # Parse JSON string
    record = json.loads(record_str)

    # Calculate CTR (Click-Through Rate)
    impressions = record.get("impressions", 0)
    clicks = record.get("clicks", 0)
    ctr = (clicks / impressions) if impressions else 0

    # Output processed result
    print(f"Ad ID: {record['ad_id']}, Campaign: {record['campaign_id']}, CTR: {ctr:.2f}, Spend: {record['spend']}")