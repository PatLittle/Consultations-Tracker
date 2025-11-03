import re
from datetime import datetime, time, timedelta

import pandas as pd
import yaml

# Path to the uploaded YAML file
yaml_file_path = '/home/runner/work/Consultations-Tracker/Consultations-Tracker/.upptimerc.yml'  # Replace with your actual YAML file path

# URL of the CSV file
csv_url = 'https://open.canada.ca/data/en/datastore/dump/92bec4b7-6feb-4215-a5f7-61da342b2354'  # Replace with the actual URL of the CSV file

# URL for the consultations CSV used to detect bad URLs
consultations_csv_url = (
    'https://open.canada.ca/data/dataset/7c03f039-3753-4093-af60-74b0f7b2385d/'
    'resource/92bec4b7-6feb-4215-a5f7-61da342b2354/download/consultations.csv'
)

# Read the existing YAML file
with open(yaml_file_path, 'r', encoding='utf8') as file:
    yaml_content = yaml.safe_load(file)

# Read the CSV file
df = pd.read_csv(csv_url)

# Download and inspect consultations for bad URLs
consultations_df = pd.read_csv(consultations_csv_url)

keywords = [
    'canada-preview.adobecqms.net',
    'can01.safelinks.protection.outlook.com',
    'NA',
    'N/A',
    'S/O',
]
keyword_pattern = '|'.join(re.escape(keyword) for keyword in keywords)
invalid_url_pattern = r'^(?!https?://|www\.|mailto:).+'

url_columns = [
    'profile_page_en',
    'profile_page_fr',
    'report_link_en',
    'report_link_fr',
]

bad_url_mask = pd.Series(False, index=consultations_df.index)
for column in url_columns:
    if column in consultations_df.columns:
        column_values = consultations_df[column].fillna('').astype(str)
        keyword_matches = column_values.str.contains(
            keyword_pattern, na=False, case=False, regex=True
        )
        invalid_matches = column_values.str.contains(
            invalid_url_pattern, na=False, regex=True
        )
        bad_url_mask = bad_url_mask | keyword_matches | invalid_matches

bad_urls_df = consultations_df.loc[bad_url_mask]
bad_urls_df.to_csv('bad-urls.csv', index=False)

# Filtering the DataFrame for rows where 'status' = 'O' and 'end_date' is before today's date
today = datetime.today().date()
p5 = today + timedelta(days=5)
m5 = today - timedelta(days=5)

subset_df = df[['registration_number', 'title_en', 'start_date', 'end_date', 'status', 'owner_org']]

p5m5_start_df = subset_df[pd.to_datetime(subset_df['start_date']).isin(pd.date_range(m5, p5))]
p5m5_start_df = p5m5_start_df.sort_values(by='start_date', ascending=False)
html_p5m5_start = p5m5_start_df.to_html(index=False)

p5m5_close_df = subset_df[pd.to_datetime(subset_df['end_date']).isin(pd.date_range(m5, p5))]
p5m5_close_df = p5m5_close_df.sort_values(by='end_date', ascending=False)
html_p5m5_close = p5m5_close_df.to_html(index=False)

late_close_df = subset_df[(subset_df['status'] == 'O') & (pd.to_datetime(subset_df['end_date']).dt.date < today)]
late_close_df = late_close_df.sort_values(by='end_date', ascending=False)
html_late_close = late_close_df.to_html(index=False)

early_close_df = subset_df[(subset_df['status'] == 'C') & (pd.to_datetime(subset_df['end_date']).dt.date > today)]
early_close_df = early_close_df.sort_values(by='end_date', ascending=False)
html_early_close = early_close_df.to_html(index=False)

late_start_df = subset_df[(subset_df['status'] == 'P') & (pd.to_datetime(subset_df['start_date']).dt.date < today)]
late_start_df = late_start_df.sort_values(by='start_date', ascending=False)
html_late_start = late_start_df.to_html(index=False)

#yaml_content['status-website']['customBodyHtml'] = ''.join("<h3>Consultations Starting +/- 5 days from today</h3>"+html_p5m5_start+"<h3>Consultations Ending +/- 5 days from today</h3>"+html_p5m5_close+"<h3>Consultations Listed as Open that should be closed</h3>"+html_late_close)

# Filter out rows where 'status' column equals 'C'
df_filtered = df[df['status'] != 'C']

# Select specific columns and rename them for YAML
selected_data = df_filtered[['title_en', 'profile_page_en']].rename(columns={'title_en': 'name', 'profile_page_en': 'url'})

# Further filter out entries where the URL is nan
filtered_data = selected_data.dropna(subset=['url'])

# Remove ':' from the 'url' field
filtered_data['url'] = filtered_data['url'].str.replace(': ', '')
filtered_data['url'] = filtered_data['url'].str.replace('\n', '')
filtered_data['name'] = filtered_data['name'].str.replace('\n', '')

# Update the 'sites' section in the YAML content
yaml_content['sites'] = filtered_data.to_dict(orient='records')

# Save the updated content back to the YAML file
with open(yaml_file_path, 'w', encoding='utf8') as file:
    yaml.dump(yaml_content, file, sort_keys=False, allow_unicode=True)

print("YAML file updated successfully.")
