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

INVALID_URL_RULES = [
    (
        "canada-preview.adobecqms.net",
        "Canada-ca Preview Link",
    ),
    (
        "can01.safelinks.protection",
        "Office 365 Safe Links",
    ),
]
PLACEHOLDER_VALUES = {"", "na", "n/a", "s/o", "nan", "none", "null"}
INVALID_SCHEME_PATTERN = re.compile(r"^[a-z][a-z0-9+.-]*:", re.IGNORECASE)


def explain_invalid_url(value):
    """Return an invalid URL explanation, or an empty string when the URL is monitorable."""
    if pd.isna(value):
        return ""

    url = str(value).strip()
    normalized_url = url.lower()

    if normalized_url in PLACEHOLDER_VALUES:
        return "Placeholder or blank URL"

    for marker, reason in INVALID_URL_RULES:
        if marker in normalized_url:
            return reason

    if normalized_url.startswith("mailto:"):
        return "Email link is not monitorable"

    if normalized_url.startswith("www."):
        return "Missing URL scheme"

    if not normalized_url.startswith(("http://", "https://")):
        if INVALID_SCHEME_PATTERN.match(normalized_url):
            return "Unsupported URL scheme"
        return "Missing URL scheme"

    return ""


url_columns = [
    'profile_page_en',
    'profile_page_fr',
    'report_link_en',
    'report_link_fr',
]

invalid_url_details = []
bad_url_mask = pd.Series(False, index=consultations_df.index)
for index, row in consultations_df.iterrows():
    row_details = []
    for column in url_columns:
        if column not in consultations_df.columns:
            continue

        reason = explain_invalid_url(row[column])
        if reason:
            row_details.append(f"{column}: {reason}")

    invalid_url_details.append("; ".join(row_details))
    if row_details:
        bad_url_mask.at[index] = True

bad_urls_df = consultations_df.loc[bad_url_mask].copy()
bad_urls_df['invalid_url_fields'] = [
    details for details, is_bad in zip(invalid_url_details, bad_url_mask) if is_bad
]
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

# Further filter out entries where the URL is nan, blank, or not monitorable by Upptime.
filtered_data = selected_data.dropna(subset=['url']).copy()

# Normalize the name and URL values before validating them for the YAML output.
filtered_data['url'] = filtered_data['url'].astype(str).str.replace(': ', '', regex=False)
filtered_data['url'] = filtered_data['url'].str.replace('\n', '', regex=False).str.strip()
filtered_data['name'] = filtered_data['name'].astype(str).str.replace('\n', '', regex=False)
filtered_data = filtered_data[filtered_data['url'].apply(explain_invalid_url) == '']

# Update the 'sites' section in the YAML content
yaml_content['sites'] = filtered_data.to_dict(orient='records')

# Save the updated content back to the YAML file
with open(yaml_file_path, 'w', encoding='utf8') as file:
    yaml.dump(yaml_content, file, sort_keys=False, allow_unicode=True)

print("YAML file updated successfully.")
