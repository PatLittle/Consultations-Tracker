import pandas as pd
import yaml

# Path to the uploaded YAML file
yaml_file_path = '/home/runner/work/Consultations-Tracker/Consultations-Tracker/.upptimerc.yml'  # Replace with your actual YAML file path

# URL of the CSV file
csv_url = 'https://open.canada.ca/data/en/datastore/dump/92bec4b7-6feb-4215-a5f7-61da342b2354'  # Replace with the actual URL of the CSV file

# Read the existing YAML file
with open(yaml_file_path, 'r', encoding='utf8') as file:
    yaml_content = yaml.safe_load(file)

# Read the CSV file
df = pd.read_csv(csv_url)

# Filter out rows where 'status' column equals 'C'
df_filtered = df[df['status'] != 'C']

# Select specific columns and rename them for YAML
selected_data = df_filtered[['title_en', 'profile_page_en']].rename(columns={'title_en': 'name', 'profile_page_en': 'url'})

# Remove ':' from the 'url' field
selected_data['url'] = selected_data['url'].str.replace(':', '')
selected_data['url'] = selected_data['url'].str.replace('\n', '')
selected_data['name'] = selected_data['url'].str.replace('\n', '')

# Further filter out entries where the URL is nan
filtered_data = selected_data.dropna(subset=['url'])

# Update the 'sites' section in the YAML content
yaml_content['sites'] = filtered_data.to_dict(orient='records')

# Save the updated content back to the YAML file
with open(yaml_file_path, 'w', encoding='utf8') as file:
    yaml.dump(yaml_content, file, sort_keys=False, allow_unicode=True)

print("YAML file updated successfully.")
