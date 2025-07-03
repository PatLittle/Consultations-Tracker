import hashlib
import pandas as pd
from datetime import datetime, timedelta

# Define the required column order for the change log
LOG_COL_ORDER = [
    'composite_key', 'registration_number', 'partner_departments', 'subjects',
    'title_en', 'title_fr', 'description_en', 'description_fr',
    'start_date', 'end_date', 'status',
    'profile_page_en', 'profile_page_fr',
    'report_available_online', 'report_link_en', 'report_link_fr',
    'owner_org', 'owner_org_title', 'hash', 'row_chng_datetime'
]

def compute_row_hash(row, exclude_cols=None):
    """Compute a SHA256 hash for a pandas Series, excluding certain columns."""
    if exclude_cols is None:
        exclude_cols = []
    row_data = tuple(row.drop(labels=exclude_cols, errors='ignore').astype(str))
    return hashlib.sha256(str(row_data).encode('utf-8')).hexdigest()

# URL to the CSV file from the Government Open Data portal.
csv_url = 'https://open.canada.ca/data/en/datastore/dump/92bec4b7-6feb-4215-a5f7-61da342b2354'

# Load latest data & compute hashes
df = pd.read_csv(csv_url)
df['row_chng_datetime'] = datetime.now()
df['hash'] = df.apply(lambda row: compute_row_hash(row, exclude_cols=['hash', 'row_chng_datetime', 'datetime']), axis=1)
df['composite_key'] = df['owner_org'].astype(str) + "-" + df['registration_number'].astype(str)

# Ensure consistent column order as specified
df = df.reindex(columns=LOG_COL_ORDER)

try:
    log_df = pd.read_csv('consultations_chng_log.csv')
except FileNotFoundError:
    log_df = pd.DataFrame(columns=LOG_COL_ORDER)

# --- Append new/changed rows to log ---
if not log_df.empty:
    merged = df.merge(log_df[['composite_key', 'hash']], on=['composite_key', 'hash'], how='left', indicator=True)
    to_append = merged[merged['_merge'] == 'left_only'].drop(columns='_merge')
else:
    to_append = df.copy()

if not to_append.empty:
    to_append = to_append.reindex(columns=LOG_COL_ORDER)
    to_append.to_csv('consultations_chng_log.csv', mode='a', header=log_df.empty, index=False)
    print(f"{len(to_append)} new or changed rows appended.")
else:
    print("No new rows to append.")

# --- Detect and log deletions ---
existing_keys = set(df['composite_key'])
log_not_deleted = log_df[~log_df['status'].fillna('').eq('DELETED')] if 'status' in log_df.columns else log_df
to_delete = log_not_deleted[~log_not_deleted['composite_key'].isin(existing_keys)].copy()

if not to_delete.empty:
    to_delete['status'] = 'DELETED'
    to_delete['row_chng_datetime'] = datetime.now()
    to_delete['hash'] = to_delete.apply(lambda row: compute_row_hash(row, exclude_cols=['hash', 'row_chng_datetime', 'datetime']), axis=1)
    to_delete = to_delete.reindex(columns=LOG_COL_ORDER)
    hashes_in_log = set(log_df['hash'])
    new_deletions = to_delete[~to_delete['hash'].isin(hashes_in_log)]
    if not new_deletions.empty:
        new_deletions.to_csv('consultations_chng_log.csv', mode='a', header=False, index=False)
        print(f"{len(new_deletions)} deletions logged as DELETED.")
    else:
        print("No new deletions to log.")
else:
    print("No deleted rows detected.")

# --- Create deleted-only CSV ---
chng_log = pd.read_csv('consultations_chng_log.csv')
if 'status' in chng_log.columns:
    deleted_records = chng_log[chng_log['status'] == 'DELETED']
else:
    deleted_records = pd.DataFrame(columns=chng_log.columns)
deleted_records = deleted_records.reindex(columns=LOG_COL_ORDER)
deleted_records.to_csv('consultations_deleted_log.csv', index=False)
print(f"Deleted records exported to consultations_deleted_log.csv ({len(deleted_records)} rows).")

# --- Remainder: reporting/HTML output (unchanged from your script) ---

# Convert date columns to datetime objects.
df['start_date'] = pd.to_datetime(df['start_date'])
df['end_date'] = pd.to_datetime(df['end_date'])

# Define date ranges.
today = datetime.today().date()
p5 = today + timedelta(days=5)
m5 = today - timedelta(days=5)

# Select a subset of columns for our report.
subset_df = df[['registration_number', 'title_en', 'start_date', 'end_date', 'status', 'owner_org']]

# 1. Consultations starting between m5 and p5.
p5m5_start_df = subset_df[subset_df['start_date'].dt.date.between(m5, p5)]
p5m5_start_df = p5m5_start_df.sort_values(by='start_date', ascending=False)
html_p5m5_start = p5m5_start_df.to_html(index=False, classes="table table-striped")
p5m5_start_df.to_csv("p5m5_start.csv", index=False)

# 2. Consultations ending between m5 and p5.
p5m5_close_df = subset_df[subset_df['end_date'].dt.date.between(m5, p5)]
p5m5_close_df = p5m5_close_df.sort_values(by='end_date', ascending=False)
html_p5m5_close = p5m5_close_df.to_html(index=False, classes="table table-striped")
p5m5_close_df.to_csv("p5m5_close.csv", index=False)

# 3. Late closing consultations (status 'O' and end_date before today).
late_close_df = subset_df[(subset_df['status'] == 'O') & (subset_df['end_date'].dt.date < today)]
late_close_df = late_close_df.sort_values(by='end_date', ascending=False)
html_late_close = late_close_df.to_html(index=False, classes="table table-striped")
late_close_df.to_csv("late_close.csv", index=False)

# 4. Early closing consultations (status 'C' and end_date after today).
early_close_df = subset_df[(subset_df['status'] == 'C') & (subset_df['end_date'].dt.date > today)]
early_close_df = early_close_df.sort_values(by='end_date', ascending=False)
html_early_close = early_close_df.to_html(index=False, classes="table table-striped")
early_close_df.to_csv("early_close.csv", index=False)

# 5. Late starting consultations (status 'P' and start_date before today).
late_start_df = subset_df[(subset_df['status'] == 'P') & (subset_df['start_date'].dt.date < today)]
late_start_df = late_start_df.sort_values(by='start_date', ascending=False)
html_late_start = late_start_df.to_html(index=False, classes="table table-striped")
late_start_df.to_csv("late_start.csv", index=False)

# Create the final HTML page by injecting the tables into a template.
html_template = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Consultations Tracker Report</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <!-- Integrate with existing CSS -->
    <link href="https://PatLittle.github.io/Consultations-Tracker/global.css" rel="stylesheet">
    <link href="https://PatLittle.github.io/Consultations-Tracker/themes/night.css" rel="stylesheet">
    <style>
      body {{ padding: 20px; font-family: Arial, sans-serif; }}
      table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; }}
      th, td {{ padding: 8px; border: 1px solid #ddd; text-align: left; }}
      th {{ padding-top: 12px; padding-bottom: 12px; text-align: left; background-color: #04AA6D; color: white; }}
      tr:nth-child(even) {{ background-color: #f2f2f2; color: black; }}
      tr:hover {{background-color: #ddd;}}
      h2 {{ margin-top: 40px; }}
      header, footer {{ text-align: center; margin-bottom: 40px; }}
      .anchor-link {{ text-decoration: none; margin-left: 8px; color: #555; }}
      .topnav {{ overflow: hidden; background-color: #333; }}
      .topnav a {{ float: left; color: #f2f2f2; text-align: center; padding: 14px 16px;  text-decoration: none;  font-size: 17px; }}
      .topnav a:hover {{ background-color: #ddd; color: black; }}
      .topnav a.active {{ background-color: #04AA6D; color: white; }}
    </style>
</head>
<body>
    <div class="topnav">
      <a class="active" href="https://patlittle.github.io/Consultations-Tracker/report.html">Consultations Tracker Report</a>
      <a href="https://patlittle.github.io/Consultations-Tracker/changelog.html">Consultations Change Log Report</a>
      <a href="https://patlittle.github.io/Consultations-Tracker">Web Monitoring</a>
      <a href="https://open.canada.ca/data/en/dataset/7c03f039-3753-4093-af60-74b0f7b2385d">Open Data↗️</a>
      <a href="https://www.canada.ca/en/government/system/consultations/consultingcanadians.html">Consulting with Canadians↗️</a>
    </div>
    <header>
        <h1>Consultations Tracker Report</h1>
        <p>Report generated on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
    </header>
    <!-- Table of Contents -->
    <nav>
      <ul>
        <li><a href="#consultations-starting">Consultations Starting Between {m5} and {p5}</a></li>
        <li><a href="#consultations-ending">Consultations Ending Between {m5} and {p5}</a></li>
        <li><a href="#late-closing">Late Closing Consultations (Status 'O')</a></li>
        <li><a href="#early-closing">Early Closing Consultations (Status 'C')</a></li>
        <li><a href="#late-starting">Late Starting Consultations (Status 'P')</a></li>
      </ul>
    </nav>

    <section id="consultations-starting">
        <h2>🆕🔜Consultations Starting Between {m5} and {p5}
          <a href="#consultations-starting" class="anchor-link">🔗</a>
        </h2>
        {html_p5m5_start}
    </section>

    <section id="consultations-ending">
        <h2>⌛🔚Consultations Ending Between {m5} and {p5}
          <a href="#consultations-ending" class="anchor-link">🔗</a>
        </h2>
        {html_p5m5_close}
    </section>

    <section id="late-closing">
        <h2>😴Late Closing Consultations (Status 'O')
          <a href="#late-closing" class="anchor-link">🔗</a>
        </h2>
        {html_late_close}
    </section>

    <section id="early-closing">
        <h2>🏎️Early Closing Consultations (Status 'C')
          <a href="#early-closing" class="anchor-link">🔗</a>
        </h2>
        {html_early_close}
    </section>

    <section id="late-starting">
        <h2>🐌Late Starting Consultations (Status 'P')
          <a href="#late-starting" class="anchor-link">🔗</a>
        </h2>
        {html_late_start}
    </section>

    <footer>
        <p>Consultations Tracker Report</p>
        <div class="license" data-license-id="OGL-Canada-2.0">
            <div class="">
                <h3>
                    <a href="https://open.canada.ca/en/open-government-licence-canada" class="tmpl-title">Open Government License 2.0 (Canada)</a>
                    <a href="https://licenses.opendefinition.org/licenses/OGL-Canada-2.0.json" class="tmpl-title">🤖</a>
                    <div class="icons">
                        <a href="https://opendefinition.org/od/" title="Open Data" class="open-icon">
                          <img src="https://assets.okfn.org/images/ok_buttons/od_80x15_blue.png" alt="Open Data">
                        </a>
                        <a href="https://opendefinition.org/od/" title="Open Data" class="open-icon">
                          <img src="https://assets.okfn.org/images/ok_buttons/oc_80x15_red_green.png" alt="Open Data">
                        </a>
                    </div>
                </h3>
            </div>
        </div>
    </footer>
</body>
</html>
"""

# Write the final HTML into a file.
with open("report.html", "w", encoding="utf-8") as f:
    f.write(html_template)

# Create the final HTML page by injecting the tables into a template.
chng_log_template = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Consultations Change Log Report</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <!-- Integrate with existing CSS -->
    <link href="https://PatLittle.github.io/Consultations-Tracker/global.css" rel="stylesheet">
    <link href="https://PatLittle.github.io/Consultations-Tracker/themes/night.css" rel="stylesheet">
    <style>
      body {{ padding: 20px; font-family: Arial, sans-serif; }}
      table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; }}
      th, td {{ padding: 8px; border: 1px solid #ddd; text-align: left; }}
      th {{ padding-top: 12px; padding-bottom: 12px; text-align: left; background-color: #04AA6D; color: white; }}
      tr:nth-child(even) {{ background-color: #f2f2f2; color: black; }}
      tr:hover {{background-color: #ddd;}}
      h2 {{ margin-top: 40px; }}
      header, footer {{ text-align: center; margin-bottom: 40px; }}
      .anchor-link {{ text-decoration: none; margin-left: 8px; color: #555; }}
      .topnav {{ overflow: hidden; background-color: #333; }}
      .topnav a {{ float: left; color: #f2f2f2; text-align: center; padding: 14px 16px;  text-decoration: none;  font-size: 17px; }}
      .topnav a:hover {{ background-color: #ddd; color: black; }}
      .topnav a.active {{ background-color: #04AA6D; color: white; }}
      iframe {{
            width: 95vw;
            height: 95vh;
            border: none;
            box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
        }}
    </style>
</head>
<body>
    <div class="topnav">
      <a class="active" href="https://patlittle.github.io/Consultations-Tracker/changelog.html">Consultations Change Log Report</a>
      <a href="https://patlittle.github.io/Consultations-Tracker/report.html">Consultations Tracker Report</a>
      <a href="https://patlittle.github.io/Consultations-Tracker">Web Monitoring</a>
      <a href="https://open.canada.ca/data/en/dataset/7c03f039-3753-4093-af60-74b0f7b2385d">Open Data↗️</a>
      <a href="https://www.canada.ca/en/government/system/consultations/consultingcanadians.html">Consulting with Canadians↗️</a>
    </div>
    <header>
        <h1>Consultations Change Log Report</h1>
        <p>Report generated on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
    </header>
    
    <iframe src="https://flatgithub.com/PatLittle/Consultations-Tracker/blob/master/consultations_chng_log.csv?filename=consultations_chng_log.csv&sort=row_chng_datetime%2Cdesc&stickyColumnName=row_chng_datetime" title="Change Log"></iframe>
    <footer>
        <p>Consultations Change Log Report</p>
        <div class="license" data-license-id="OGL-Canada-2.0">
            <div class="">
                <h3>
                    <a href="https://open.canada.ca/en/open-government-licence-canada" class="tmpl-title">Open Government License 2.0 (Canada)</a>
                    <a href="https://licenses.opendefinition.org/licenses/OGL-Canada-2.0.json" class="tmpl-title">🤖</a>
                    <div class="icons">
                        <a href="https://opendefinition.org/od/" title="Open Data" class="open-icon">
                          <img src="https://assets.okfn.org/images/ok_buttons/od_80x15_blue.png" alt="Open Data">
                        </a>
                        <a href="https://opendefinition.org/od/" title="Open Data" class="open-icon">
                          <img src="https://assets.okfn.org/images/ok_buttons/oc_80x15_red_green.png" alt="Open Data">
                        </a>
                    </div>
                </h3>
            </div>
        </div>
    </footer>
</body>
</html>
"""

# Write the final HTML into a file.
with open("changelog.html", "w", encoding="utf-8") as f:
    f.write(chng_log_template)
