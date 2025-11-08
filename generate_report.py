import hashlib
from urllib.error import URLError

import pandas as pd
from datetime import datetime, timedelta

# URL to the CSV file from the Government Open Data portal.
csv_url = 'https://open.canada.ca/data/en/datastore/dump/92bec4b7-6feb-4215-a5f7-61da342b2354'  # Replace with the actual URL if necessary

# Read the CSV file into a DataFrame.
try:
    df = pd.read_csv(csv_url)
    data_from_remote = True
except URLError:
    df = pd.read_csv('consultations_chng_log.csv')
    data_from_remote = False


# Calculate the hash of each row (excluding the hash and timestamp columns if they already exist)
# and add it to a new 'hash' column.
# Convert the output of pd.util.hash_pandas_object to a string before encoding
df['hash'] = df.apply(lambda row: hashlib.sha256(str(pd.util.hash_pandas_object(row.drop(['hash', 'datetime'], errors='ignore'))).encode('utf-8')).hexdigest(), axis=1)

# Add current datetime
df['row_chng_datetime'] = datetime.now()

# Create the 'composite_key' column
df['composite_key'] = df['owner_org'].astype(str) + "-" + df['registration_number'].astype(str)

# Move 'composite_key' to the first column position
cols = ['composite_key'] + [col for col in df.columns if col != 'composite_key']
df = df[cols]

# Check if the log file exists. If not, create it with the current data.
if data_from_remote:
    try:
        existing_df = pd.read_csv('consultations_chng_log.csv')
    except FileNotFoundError:
        df.to_csv('consultations_chng_log.csv', index=False)
        print("Log file created.")
        newly_appended_rows = pd.DataFrame()
        appended_count = 0
    else:
        # Identify rows in the new DataFrame that are not present in the existing log file
        # using the 'hash' and 'composite_key' columns
        merged_df = df.merge(existing_df[['composite_key', 'hash']], on=['composite_key', 'hash'], how='left', indicator=True)
        rows_to_append = merged_df[merged_df['_merge'] == 'left_only'].drop(columns='_merge')

        # Append the new rows to the existing log file
        if not rows_to_append.empty:
            rows_to_append.to_csv('consultations_chng_log.csv', mode='a', header=False, index=False)
            print(f"{len(rows_to_append)} new rows appended to consultations_chng_log.csv")
            newly_appended_rows = rows_to_append
            appended_count = len(rows_to_append)
        else:
            print("No new rows to append.")
            newly_appended_rows = pd.DataFrame()
            appended_count = 0
else:
    newly_appended_rows = pd.DataFrame()
    appended_count = 0

print("\nNewly appended rows (if any):")
print(newly_appended_rows)
print(f"\nTotal rows appended in this run: {appended_count}")

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
html_p5m5_start = p5m5_start_df.to_html(index=False, classes="data-table", border=0)
p5m5_start_df.to_csv("p5m5_start.csv", index=False)

# 2. Consultations ending between m5 and p5.
p5m5_close_df = subset_df[subset_df['end_date'].dt.date.between(m5, p5)]
p5m5_close_df = p5m5_close_df.sort_values(by='end_date', ascending=False)
html_p5m5_close = p5m5_close_df.to_html(index=False, classes="data-table", border=0)
p5m5_close_df.to_csv("p5m5_close.csv", index=False)

# 3. Late closing consultations (status 'O' and end_date before today).
late_close_df = subset_df[(subset_df['status'] == 'O') & (subset_df['end_date'].dt.date < today)]
late_close_df = late_close_df.sort_values(by='end_date', ascending=False)
html_late_close = late_close_df.to_html(index=False, classes="data-table", border=0)
late_close_df.to_csv("late_close.csv", index=False)

# 4. Early closing consultations (status 'C' and end_date after today).
early_close_df = subset_df[(subset_df['status'] == 'C') & (subset_df['end_date'].dt.date > today)]
early_close_df = early_close_df.sort_values(by='end_date', ascending=False)
html_early_close = early_close_df.to_html(index=False, classes="data-table", border=0)
early_close_df.to_csv("early_close.csv", index=False)

# 5. Late starting consultations (status 'P' and start_date before today).
late_start_df = subset_df[(subset_df['status'] == 'P') & (subset_df['start_date'].dt.date < today)]
late_start_df = late_start_df.sort_values(by='start_date', ascending=False)
html_late_start = late_start_df.to_html(index=False, classes="data-table", border=0)
late_start_df.to_csv("late_start.csv", index=False)

# Create the final HTML page by injecting the tables into a template.
generated_datetime = datetime.now()
generated_datetime_str = generated_datetime.strftime("%Y-%m-%d %H:%M:%S")
generated_date_str = generated_datetime.strftime("%Y-%m-%d")
range_start_str = m5.strftime("%Y-%m-%d")
range_end_str = p5.strftime("%Y-%m-%d")

html_template = f"""<!DOCTYPE html>
<html dir="ltr" lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <meta
      name="description"
      content="Consultations Tracker report summarizing upcoming consultation activity."
    />
    <title>Consultations Tracker Report</title>
    <link
      rel="stylesheet"
      href="https://cdn.design-system.alpha.canada.ca/@gcds-core/css-shortcuts@1.0.1/dist/gcds-css-shortcuts.min.css"
    />
    <link
      rel="stylesheet"
      href="https://cdn.design-system.alpha.canada.ca/@cdssnc/gcds-components@0.43.1/dist/gcds/gcds.css"
    />
    <script
      type="module"
      src="https://cdn.design-system.alpha.canada.ca/@cdssnc/gcds-components@0.43.1/dist/gcds/gcds.esm.js"
    ></script>
    <style>
      .table-wrapper {{
        overflow-x: auto;
        margin-block: 1.5rem;
      }}

      table {{
        width: 100%;
        border-collapse: collapse;
        min-width: 640px;
      }}

      th,
      td {{
        padding: 0.75rem;
        border: 1px solid #d6d6d6;
        text-align: left;
      }}

      th {{
        background-color: #26374a;
        color: #ffffff;
      }}

      tr:nth-child(even) {{
        background-color: #f5f5f5;
      }}

      .page-layout {{
        display: grid;
        gap: 2rem;
      }}

      @media (min-width: 64em) {{
        .page-layout {{
          grid-template-columns: minmax(220px, 280px) 1fr;
        }}
      }}

      .side-nav {{
        position: sticky;
        top: 2rem;
        align-self: start;
      }}

      .page-content > section + section {{
        margin-block-start: 2rem;
      }}

      .table-of-contents {{
        margin-block-start: 2rem;
      }}

      .table-of-contents ul {{
        margin: 0;
        padding-inline-start: 1.25rem;
      }}

      .table-of-contents li {{
        margin-block-end: 0.75rem;
      }}
    </style>
  </head>
  <body>
    <gcds-header
      lang-href="https://patlittle.github.io/Consultations-Tracker/report.html"
      skip-to-href="#main-content"
    >
      <gcds-breadcrumbs slot="breadcrumb">
        <gcds-breadcrumbs-item href="https://patlittle.github.io/Consultations-Tracker/">
          Consultations Tracker
        </gcds-breadcrumbs-item>
        <gcds-breadcrumbs-item href="https://patlittle.github.io/Consultations-Tracker/report.html">
          Report
        </gcds-breadcrumbs-item>
      </gcds-breadcrumbs>
    </gcds-header>
    <gcds-container
      id="main-content"
      main-container
      size="xl"
      centered
      tag="main"
    >
      <div class="page-layout">
        <aside class="side-nav" aria-label="Consultations Tracker navigation">
          <gcds-side-nav label="Consultations Tracker navigation">
            <gcds-nav-link
              href="https://patlittle.github.io/Consultations-Tracker/report.html"
              current
            >
              Consultations Tracker Report
            </gcds-nav-link>
            <gcds-nav-link href="https://patlittle.github.io/Consultations-Tracker/changelog.html">
              Change Log Report
            </gcds-nav-link>
            <gcds-nav-link href="https://open.canada.ca/data/en/dataset/7c03f039-3753-4093-af60-74b0f7b2385d">
              Source Open Data Set
            </gcds-nav-link>
            <gcds-nav-link href="https://www.canada.ca/en/government/system/consultations/consultingcanadians.html">
              Consulting with Canadians
            </gcds-nav-link>
          </gcds-side-nav>
        </aside>
        <div class="page-content">
          <section>
            <gcds-heading tag="h1">Consultations Tracker Report</gcds-heading>
            <gcds-notice type="success" notice-title-tag="h2" notice-title="Report Generated">
              <gcds-text>{generated_datetime_str}</gcds-text>
            </gcds-notice>
            <gcds-text>
              Explore the sections below for details about consultations starting,
              ending, and changing statuses.
            </gcds-text>
          </section>
          <section class="table-of-contents" aria-label="On this page">
            <gcds-heading tag="h2">On this page</gcds-heading>
            <ul class="list-disc mb-300">
              <li class="mb-75">
                <gcds-link href="#consultations-starting">
                  Consultations Starting Between {range_start_str} and {range_end_str}
                </gcds-link>
              </li>
              <li class="mb-75">
                <gcds-link href="#consultations-ending">
                  Consultations Ending Between {range_start_str} and {range_end_str}
                </gcds-link>
              </li>
              <li class="mb-75">
                <gcds-link href="#late-closing">
                  Late Closing Consultations (Status 'O')
                </gcds-link>
              </li>
              <li class="mb-75">
                <gcds-link href="#early-closing">
                  Early Closing Consultations (Status 'C')
                </gcds-link>
              </li>
              <li>
                <gcds-link href="#late-starting">
                  Late Starting Consultations (Status 'P')
                </gcds-link>
              </li>
            </ul>
          </section>
          <section id="consultations-starting">
            <gcds-heading tag="h2">
              🆕🔜Consultations Starting Between {range_start_str} and {range_end_str}
            </gcds-heading>
            <div class="table-wrapper">
              {html_p5m5_start}
            </div>
          </section>
          <section id="consultations-ending">
            <gcds-heading tag="h2">
              ⌛🔚Consultations Ending Between {range_start_str} and {range_end_str}
            </gcds-heading>
            <div class="table-wrapper">
              {html_p5m5_close}
            </div>
          </section>
          <section id="late-closing">
            <gcds-heading tag="h2">
              😴Late Closing Consultations (Status 'O')
            </gcds-heading>
            <div class="table-wrapper">
              {html_late_close}
            </div>
          </section>
          <section id="early-closing">
            <gcds-heading tag="h2">
              🏎️Early Closing Consultations (Status 'C')
            </gcds-heading>
            <div class="table-wrapper">
              {html_early_close}
            </div>
          </section>
          <section id="late-starting">
            <gcds-heading tag="h2">
              🐌Late Starting Consultations (Status 'P')
            </gcds-heading>
            <div class="table-wrapper">
              {html_late_start}
            </div>
          </section>
          <gcds-date-modified>{generated_date_str}</gcds-date-modified>
        </div>
      </div>
    </gcds-container>
    <gcds-footer display="simple"></gcds-footer>
  </body>
</html>
"""

# Write the final HTML into a file.
with open("report.html", "w", encoding="utf-8") as f:
    f.write(html_template)

# Create the final HTML page by injecting the tables into a template.
chng_log_template = f"""<!DOCTYPE html>
<html dir="ltr" lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <meta
      name="description"
      content="Change log view of consultation updates sourced from the Consultations Tracker."
    />
    <title>Consultations Change Log Report</title>
    <link
      rel="stylesheet"
      href="https://cdn.design-system.alpha.canada.ca/@gcds-core/css-shortcuts@1.0.1/dist/gcds-css-shortcuts.min.css"
    />
    <link
      rel="stylesheet"
      href="https://cdn.design-system.alpha.canada.ca/@cdssnc/gcds-components@0.43.1/dist/gcds/gcds.css"
    />
    <script
      type="module"
      src="https://cdn.design-system.alpha.canada.ca/@cdssnc/gcds-components@0.43.1/dist/gcds/gcds.esm.js"
    ></script>
    <style>
      .page-layout {{
        display: grid;
        gap: 2rem;
      }}

      @media (min-width: 64em) {{
        .page-layout {{
          grid-template-columns: minmax(220px, 280px) 1fr;
        }}
      }}

      .side-nav {{
        position: sticky;
        top: 2rem;
        align-self: start;
      }}

      .page-content > section + section {{
        margin-block-start: 2rem;
      }}

      .iframe-wrapper {{
        margin-block-start: 2rem;
      }}

      .iframe-wrapper iframe {{
        width: 100%;
        min-height: 70vh;
        border: none;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
      }}
    </style>
  </head>
  <body>
    <gcds-header
      lang-href="https://patlittle.github.io/Consultations-Tracker/changelog.html"
      skip-to-href="#main-content"
    >
      <gcds-breadcrumbs slot="breadcrumb">
        <gcds-breadcrumbs-item href="https://patlittle.github.io/Consultations-Tracker/">
          Consultations Tracker
        </gcds-breadcrumbs-item>
        <gcds-breadcrumbs-item href="https://patlittle.github.io/Consultations-Tracker/changelog.html">
          Change Log Report
        </gcds-breadcrumbs-item>
      </gcds-breadcrumbs>
    </gcds-header>
    <gcds-container
      id="main-content"
      main-container
      size="xl"
      centered
      tag="main"
    >
      <div class="page-layout">
        <aside class="side-nav" aria-label="Consultations Tracker navigation">
          <gcds-side-nav label="Consultations Tracker navigation">
            <gcds-nav-link href="https://patlittle.github.io/Consultations-Tracker/report.html">
              Consultations Tracker Report
            </gcds-nav-link>
            <gcds-nav-link
              href="https://patlittle.github.io/Consultations-Tracker/changelog.html"
              current
            >
              Change Log Report
            </gcds-nav-link>
            <gcds-nav-link href="https://open.canada.ca/data/en/dataset/7c03f039-3753-4093-af60-74b0f7b2385d">
              Source Open Data Set
            </gcds-nav-link>
            <gcds-nav-link href="https://www.canada.ca/en/government/system/consultations/consultingcanadians.html">
              Consulting with Canadians
            </gcds-nav-link>
          </gcds-side-nav>
        </aside>
        <div class="page-content">
          <section>
            <gcds-heading tag="h1">Consultations Change Log Report</gcds-heading>
            <gcds-text>
              Report generated on {generated_datetime_str}. Review the embedded table to see the
              latest updates to consultation records, ordered by the most recent change first.
            </gcds-text>
            <gcds-text>
              Use the side navigation to move between resources related to the Consultations Tracker initiative.
            </gcds-text>
          </section>
          <section class="iframe-wrapper">
            <iframe
              src="https://flatgithub.com/PatLittle/Consultations-Tracker/blob/master/consultations_chng_log.csv?filename=consultations_chng_log.csv&sort=row_chng_datetime%2Cdesc&stickyColumnName=row_chng_datetime"
              title="Consultations Tracker change log table"
            ></iframe>
          </section>
          <gcds-date-modified>{generated_date_str}</gcds-date-modified>
        </div>
      </div>
    </gcds-container>
    <gcds-footer display="simple"></gcds-footer>
  </body>
</html>
"""

# Write the final HTML into a file.
with open("changelog.html", "w", encoding="utf-8") as f:
    f.write(chng_log_template)
