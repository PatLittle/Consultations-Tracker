import pandas as pd
from datetime import datetime, timedelta

# URL to the CSV file from the Government Open Data portal.
csv_url = 'https://open.canada.ca/data/en/datastore/dump/92bec4b7-6feb-4215-a5f7-61da342b2354'  # Replace with the actual URL if necessary

# Read the CSV file into a DataFrame.
df = pd.read_csv(csv_url)

# Convert date columns to datetime objects.
df['start_date'] = pd.to_datetime(df['start_date'])
df['end_date'] = pd.to_datetime(df['end_date'])

# Define date ranges.
today = datetime.today().date()
p5 = today + timedelta(days=5)
m5 = today - timedelta(days=5)

# Select a subset of columns for our report.
subset_df = df[['registration_number', 'title_en', 'start_date', 'end_date', 'status', 'owner_org']]

# Consultations starting within 5 days before/after today.
p5m5_start_df = subset_df[subset_df['start_date'].dt.date.between(m5, p5)]
p5m5_start_df = p5m5_start_df.sort_values(by='start_date', ascending=False)
html_p5m5_start = p5m5_start_df.to_html(index=False, classes="table table-striped")

# Consultations ending within 5 days before/after today.
p5m5_close_df = subset_df[subset_df['end_date'].dt.date.between(m5, p5)]
p5m5_close_df = p5m5_close_df.sort_values(by='end_date', ascending=False)
html_p5m5_close = p5m5_close_df.to_html(index=False, classes="table table-striped")

# Consultations with status 'O' whose end_date is in the past (late closing).
late_close_df = subset_df[(subset_df['status'] == 'O') & (subset_df['end_date'].dt.date < today)]
late_close_df = late_close_df.sort_values(by='end_date', ascending=False)
html_late_close = late_close_df.to_html(index=False, classes="table table-striped")

# Consultations with status 'C' that have an end_date in the future (early closing).
early_close_df = subset_df[(subset_df['status'] == 'C') & (subset_df['end_date'].dt.date > today)]
early_close_df = early_close_df.sort_values(by='end_date', ascending=False)
html_early_close = early_close_df.to_html(index=False, classes="table table-striped")

# Consultations with status 'P' that have a start_date in the past (late starting).
late_start_df = subset_df[(subset_df['status'] == 'P') & (subset_df['start_date'].dt.date < today)]
late_start_df = late_start_df.sort_values(by='start_date', ascending=False)
html_late_start = late_start_df.to_html(index=False, classes="table table-striped")

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
      /* Additional styling for our report */
      body {{ padding: 20px; font-family: Arial, sans-serif; }}
      table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; }}
      th, td {{ padding: 8px; border: 1px solid #ddd; text-align: left; }}
      th {{ background-color: #f2f2f2; }}
      h2 {{ margin-top: 40px; }}
      header, footer {{ text-align: center; margin-bottom: 40px; }}
    </style>
</head>
<body>
    <header>
        <h1>Consultations Tracker Report</h1>
        <p>Report generated on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
    </header>

    <section>
        <h2>🆕🔜Consultations Starting Between {m5} and {p5}</h2>
        {html_p5m5_start}
    </section>

    <section>
        <h2>⌛🔚Consultations Ending Between {m5} and {p5}</h2>
        {html_p5m5_close}
    </section>

    <section>
        <h2>😴Late Closing Consultations (Status 'O')</h2>
        {html_late_close}
    </section>

    <section>
        <h2>🏎️Early Closing Consultations (Status 'C')</h2>
        {html_early_close}
    </section>

    <section>
        <h2>🐌Late Starting Consultations (Status 'P')</h2>
        {html_late_start}
    </section>

    <footer>
        <p>Consultations Tracker Report</p>
        <div class="license" data-license-id="OGL-Canada-2.0">
    <div class="">
      <h3>
        <a href="https://licenses.opendefinition.org/licenses/OGL-Canada-2.0.json" class="tmpl-title">Open Government License 2.0 (Canada)</a>
        <div class="icons"><a href="https://opendefinition.org/od/" title="Open Data" class="open-icon"><img src="https://assets.okfn.org/images/ok_buttons/od_80x15_blue.png" alt="Open Data"></a><a href="https://opendefinition.org/od/" title="Open Data" class="open-icon"><img src="https://assets.okfn.org/images/ok_buttons/oc_80x15_red_green.png" alt="Open Data"></a></div>
      </h3>
      <p><a href="https://open.canada.ca/en/open-government-licence-canada" class="tmpl-url">https://open.canada.ca/en/open-government-licence-canada</a></p>
      
    </div>
  </div>
    </footer>
</body>
</html>
"""

# Write the final HTML into a file.
with open("report.html", "w", encoding="utf-8") as f:
    f.write(html_template)
