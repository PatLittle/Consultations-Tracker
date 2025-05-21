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
      /* Additional styling for our report */
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
      <a class="active" href="https://patlittle.github.io/Consultations-Tracker/report.htm">Consultations Tracker Report</a>
      <a href="https://patlittle.github.io/Consultations-Tracker">Web Monitoring</a>
      <a href="https://open.canada.ca/data/en/dataset/7c03f039-3753-4093-af60-74b0f7b2385d">Open Data</a>
      <a href="https://www.canada.ca/en/government/system/consultations/consultingcanadians.html">Consulting with Canadians</a>
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
