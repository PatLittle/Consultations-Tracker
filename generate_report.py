import hashlib
import re
from html.parser import HTMLParser
from urllib.parse import urljoin
from urllib.error import URLError
from urllib.request import Request, urlopen

import pandas as pd
from datetime import datetime, timedelta

# URL to the CSV file from the Government Open Data portal.
csv_url = 'https://open.canada.ca/data/en/datastore/dump/92bec4b7-6feb-4215-a5f7-61da342b2354'  # Replace with the actual URL if necessary
gazette_consultations_en_url = 'https://gazette.gc.ca/consult/consult-eng.html#a4'
gazette_consultations_fr_url = 'https://gazette.gc.ca/consult/consult-fra.html#a4'
gazette_base_url = 'https://gazette.gc.ca'

ENGLISH_MONTHS = {
    'january': 1,
    'february': 2,
    'march': 3,
    'april': 4,
    'may': 5,
    'june': 6,
    'july': 7,
    'august': 8,
    'september': 9,
    'october': 10,
    'november': 11,
    'december': 12,
}
FRENCH_MONTHS = {
    'janvier': 1,
    'février': 2,
    'fevrier': 2,
    'mars': 3,
    'avril': 4,
    'mai': 5,
    'juin': 6,
    'juillet': 7,
    'août': 8,
    'aout': 8,
    'septembre': 9,
    'octobre': 10,
    'novembre': 11,
    'décembre': 12,
    'decembre': 12,
}


class GazetteConsultationParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.in_open_section = False
        self.in_entry = False
        self.in_link = False
        self.in_list_item = False
        self.current_entry = None
        self.current_text = []
        self.entries = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'h2' and attrs.get('id') == 'a4':
            self.in_open_section = True
            return

        if not self.in_open_section:
            return

        if tag == 'h2':
            self.in_open_section = False
            return

        if tag == 'div':
            self.in_entry = True
            self.current_entry = {'title': '', 'link': '', 'items': []}
            return

        if not self.in_entry:
            return

        if tag == 'a':
            self.in_link = True
            self.current_text = []
            self.current_entry['link'] = attrs.get('href', '')
        elif tag == 'li':
            self.in_list_item = True
            self.current_text = []

    def handle_endtag(self, tag):
        if tag == 'h2' and self.in_open_section and not self.in_entry:
            return

        if not self.in_open_section or not self.in_entry:
            return

        if tag == 'a' and self.in_link:
            self.current_entry['title'] = normalize_space(''.join(self.current_text))
            self.in_link = False
            self.current_text = []
        elif tag == 'li' and self.in_list_item:
            self.current_entry['items'].append(normalize_space(''.join(self.current_text)))
            self.in_list_item = False
            self.current_text = []
        elif tag == 'div':
            if self.current_entry and self.current_entry.get('title'):
                self.entries.append(self.current_entry)
            self.current_entry = None
            self.in_entry = False

    def handle_data(self, data):
        if self.in_link or self.in_list_item:
            self.current_text.append(data)


def normalize_space(value):
    return re.sub(r'\s+', ' ', value).strip()


def parse_text_date(value, language):
    months = ENGLISH_MONTHS if language == 'en' else FRENCH_MONTHS
    normalized_value = normalize_space(value).lower()
    for month_name, month_number in months.items():
        if language == 'en':
            match = re.search(rf'{month_name}\s+(\d{{1,2}}),?\s+(\d{{4}})', normalized_value)
        else:
            match = re.search(rf'(\d{{1,2}})\s+{month_name}\s+(\d{{4}})', normalized_value)
        if match:
            if language == 'en':
                day = int(match.group(1))
                year = int(match.group(2))
            else:
                day = int(match.group(1))
                year = int(match.group(2))
            return f'{year:04d}-{month_number:02d}-{day:02d}'
    return ''


def fetch_gazette_consultations(url, language):
    request = Request(url, headers={'User-Agent': 'Consultations-Tracker/1.0'})
    with urlopen(request, timeout=30) as response:
        html = response.read().decode('utf-8')

    parser = GazetteConsultationParser()
    parser.feed(html)

    consultations = []
    for entry in parser.entries:
        published_text = next(
            (item for item in entry['items'] if 'published' in item.lower() or 'publié' in item.lower()),
            '',
        )
        close_text = next(
            (item for item in entry['items'] if 'until' in item.lower() or 'jusqu' in item.lower()),
            '',
        )
        consultations.append(
            {
                f'title_{language}': entry['title'],
                f'link_{language}': urljoin(gazette_base_url, entry['link']),
                'date_published': parse_text_date(published_text, language),
                'date_close': parse_text_date(close_text, language),
            }
        )

    return consultations


def collect_gazette_consultations():
    english_consultations = fetch_gazette_consultations(gazette_consultations_en_url, 'en')
    french_consultations = fetch_gazette_consultations(gazette_consultations_fr_url, 'fr')
    consultation_rows = []

    for english_entry in english_consultations:
        french_link = english_entry['link_en'].replace('-eng.html', '-fra.html')
        french_entry = next(
            (
                entry
                for entry in french_consultations
                if entry['link_fr'] == french_link
            ),
            {},
        )
        consultation_rows.append(
            {
                'date_published': english_entry['date_published'] or french_entry.get('date_published', ''),
                'date_close': english_entry['date_close'] or french_entry.get('date_close', ''),
                'title_en': english_entry['title_en'],
                'title_fr': french_entry.get('title_fr', ''),
                'link_en': english_entry['link_en'],
                'link_fr': french_entry.get('link_fr', french_link),
            }
        )

    return pd.DataFrame(
        consultation_rows,
        columns=['date_published', 'date_close', 'title_en', 'title_fr', 'link_en', 'link_fr'],
    )

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

# 6. Open Canada Gazette consultations.
try:
    gazette_consultations_df = collect_gazette_consultations()
except URLError:
    try:
        gazette_consultations_df = pd.read_csv('gazette_consultations.csv')
    except FileNotFoundError:
        gazette_consultations_df = pd.DataFrame(
            columns=['date_published', 'date_close', 'title_en', 'title_fr', 'link_en', 'link_fr'],
        )

gazette_consultations_df.to_csv("gazette_consultations.csv", index=False)
html_gazette_consultations = gazette_consultations_df.to_html(
    index=False,
    classes="data-table",
    border=0,
    render_links=True,
)

# Create the final HTML page by injecting the tables into a template.
generated_datetime = datetime.now()
generated_datetime_str = generated_datetime.strftime("%Y-%m-%d %H:%M:%S")
generated_date_str = generated_datetime.strftime("%Y-%m-%d")
range_start_str = m5.strftime("%Y-%m-%d")
range_end_str = p5.strftime("%Y-%m-%d")

iframe_fullscreen_styles = """
      .viewer-controls {
        margin-block-start: 1rem;
        display: flex;
        gap: 0.75rem;
        flex-wrap: wrap;
      }

      .viewer-overlay {
        position: fixed;
        inset: 0;
        background: #ffffff;
        z-index: 9999;
        padding: 1rem;
        display: none;
      }

      .viewer-overlay.active {
        display: block;
      }

      .viewer-overlay iframe {
        width: 100%;
        height: calc(100vh - 4rem);
        border: none;
      }
"""

iframe_fullscreen_script = """
    <script>
      document.querySelectorAll("[data-open-fullscreen]").forEach((openBtn) => {
        const overlayId = openBtn.getAttribute("data-open-fullscreen");
        const overlay = document.getElementById(overlayId);
        const closeBtn = overlay?.querySelector("[data-close-fullscreen]");

        openBtn.addEventListener("click", () => {
          overlay?.classList.add("active");
          overlay?.setAttribute("aria-hidden", "false");
        });

        closeBtn?.addEventListener("click", () => {
          overlay.classList.remove("active");
          overlay.setAttribute("aria-hidden", "true");
        });
      });
    </script>
"""


def flatgithub_viewer_section(src, title, overlay_id):
    return f"""          <section class="iframe-wrapper">
            <iframe
              src="{src}"
              title="{title}"
            ></iframe>
            <div class="viewer-controls">
              <gcds-button data-open-fullscreen="{overlay_id}">Open full screen table view</gcds-button>
            </div>
          </section>
          <div id="{overlay_id}" class="viewer-overlay" aria-hidden="true">
            <gcds-button data-close-fullscreen button-role="secondary">Return to standard view</gcds-button>
            <iframe src="{src}" title="{title} full screen"></iframe>
          </div>"""

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
            <gcds-nav-link href="https://patlittle.github.io/Consultations-Tracker/url_errors.html">
              URL Errors Report
            </gcds-nav-link>
            <gcds-nav-link href="https://patlittle.github.io/Consultations-Tracker/changelog.html">
              Change Log Report
            </gcds-nav-link>
            <gcds-nav-link href="https://patlittle.github.io/Consultations-Tracker/consultations_dataset.html">
              Consultations Data View
            </gcds-nav-link>
            <gcds-nav-link href="https://open.canada.ca/data/en/dataset/7c03f039-3753-4093-af60-74b0f7b2385d">
              Consultations Open Dataset
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
              <li>
                <gcds-link href="#gazette-consultations">
                  Open Canada Gazette Consultations
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
          <section id="gazette-consultations">
            <gcds-heading tag="h2">
              Open Canada Gazette Consultations
            </gcds-heading>
            <div class="table-wrapper">
              {html_gazette_consultations}
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

# Create the change log HTML page by injecting the tables into a template.
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
{iframe_fullscreen_styles}
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
            <gcds-nav-link href="https://patlittle.github.io/Consultations-Tracker/url_errors.html">
              URL Errors Report
            </gcds-nav-link>
            <gcds-nav-link
              href="https://patlittle.github.io/Consultations-Tracker/changelog.html"
              current
            >
              Change Log Report
            </gcds-nav-link>
            <gcds-nav-link href="https://patlittle.github.io/Consultations-Tracker/consultations_dataset.html">
              Consultations Data View
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
           
            <gcds-notice type="success" notice-title-tag="h2" notice-title="Report Generated">
              <gcds-text>{generated_datetime_str}</gcds-text>
            </gcds-notice>
            
         
          </section>
{flatgithub_viewer_section(
            "https://flatgithub.com/PatLittle/Consultations-Tracker/blob/master/consultations_chng_log.csv?filename=consultations_chng_log.csv&sort=row_chng_datetime%2Cdesc&stickyColumnName=row_chng_datetime",
            "Consultations Tracker change log table",
            "change-log-viewer-overlay",
          )}
          <gcds-date-modified>{generated_date_str}</gcds-date-modified>
        </div>
      </div>
    </gcds-container>
    <gcds-footer display="simple"></gcds-footer>
{iframe_fullscreen_script}
  </body>
</html>
"""

# Create the URL errors HTML page with the same layout as the change log.
url_errors_template = f"""<!DOCTYPE html>
<html dir="ltr" lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <meta
      name="description"
      content="Report highlighting consultation URL errors sourced from the Consultations Tracker."
    />
    <title>Consultations URL Errors Report</title>
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
{iframe_fullscreen_styles}
    </style>
  </head>
  <body>
    <gcds-header
      lang-href="https://patlittle.github.io/Consultations-Tracker/url_errors.html"
      skip-to-href="#main-content"
    >
      <gcds-breadcrumbs slot="breadcrumb">
        <gcds-breadcrumbs-item href="https://patlittle.github.io/Consultations-Tracker/">
          Consultations Tracker
        </gcds-breadcrumbs-item>
        <gcds-breadcrumbs-item href="https://patlittle.github.io/Consultations-Tracker/url_errors.html">
          URL Errors Report
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
            <gcds-nav-link href="https://patlittle.github.io/Consultations-Tracker/url_errors.html" current>
              URL Errors Report
            </gcds-nav-link>
            <gcds-nav-link href="https://patlittle.github.io/Consultations-Tracker/changelog.html">
              Change Log Report
            </gcds-nav-link>
            <gcds-nav-link href="https://patlittle.github.io/Consultations-Tracker/consultations_dataset.html">
              Consultations Data View
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
            <gcds-heading tag="h1">Consultations URL Errors Report</gcds-heading>

            <gcds-notice type="success" notice-title-tag="h2" notice-title="Report Generated">
              <gcds-text>{generated_datetime_str}</gcds-text>
            </gcds-notice>


          </section>
{flatgithub_viewer_section(
            "https://flatgithub.com/PatLittle/Consultations-Tracker/blob/master/bad-urls.csv?filename=bad-urls.csv",
            "Consultations Tracker URL errors table",
            "url-errors-viewer-overlay",
          )}
          <gcds-date-modified>{generated_date_str}</gcds-date-modified>
        </div>
      </div>
    </gcds-container>
    <gcds-footer display="simple"></gcds-footer>
{iframe_fullscreen_script}
  </body>
</html>
"""

# Write the final HTML files.
with open("changelog.html", "w", encoding="utf-8") as f:
    f.write(chng_log_template)

with open("url_errors.html", "w", encoding="utf-8") as f:
    f.write(url_errors_template)
