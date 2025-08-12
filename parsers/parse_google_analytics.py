from bs4 import BeautifulSoup
import re
import csv
import os

def parse_and_save_csv(html_file, csv_file):
    with open(html_file, 'r', encoding='utf-8') as f, \
         open(csv_file, 'w', newline='', encoding='utf-8') as out_csv:

        writer = csv.writer(out_csv)
        writer.writerow(["Header", "URL", "Datetime", "Product"])  # CSV header

        soup = BeautifulSoup(f, 'html.parser')

        # Iterate over each activity block
        for record in soup.select('.outer-cell'):
            header = ""
            url = ""
            datetime = ""
            product = ""

            # Header cell text
            header_tag = record.select_one('.header-cell p')
            if header_tag:
                header = header_tag.get_text(strip=True)

            # First content-cell: URL + datetime
            content_cells = record.select('.content-cell')
            if content_cells:
                main_content = content_cells[0]

                # Extract link (clean from Google's redirect)
                link_tag = main_content.find('a', href=True)
                if link_tag:
                    match = re.search(r'q=(http.*?)&', link_tag['href'])
                    url = match.group(1) if match else link_tag['href']

                # Date/time extraction
                text_parts = main_content.get_text(separator=' ', strip=True).split()
                datetime = " ".join(text_parts[-4:]) if len(text_parts) >= 4 else ""

            # Product info
            for cell in content_cells:
                if 'Produits' in cell.get_text():
                    product = cell.get_text(separator=' ', strip=True)
                    break

            # Write row directly to CSV
            writer.writerow([header, url, datetime, product])

if __name__ == "__main__":
    html_file = "data/raw_data/Mon activité/Google Analytics/MonActivité.html"
    csv_file = "google_analytics_data.csv"
    print(f"Current working directory: {os.getcwd()}")
    parse_and_save_csv(html_file, csv_file)
    print(f"Data saved to {csv_file}")
