import argparse
import csv
from pathlib import Path

from bs4 import BeautifulSoup


class ExtractLocations:
    """
    Given an HTML file containing the names and locations of VA facilities throughout the United States,
    extract the facility codes, names, locations, and other information and save it to a CSV file. The HTML
    file is created as follows:

        - navigate to https://www.va.gov/directory/guide/rpt_fac_list.cfm
        - select "All VA Facilities" from the choice list
        - hit the "GO" button
        - in your browser, save the results as a WebArchive file
        - use the "textutil" tool to extract the HTML from the WebArchive file:
            textutil -convert html <name of webarchive file>

    With the extracted HTML file, you can run this script.
    """

    def __init__(self, html_file: str, csv_file: str = None) -> None:
        # parse the HTML File using BeautifulSoup
        with open(html_file) as fp:
            soup = BeautifulSoup(fp, features="lxml")

        # find all "row" elements in the HTML
        rows = soup.find_all("tr")

        # make sure we found some rows
        assert rows

        # first row contains sort information; discard it
        if rows[0].text.strip().lower().startswith("list by:"):
            rows = rows[1:]

        # second row should be header; verify content
        assert rows[0].text.strip().lower().startswith("station id")

        # now start CSV Export
        if csv_file:
            csv_file = Path(csv_file)
        else:
            csv_file = Path(html_file)
        csv_file = csv_file.with_suffix(".csv")

        # open CSV File for output; this will overwrite any existing file content
        with open(csv_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)

            # write header
            header=[]
            for col in rows[0].find_all("td"):
                col_name = col.text.strip()
                header.append(col_name)
                if col_name == "Facility":
                    header.append("Link")
            writer.writerow(header)
            # now write data
            for row in rows[1:]:
                cols = []
                for col in row.find_all("td"):
                    cols.append(col.text.strip())
                    if col.find(href=True):
                        cols.append(col.find(href=True)['href'])
                writer.writerow(cols)

if __name__ == "__main__":
    parser = argparse.ArgumentParser('Extract VA Facility Locations from HTML File')
    parser.add_argument('html_file', metavar='html_file', help='HTML File to be parsed')
    parser.add_argument('-csv_file', help='CSV File to be created')

    args = parser.parse_args()

    ExtractLocations(html_file=args.html_file, csv_file=args.csv_file)
