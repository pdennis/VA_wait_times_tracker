import argparse
import csv
from pathlib import Path

from bs4 import BeautifulSoup

"""
For some reason, the VA file uses several non-ASCII characters, especially chr 160, which is a non-breaking space.
This is a problem for databases, so we need to translate these characters to something else.
"""
XLATE_MAP = {
    f" {chr(160)}": ",",
    chr(160): ", ",
    chr(8211): ", ",
    chr(8217): "'",
}


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
                col_no = 0
                for col in row.find_all("td"):
                    text = col.text.strip()
                    if col_no == 3:
                        text = self.extract_address(col)
                    else:
                        text = col.text.strip()
                    cols.append(text)
                    if col_no == 1:
                        href = col.find(href=True)
                        if href:
                            href = href['href'].lower().replace("http://", "").replace("https://", "")
                            cols.append(href)
                        else:
                            cols.append("")
                        col_no += 1  # account for link column
                    col_no += 1
                writer.writerow(cols)

    @staticmethod
    def strip_ascii(text: str) -> str:
        """
        Remove non-ASCII characters from a string.
        Although the method is no longer called, it is kept for debugging and in
        the event the VA adds more special characters going forward.
        """
        for char in text:
            if ord(char) < 31 or ord(char) > 127:
                print(f"Removing non-ASCII character: '{char}' {ord(char)} {hex(ord(char))}")
        return "".join(char for char in text if 31 < ord(char) < 127).replace("  ", " ")

    @staticmethod
    def extract_address(col) -> str:
        address = ""
        for span in col.find_all("span"):
            text = span.text.strip()
            for k, v in XLATE_MAP.items():
                if k in text:
                    text = text.replace(k, v)
            text = text.strip()
            if text:
                address += ", " if address else ""
                address += text
        return address


if __name__ == "__main__":
    parser = argparse.ArgumentParser('Extract VA Facility Locations from HTML File')
    parser.add_argument('html_file', metavar='html_file', help='HTML File to be parsed')
    parser.add_argument('-csv_file', help='CSV File to be created')

    args = parser.parse_args()

    ExtractLocations(html_file=args.html_file, csv_file=args.csv_file)
