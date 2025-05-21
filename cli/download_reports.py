from argparse import ArgumentParser

from src.db.download_reports import DownloadReports

if __name__ == "__main__":
    parser = ArgumentParser()

    parser.add_argument(
        "-germane",
        action="store_true",
        help="Only download reports that are in the Germane list",
    )
    parser.add_argument(
        "-pause",
        type=float,
        default=2.0,
        help="Pause time between downloads (default: 2 seconds)",
    )
    parser.add_argument(
        "-station_id",
        type=str,
        help="Only download reports for specified Station",
    )

    args = parser.parse_args()
    DownloadReports(
        station_id=args.station_id,
        pause=args.pause,
        only_germane=args.germane,
    )
