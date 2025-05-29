#!/usr/bin/env python3

from argparse import ArgumentParser

from src.db.download_reports import DownloadReports

if __name__ == "__main__":
    parser = ArgumentParser()

    parser.add_argument(
        "-all",
        action="store_true",
        help="Download reports from all stations, not just Germane",
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
    parser.add_argument(
        "-update_stats",
        action="store_true",
        help="Update all stats for all reports, all stations",
    )

    args = parser.parse_args()
    DownloadReports(
        station_id=args.station_id,
        pause=args.pause,
        only_germane=not args.all,
        update_all_stats=args.update_stats,
    ).join()
