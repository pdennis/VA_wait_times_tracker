from argparse import ArgumentParser

from src.legacy.legacy_report_loader import LegacyReportLoader

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "-path",
        type=str,
        default=".",
        help="Path to legacy reports (default: current directory)",
    )
    parser.add_argument(
        "-prefix",
        type=str,
        default="va_facility_data",
        help="Prefix for legacy report files (default: va_facility_data)",
    )

    args = parser.parse_args()
    LegacyReportLoader(report_path=args.path, prefix=args.prefix)
