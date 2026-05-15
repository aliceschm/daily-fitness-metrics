import pytest

from scripts.run_daily_pipeline import parse_args


def test_cli_accepts_metric_date():
    args = parse_args(["--metric-date", "2026-05-13"])

    assert args.metric_date == "2026-05-13"
    assert args.start_date is None
    assert args.end_date is None


def test_cli_accepts_start_date_and_end_date():
    args = parse_args(["--start-date", "2026-05-13", "--end-date", "2026-05-14"])

    assert args.metric_date is None
    assert args.start_date == "2026-05-13"
    assert args.end_date == "2026-05-14"


def test_cli_rejects_missing_date_arguments():
    with pytest.raises(SystemExit):
        parse_args([])


@pytest.mark.parametrize(
    "args",
    [
        ["--metric-date", "2026-05-13", "--start-date", "2026-05-13"],
        ["--metric-date", "2026-05-13", "--end-date", "2026-05-13"],
        [
            "--metric-date",
            "2026-05-13",
            "--start-date",
            "2026-05-13",
            "--end-date",
            "2026-05-14",
        ],
    ],
)
def test_cli_rejects_metric_date_with_range_arguments(args):
    with pytest.raises(SystemExit):
        parse_args(args)
