from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class DailySignupMetric:
    metric_date: date
    signup_count: int


@dataclass(frozen=True)
class DailyCheckinMetric:
    metric_date: date
    checkin_count: int


@dataclass(frozen=True)
class DailyActiveClientsMetric:
    metric_date: date
    active_clients: int


@dataclass(frozen=True)
class DailyMetric:
    metric_date: date
    signup_count: int
    checkin_count: int
    active_clients: int
    usage_rate: float