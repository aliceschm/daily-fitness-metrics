from pathlib import Path


CLIENT_COUNT = 13_007


def seed_clients() -> None:
    output_path = Path("sources/clients/clients.csv")

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    locations = ["SP", "RJ", "MG", "PR", "SC"]

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        file.write(
            "client_id,status,location_id,created_at\n"
        )

        for i in range(CLIENT_COUNT):
            file.write(
                (
                    f"client_{i},"
                    f"active,"
                    f"{locations[i % len(locations)]},"
                    f"2026-05-01\n"
                )
            )

    print(f"Seeded clients: {output_path}")


if __name__ == "__main__":
    seed_clients()