import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from db import connect, ensure_seed_data, init_db


def main() -> None:
    init_db()
    con = connect()
    try:
        ensure_seed_data(con)
    finally:
        con.close()
    print("DB initialized.")


if __name__ == "__main__":
    main()
