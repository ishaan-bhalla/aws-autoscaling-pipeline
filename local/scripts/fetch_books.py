"""Download 12 public-domain books from Project Gutenberg and split each into
10 parts: 120 text files, the same number as the AWS load test.

The AWS run used text files supplied with the course; they are not
redistributable, so the local demo uses Gutenberg instead."""
import time
import urllib.request
from pathlib import Path

BOOKS = {1342: "pride-and-prejudice", 84: "frankenstein", 11: "alice", 1661: "sherlock-holmes",
         2701: "moby-dick", 98: "tale-of-two-cities", 74: "tom-sawyer", 345: "dracula",
         174: "dorian-gray", 76: "huckleberry-finn", 1260: "jane-eyre", 768: "wuthering-heights"}
PARTS = 10
OUT = Path(__file__).resolve().parents[1] / "data"
HEADERS = {"User-Agent": "aws-autoscaling-pipeline demo (one-off download of 12 books)"}


def main() -> None:
    OUT.mkdir(exist_ok=True)
    for book_id, name in BOOKS.items():
        if len(list(OUT.glob(f"{name}-*.txt"))) == PARTS:
            continue
        url = f"https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.txt"
        req = urllib.request.Request(url, headers=HEADERS)
        text = urllib.request.urlopen(req, timeout=60).read().decode("utf-8", "ignore")
        lines = text.splitlines()
        size = len(lines) // PARTS + 1
        for i in range(PARTS):
            (OUT / f"{name}-{i + 1:02d}.txt").write_text("\n".join(lines[i * size:(i + 1) * size]))
        print(f"{name}: {len(text) / 1e6:.1f} MB -> {PARTS} parts")
        time.sleep(2)  # be polite to gutenberg.org
    print(f"{len(list(OUT.glob('*.txt')))} files in {OUT}")


if __name__ == "__main__":
    main()
