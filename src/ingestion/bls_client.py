from __future__ import annotations

from html.parser import HTMLParser
from pathlib import PurePosixPath
from urllib.parse import unquote, urljoin, urlparse, urlunsplit

from src.ingestion.config import BLS_DIRECTORY_URL


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        if tag.lower() != "a":
            return
        self.hrefs.extend(
            value
            for name, value in attrs
            if name.lower() == "href" and value
        )


def discover_file_urls(
    html: str, base_url: str = BLS_DIRECTORY_URL
) -> list[str]:
    parser = _AnchorParser()
    parser.feed(html)

    base = urlparse(base_url)
    base_path = base.path.rstrip("/") + "/"
    file_urls: set[str] = set()
    for href in parser.hrefs:
        candidate = urljoin(base_url, href)
        parsed = urlparse(candidate)
        if parsed.scheme not in {"http", "https"} or parsed.netloc != base.netloc:
            continue
        if not parsed.path.startswith(base_path) or parsed.path.endswith("/"):
            continue
        filename = unquote(PurePosixPath(parsed.path).name)
        if not filename or filename in {".", ".."} or "\\" in filename:
            continue
        file_urls.add(urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", "")))

    return sorted(file_urls)
