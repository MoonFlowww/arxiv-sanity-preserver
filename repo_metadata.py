import re
from urllib.parse import urlparse, urlunparse

_URL_PATTERN = re.compile(
    r"(https?://[^\s<>\]\[\)\(\"']+|www\.github\.com/[^\s<>\]\[\)\(\"']+|github\.com/[^\s<>\]\[\)\(\"']+)",
    re.IGNORECASE,
)


def _normalize_url(url):
    if not isinstance(url, str):
        return None

    cleaned = url.strip().rstrip('.,);\'"')
    if not cleaned:
        return None

    if not re.match(r'^[a-zA-Z][a-zA-Z0-9+.-]*://', cleaned):
        cleaned = 'https://' + cleaned

    parsed = urlparse(cleaned)
    if not parsed.netloc:
        return None

    normalized = parsed._replace(netloc=parsed.netloc.lower())
    return urlunparse(normalized)


def _is_repo_host(url):
    parsed = urlparse(url)
    hostname = parsed.hostname or parsed.netloc
    if not hostname:
        return False

    hostname = hostname.lower()
    return hostname == 'github.com' or hostname.endswith('.github.com')


def _extract_urls(value):
    urls = []
    if isinstance(value, str):
        matches = _URL_PATTERN.findall(value)
        for match in matches:
            normalized = _normalize_url(match)
            if normalized:
                urls.append(normalized)
    elif isinstance(value, dict):
        for nested_value in value.values():
            urls.extend(_extract_urls(nested_value))
    elif isinstance(value, (list, tuple, set)):
        for nested_value in value:
            urls.extend(_extract_urls(nested_value))

    return urls


def _extract_repo_links(paper):
    repo_links = set()
    if not isinstance(paper, dict):
        return []

    existing_links = paper.get('repo_links')
    if isinstance(existing_links, (list, tuple, set)):
        for link in existing_links:
            normalized = _normalize_url(link) if isinstance(link, str) else None
            if normalized and _is_repo_host(normalized):
                repo_links.add(normalized)

    repo_fields = [
        'github_url',
        'github_link',
        'github',
        'code_url',
        'code_repository_url',
        'repository_url',
        'project_url',
        'project_page',
        'link',
        'links',
    ]

    for field in repo_fields:
        for url in _extract_urls(paper.get(field)):
            if _is_repo_host(url):
                repo_links.add(url)

    text_fields = [
        'comment',
        'comments',
        'arxiv_comment',
        'summary',
    ]

    for field in text_fields:
        for url in _extract_urls(paper.get(field)):
            if _is_repo_host(url):
                repo_links.add(url)

    for value in paper.values():
        if isinstance(value, str) and 'github' in value.lower():
            for url in _extract_urls(value):
                if _is_repo_host(url):
                    repo_links.add(url)

    return sorted(repo_links)


def _compute_repo_metadata(paper):
    repo_links = _extract_repo_links(paper)
    is_opensource = bool(repo_links) or bool(
        paper.get('has_github')
        or paper.get('is_open_source')
        or paper.get('is_opensource')
    )
    return {
        'repo_links': repo_links,
        'is_opensource': is_opensource,
    }


def build_repo_metadata(paper):
    return _compute_repo_metadata(paper)
