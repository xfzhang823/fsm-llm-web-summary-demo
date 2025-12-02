"""
pipelines/webpage_reader_async.py

Description:
------------
Utility functions for Stage A of the *web summary demo* pipeline:
asynchronously scraping webpages, cleaning the extracted text, and returning
structured `WebPageContent` objects grouped inside a `WebPageBatch`.

This module performs **no LLM operations**.

Pipeline alignment:
-------------------
Stage A  (this module)
    - Playwright fetch
    - Extract visible text
    - Clean/normalize text
    - Wrap into WebPageContent
    - Return WebPageBatch

Stage B  (separate pipeline: web_summarize_pipeline_async)
    - Load WebPageRow from DB
    - Build WebPageContent
    - Create summary prompts
    - LLM summarization
    - Persist WebSummaryRow
    - FSM progresses to WEB_SUMMARY

This separation matches the job_bot architecture and ensures a clean,
single-responsibility Stage A.
"""

import re
import time
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Union

from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
)

# Project models
from models.web_content_models import WebPageContent, WebPageBatch

logger = logging.getLogger(__name__)


# ============================================================================
# Cleaning utilities
# ============================================================================


def clean_webpage_text(content: str) -> str:
    """
    Clean the extracted visible text from a webpage.

    Steps:
    - Remove JS fragments
    - Remove script blocks
    - Strip visible URLs
    - Collapse duplicate whitespace
    - Normalize newlines

    Args:
        content (str): Raw text extracted from Playwright.

    Returns:
        str: Cleaned, human-readable text.
    """
    # Remove JavaScript fragments (e.g., requireLazy([...]))
    content = re.sub(r"requireLazy\([^)]+\)", "", content)

    # Remove <script> tags completely
    content = re.sub(r"<script[^>]*>[\s\S]*?</script>", "", content)

    # Remove visible URLs
    content = re.sub(r"https?:\/\/\S+", "", content)

    # Collapse whitespace
    content = re.sub(r"\s+", " ", content).strip()

    # Normalize double newlines
    content = content.replace("\n\n", "\n")

    return content


# ============================================================================
# Cookie banner handling
# ============================================================================


async def handle_cookie_banner(page) -> None:
    """
    Best-effort cookie banner handling. It will not block if banners aren't present.

    Args:
        page: Playwright page object.
    """
    domain = None
    try:
        domain = page.url.split("/")[2]  # e.g., example.com

        accept_button = await page.query_selector("button:has-text('Accept')")
        reject_button = await page.query_selector("button:has-text('Reject')")
        close_button = await page.query_selector("button:has-text('×')")

        async def safe_click(btn, label: str):
            if not btn:
                return
            try:
                await btn.click(timeout=2000)
                logger.info(f"{label}ed cookie banner on {domain}")
            except Exception as e:
                logger.warning(f"Failed to click {label} on {domain}: {e}")

        # Prefer Reject → Accept → Close
        if reject_button:
            await safe_click(reject_button, "Reject")
        elif accept_button:
            await safe_click(accept_button, "Accept")
        elif close_button:
            await safe_click(close_button, "Closed")
        else:
            logger.info(f"No cookie banner detected on {domain}")

        # Set a consent cookie to avoid future prompts
        await page.context.add_cookies(
            [
                {
                    "name": "cookie_consent",
                    "value": "accepted",
                    "domain": domain,
                    "path": "/",
                }
            ]
        )

    except Exception as e:
        logger.error(
            f"Error handling cookie banner on {domain or 'unknown'}: {e}",
            exc_info=True,
        )


# ============================================================================
# Core scraping functions
# ============================================================================


async def load_webpages_with_playwright_async(
    urls: List[str],
) -> Tuple[Dict[str, str], List[str]]:
    """
    Fetch and extract visible text from a list of webpages using Playwright.

    Returns:
        (content_dict, failed_urls)
        - content_dict: { url: cleaned_text }
        - failed_urls: [url1, url2, ...]
    """
    content_dict: Dict[str, str] = {}
    failed_urls: List[str] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for url in urls:
            try:
                logger.info(f"Loading: {url}")

                start = time.time()
                try:
                    await page.goto(url, timeout=8000)
                except Exception as e:
                    logger.error(f"Timeout when loading {url}: {e}")
                    failed_urls.append(url)
                    continue

                load_time = time.time() - start
                logger.info(f"Loaded {url} in {load_time:.2f}s")

                await handle_cookie_banner(page)

                # Try to ensure major DOM content is loaded
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=6000)
                except PlaywrightTimeoutError:
                    logger.warning(f"DOM load timeout for {url}; using partial content")

                raw_text = await page.evaluate("document.body.innerText")

                if raw_text and raw_text.strip():
                    cleaned = clean_webpage_text(raw_text)
                    content_dict[url] = cleaned
                else:
                    logger.error(f"No visible text extracted for {url}")
                    failed_urls.append(url)

            except Exception as e:
                logger.error(f"Unexpected error fetching {url}: {e}")
                failed_urls.append(url)

        await browser.close()

    return content_dict, failed_urls


async def read_webpages_async(urls: List[str]) -> Tuple[Dict[str, str], List[str]]:
    """
    High-level wrapper around Playwright loader.

    Returns:
        (documents, failed_urls)
    """
    documents, failed_urls = await load_webpages_with_playwright_async(urls)

    if failed_urls:
        logger.warning(f"Failed URLs: {failed_urls}")

    return documents, failed_urls


# ============================================================================
# Batch processing — Stage A final output
# ============================================================================


async def process_webpages_to_json_async(
    urls: Union[List[str], str],
) -> WebPageBatch:
    """
    Stage A: Scrape → Clean → Wrap in `WebPageContent` → Return `WebPageBatch`.

    This function:
        - Accepts one URL or a list of URLs
        - Fetches webpage text asynchronously
        - Cleans the text
        - Creates WebPageContent for each URL
        - Returns WebPageBatch (RootModel mapping URL → WebPageContent)

    No LLM calls occur here. Summarization is handled in Stage B.

    Args:
        urls (str | list[str]): Webpages to fetch.

    Returns:
        WebPageBatch: root={ url: WebPageContent, ... }
    """
    if isinstance(urls, str):
        urls = [urls]

    # 1) Scrape the webpages
    scraped_texts, failed_urls = await read_webpages_async(urls)

    # 2) Convert each into WebPageContent (minimal fields for Stage A)
    batch_root: Dict[str, WebPageContent] = {}

    for url, clean_text in scraped_texts.items():
        batch_root[url] = WebPageContent(
            url=url,
            clean_text=clean_text,
            title=None,
            headings=None,
            metadata=None,
            raw_html=None,
        )

    # Stage B summarization will handle the failed URLs (if needed)
    return WebPageBatch(root=batch_root)


# ============================================================================
# Optional: Save raw text for debugging
# ============================================================================


def save_webpage_content(
    content_dict: Dict[str, str],
    file_path: Union[Path, str],
    file_format: str = "json",
) -> None:
    """
    Save scraped content (raw or cleaned) to a text or JSON file.

    Args:
        content_dict: Mapping: url → cleaned text
        file_path: Destination path
        file_format: "json" | "txt"
    """
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            if file_format == "json":
                import json

                json.dump(content_dict, f, indent=4, ensure_ascii=False)
            else:
                for url, content in content_dict.items():
                    f.write(f"--- {url} ---\n{content}\n\n")

        logger.info(f"Saved content to {file_path}")

    except Exception as e:
        logger.error(f"Error saving content: {e}")
        raise
