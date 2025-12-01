"""
Filename: webpage_reader_async.py
Last updated: 2024 Dec

Description: Utilities functions to read and/or summarize webpage(s).

This module facilitates a two-step process:
1. Extract content from webpages using Playwright.
   - Fetches everything visible on the page, cleans the text, and identifies URLs
   that fail to load.

2. Transform the extracted content into structured JSON format using an LLM (OpenAI API).
   - Converts raw webpage content into JSON representations suitable for specific use cases,
   such as job postings.

Returns JobSiteResponse models (per URL), wrapped in a JobPostingsBatch for further ingestion.
"""

import re
from pathlib import Path
import logging
import time
import json
from typing import Any, Dict, List, Literal, Tuple, Union
from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
)

# User defined
from job_bot.llm_providers.llm_api_utils_async import (
    call_openai_api_async,
    call_anthropic_api_async,
)
from job_bot.prompts.prompt_templates import CONVERT_JOB_POSTING_TO_JSON_PROMPT
from job_bot.models.llm_response_models import JobSiteResponse
from job_bot.models.resume_job_description_io_models import JobPostingsBatch
from job_bot.config.project_config import (
    OPENAI,
    ANTHROPIC,
    GPT_4_1_NANO,
    GPT_35_TURBO,
    CLAUDE_HAIKU,
    CLAUDE_SONNET_3_5,
)


# Set up logging
logger = logging.getLogger(__name__)


def clean_webpage_text(content):
    """
    Clean the extracted text by removing JavaScript, URLs, scripts, and excessive whitespace.

    This function performs the following cleaning steps:
    - Removes JavaScript function calls.
    - Removes URLs (e.g., tracking or other unwanted URLs).
    - Removes script tags and their content.
    - Replaces multiple spaces or newline characters with a single space or newline.
    - Strips leading and trailing whitespace.

    Args:
        content (str): The text content to be cleaned.

    Returns:
        str: The cleaned and processed text.
    """
    # Remove JavaScript function calls (e.g., requireLazy([...]))
    content = re.sub(r"requireLazy\([^)]+\)", "", content)

    # Remove URLs (e.g., http, https)
    content = re.sub(r"https?:\/\/\S+", "", content)

    # Remove <script> tags and their contents
    content = re.sub(r"<script[^>]*>[\s\S]*?</script>", "", content)

    # Remove excessive whitespace (more than one space)
    content = re.sub(r"\s+", " ", content).strip()

    # Replace double newlines with single newlines
    content = content.replace("\n\n", "\n")

    return content


async def handle_cookie_banner(page):
    """Func to handle cookie acceptance banner dynamically."""
    domain = None
    try:
        url = page.url
        domain = url.split("/")[2]  # "example.com"

        # Try to find buttons quickly (no long blocking waits)
        accept_button = await page.query_selector("button:has-text('Accept')")
        reject_button = await page.query_selector("button:has-text('Reject')")
        close_button = await page.query_selector("button:has-text('×')")

        # Wrap clicks in short, best-effort tries
        async def safe_click(btn, label: str):
            if not btn:
                return
            try:
                # Short timeout so we don't hang for 30s
                await btn.click(timeout=2000)
                logger.info(f"{label}ed cookie banner on {domain}")
            except Exception as e:
                logger.warning(f"Failed to click {label} button on {domain}: {e}")

        if reject_button:
            await safe_click(reject_button, "Reject")
        elif accept_button:
            await safe_click(accept_button, "Accept")
        elif close_button:
            await safe_click(close_button, "Closed")
        else:
            logger.info(f"No cookie banner found on {domain}")

        # Best-effort cookie
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
        logger.info(f"Manually set cookie consent for {domain}")

    except Exception as e:
        logger.error(
            f"Error handling cookie banner on {domain or 'unknown'}: {e}", exc_info=True
        )


# Function to load webpage content using Playwright
async def load_webpages_with_playwright_async(
    urls: List[str],
) -> Tuple[Dict[str, str], List[str]]:
    """
    Fetch webpage content using Playwright.

    Args:
        urls (List[str]): List of job posting URLs to load and extract content from.

    Returns:
        Tuple[Dict[str, str], List[str]]:
            - A mapping from URL to cleaned page content (str).
            - A list of URLs that failed to load or extract.

    Notes:
        Cleans text using `clean_webpage_text`.
        Uses `handle_cookie_banner` to bypass consent popups.

    Raises:
        Logs errors per URL; continues processing remaining pages.
    """

    content_dict = {}
    failed_urls = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False
        )  # Set headless=False for debugging if needed
        page = await browser.new_page()

        for url in urls:
            try:
                logger.info(
                    f"Attempting to load content with Playwright for URL: {url}"
                )

                start_time = time.time()  # Start tracking time
                try:
                    await page.goto(url, timeout=5000)
                except Exception as e:
                    logger.error(
                        f"Timeout error for {url} after {time.time() - start_time:.2f} seconds: {e}"
                    )
                    failed_urls.append(url)
                    continue  # ✅ Move to the next page immediately

                logger.info(
                    f"Successfully loaded {url} in {time.time() - start_time:.2f} seconds"
                )

                await handle_cookie_banner(page)  # ✅ Accepts cookies

                # * Wait for network to be idle
                # * "networkidle" waits for all page resources to finish loading,
                # * while "domcontentloaded" waits only for the initial HTML document to be
                # * loaded and parsed.

                # await page.wait_for_load_state("networkidle")
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=10_000)
                except PlaywrightTimeoutError as e:
                    logger.warning(
                        f"wait_for_load_state('domcontentloaded') timed out for {url}: {e}. "
                        "Continuing with whatever content is available."
                    )

                # *page.text_content() vs page.evaluate (page.text_content is used in
                # * most tutorials)
                # *The page.text_content("body") extracts everything, including all the HTML
                # *and CSS stuff
                # *page.evaluate() directly access the text content via JavaScript.
                # *Here, evaluate is better, b/c it has a more "how readers see it" format,
                # *which is cleaner

                logger.info(f"Extracting content from {url}")  # debugging
                content = await page.evaluate("document.body.innerText")
                logger.debug(f"Extracted content: {content}")

                logger.debug(f"Raw content from Playwright for {url}: {content}\n")
                if content and content.strip():
                    clean_content = clean_webpage_text(content)
                    content_dict[url] = {
                        "url": url,
                        "content": clean_content,
                    }  # include url (needed for later parsing by LLM)

                    logger.info(
                        f"Playwright successfully processed content for {url}\n"
                    )

                else:
                    logger.error(f"No content extracted for {url}, skipping.")
                    failed_urls.append(url)  # ✅ Marks failed URL but continues

            except Exception as e:
                logger.error(f"Error occurred while fetching content for {url}: {e}")
                logger.debug(f"Failed URL: {url}")
                failed_urls.append(
                    url
                )  # ✅ Mark the URL as failed (failed_url triggers it to "move on")

        await browser.close()

    if failed_urls:
        logger.warning(f"Failed URLs: {failed_urls}")

    return content_dict, failed_urls  # Return both the content and the failed URLs


async def read_webpages_async(urls: List[str]) -> Tuple[Dict[str, str], List[str]]:
    """
    Extract and clean text content from one or multiple webpages using Playwright.

    Args:
        urls (List[str]): List of URLs to load and process.

    Returns:
        Tuple[Dict[str, str], List[str]]:
            A tuple containing:
            - A dictionary where keys are URLs (str) and values are cleaned webpage content (str).
            - A list of URLs (str) that failed to load.

    Logs:
        Warnings for any URLs that failed to load.
    """
    documents, failed_urls = await load_webpages_with_playwright_async(urls)

    if failed_urls:
        logger.warning(f"Failed to load the following URLs: {failed_urls}\n")

    return documents, failed_urls


# Todo: refactor into convert_to_json_with_llm_async later on;
# todo: now live w/t ...with_gpt and ...with_claude
async def convert_to_json_with_gpt_async(
    input_text: str,
    model_id: str = GPT_35_TURBO,  # use cheapest - easy task
    max_tokens: int = 2048,
    temperature: float = 0.3,
) -> JobSiteResponse:
    """
    Parse and convert job posting content to JSON format using OpenAI API asynchronously.

    Args:
        - input_text (str): The cleaned text to convert to JSON.
        - model_id (str, optional): The model ID to use for OpenAI (default is "gpt-4-turbo").
        - temperature (float, optional): temperature for the model (default is 0.3).

    Returns:
        JobSiteResponse: A pydantic model representation of the extracted JSON content.

    Raises:
        ValueError: If the input text is empty or the response is not in the expected format.

    Logs:
        Information about the prompt and raw response.
        Errors if the response does not match the expected model format.
    """
    if not input_text:
        logger.error("Input text is empty or invalid.")
        raise ValueError("Input text cannot be empty.")

    # Set up the prompt
    prompt = CONVERT_JOB_POSTING_TO_JSON_PROMPT.format(content=input_text)
    logger.info(f"Prompt to convert job posting to JSON format:\n{prompt}")

    # Call the async OpenAI API
    response_model = await call_openai_api_async(
        prompt=prompt,
        model_id=model_id,
        expected_res_type="json",
        json_type="job_site",
        temperature=temperature,
        max_tokens=max_tokens,
    )
    logger.info(f"Validated LLM Response Model: {response_model}")

    # Validate that the response is in the expected JobSiteResponseModel format
    if not isinstance(response_model, JobSiteResponse):
        logger.error(
            "Received response is not in expected JobSiteResponseModel format."
        )
        raise ValueError(
            "Received response is not in expected JobSiteResponseModel format."
        )

    # Return the model-dumped dictionary (from Pydantic obj to dict)
    return response_model


async def convert_to_json_with_claude_async(
    input_text: str,
    model_id: str = CLAUDE_HAIKU,
    max_tokens: int = 2048,
    temperature: float = 0.3,
) -> JobSiteResponse:
    """
    Parse and convert job posting content to JSON format using Anthropic API
    asynchronously.

    Args:
        - input_text (str): The cleaned text to convert to JSON.
        - model_id (str, optional): The model ID to use for OpenAI
        (default is "gpt-4-turbo").
        - temperature (float, optional): temperature for the model
        (default is 0.3).

    Returns:
        JobSiteResponse: A pydantic model representation of the extracted JSON content.

    Raises:
        ValueError: If the input text is empty or the response is not in
        the expected format.

    Logs:
        Information about the prompt and raw response.
        Errors if the response does not match the expected model format.
    """
    if not input_text:
        logger.error("Input text is empty or invalid.")
        raise ValueError("Input text cannot be empty.")

    # Set up the prompt
    prompt = CONVERT_JOB_POSTING_TO_JSON_PROMPT.format(content=input_text)
    logger.info(f"Prompt to convert job posting to JSON format:\n{prompt}")

    # Call the async OpenAI API
    response_model = await call_anthropic_api_async(
        prompt=prompt,
        model_id=model_id,
        expected_res_type="json",
        json_type="job_site",
        temperature=temperature,
        max_tokens=max_tokens,
    )
    logger.info(f"Validated LLM Response Model: {response_model}")

    # Validate that the response is in the expected JobSiteResponseModel format
    if not isinstance(response_model, JobSiteResponse):
        logger.error(
            "Received response is not in expected JobSiteResponseModel format."
        )
        raise ValueError(
            "Received response is not in expected JobSiteResponseModel format."
        )

    # Return the model-dumped dictionary (from Pydantic obj to dict)
    return response_model


def save_webpage_content(
    content_dict: Union[Dict[str, str], Dict[str, Dict[str, Any]]],
    file_path: Union[Path, str],
    file_format: Literal["json", "txt"],
) -> None:
    """
    Save the output content to a file in either txt or json format.

    Args:
        - content_dict (Union[Dict[str, str], Dict[str, Dict[str, Any]]]):
        A dictionary with URLs as keys and content as values.
        - file_path (Union[Path, str]): The file path where the content should be saved.
        - file_format (Literal["json", "txt"]): The format to save the content as,
        either "json" or "txt".

    Returns:
        None

    Raises:
        Exception: If there is an error saving the content to the specified file path.

    Logs:
        Information about successful save operations.
        Errors if the save operation fails.
    """
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            if file_format == "json":
                json.dump(content_dict, f, ensure_ascii=False, indent=4)
            else:
                for url, content in content_dict.items():
                    f.write(f"--- Content from URL: {url} ---\n{content}\n\n")
        logger.info(f"Content saved successfully to {file_path}")
    except Exception as e:
        logger.error(f"Error saving content: {e}")
        raise


# TODO: Need to integrate call llama-3 into this function later on...
# TODO: llama3 has a context window issue (requires chunking first...)
# Function to orchestrate the entire process (to be called by pipeline functions)
async def process_webpages_to_json_async(
    urls: Union[List[str], str],
    llm_provider: str = OPENAI,
    model_id: str = GPT_4_1_NANO,
    max_tokens: int = 2048,
    temperature: float = 0.3,
) -> JobPostingsBatch:
    """
    Async operation that orchestrates the entire process of extracting, cleaning,
    and converting webpage content to a structured JSON format asynchronously.

    This method performs a two-step process:

    1. **Extract and Clean Raw Content**:
        - Retrieves content from the specified URLs using Playwright.
        - Cleans the extracted content by removing JavaScript, URLs, scripts,
        and excessive whitespace,
        ensuring only relevant text remains.

    2. **Convert to Structured JSON**:
        - After cleaning, the method uses the specified LLM provider (OpenAI by default)
        to convert the
        raw, cleaned text into a structured JSON format, tailored to specific use cases
        (e.g., job postings).
        - The model specified by `model_id` is used for the conversion process.

    Args:
        - urls (List[str]): List of URLs to process and extract content from.
        - llm_provider (str, optional): The LLM provider to use for content conversion
        (default is "openai").
        - model_id (str, optional): The model ID to use for OpenAI or another provider
        (default is "gpt-4-turbo").
        - max_tokens (int): The maximum number of tokens to generate. Defaults to 2048.
        - temperature (float): The temperature value. Defaults to 0.3.

    Returns:
        JobPostingsBatch: A root model mapping each URL to a JobSiteResponse.

    Logs:
        - Information about successfully processed URLs.
        - Errors for URLs that fail during processing.
        - A list of URLs that failed to load.

    Raises:
        Exception: If any errors occur during webpage content extraction or
        conversion to JSON format.
    """

    if isinstance(urls, str):
        urls = [urls]

    # Step 1. Read raw webpage content with playwright
    webpages_content, failed_urls = await read_webpages_async(urls)  # returns 2 lists

    # Step 2. Iterate through raw web content list - root model
    batch_root: dict[str, JobSiteResponse] = {}

    # Iterate w/t OpenAI or Anthropic LLM API
    if llm_provider.lower() == OPENAI:
        for url, content in webpages_content.items():
            try:
                jobsite_model = await convert_to_json_with_gpt_async(
                    input_text=content,
                    model_id=model_id,
                    max_tokens=max_tokens,
                    temperature=temperature,
                )
                logger.info(f"Successfully converted content from {url} to JSON.")
                batch_root[url] = jobsite_model
            except Exception as e:
                logger.error(f"Error processing content for {url}: {e}")

        if failed_urls:
            logger.info(f"URLs failed to process:\n{failed_urls}")

    elif llm_provider.lower() == ANTHROPIC:
        for url, content in webpages_content.items():
            try:
                jobsite_model = await convert_to_json_with_claude_async(
                    input_text=content,
                    model_id=model_id,
                    max_tokens=max_tokens,
                    temperature=temperature,
                )
                logger.info(f"Successfully converted content from {url} to JSON.")
                batch_root[url] = jobsite_model
            except Exception as e:
                logger.error(f"Error processing content for {url}: {e}")

        if failed_urls:
            logger.info(f"URLs failed to process:\n{failed_urls}")

    else:
        raise ValueError(f"{llm_provider} is not a support LLM API.")
    return JobPostingsBatch(root=batch_root)  # type: ignore[arg-type]
