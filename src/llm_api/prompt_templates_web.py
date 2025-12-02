"""
prompt_templates_web.py

Prompt bank for the *web summary demo* project.

This module defines standardized prompt templates for converting
`WebPageContent` objects into:

  • A concise natural-language summary (free-text)
  • A structured JSON summary (LLM-validated WebSummaryJSON)

These are intentionally lightweight compared to the job_bot project,
and tailored for generic webpages rather than job postings.

Usage:
    from prompt_bank_web import (
        SUMMARIZE_WEBPAGE_TEXT_PROMPT,
        SUMMARIZE_WEBPAGE_TO_JSON_PROMPT,
    )

    prompt = SUMMARIZE_WEBPAGE_TEXT_PROMPT.format(
        url=content.url,
        title=content.title or "",
        headings=headings_str,
        clean_text=content.clean_text,
    )
"""

# ---------------------------------------------------------------------------
# Free-text summarization (default Stage B)
# ---------------------------------------------------------------------------

SUMMARIZE_WEBPAGE_TEXT_PROMPT = """
You are a concise, neutral summarization assistant.

Your task:
Summarize the following webpage into **3–6 bullet points** that capture the
main ideas, facts, and themes. Avoid filler language, avoid repetition, and do
not add commentary or interpretation. Focus strictly on what the page actually
says.

Webpage Information:
URL: {url}
Title: {title}
Headings: {headings}

CONTENT:
{clean_text}

Instructions:
- Produce 3–6 short, information-rich bullet points.
- No preamble or explanation.
- No markdown fences.
- Only output the bullet points.
"""


# ---------------------------------------------------------------------------
# JSON-structured summarization (WebSummaryJSON)
#   — optional but useful for downstream processing
# ---------------------------------------------------------------------------

# IMPORTANT: Curly braces in JSON examples MUST be doubled for .format()
SUMMARIZE_WEBPAGE_TO_JSON_PROMPT = """
You are an expert analyst trained to convert webpage content into structured,
machine-readable JSON.

Your task:
Given the webpage content below, extract its essential meaning into a clean,
high-level JSON summary. Capture only what is present in the text—do not infer
details that the content does not support.

Webpage Information:
URL: {url}
Title: {title}
Headings: {headings}

CONTENT:
{clean_text}

Output Format:
Return ONLY a valid JSON object (no comments, no markdown). The structure must be:

{{
  "title": "<short headline or null>",
  "bullets": [
    "Main idea #1",
    "Main idea #2",
    "Main idea #3"
  ],
  "key_points": [
    "Optional emphasized key takeaway #1",
    "Optional emphasized key takeaway #2"
  ],
  "sections": {{
    "section_name": [
      "Sub-point #1",
      "Sub-point #2"
    ],
    "another_section": [
      "Sub-point"
    ]
  }}
}}

Rules:
1. Use null for any field with no available information.
2. Do NOT use markdown formatting.
3. Do NOT invent content not supported by the text.
4. Ensure the JSON is syntactically valid.
"""


# ---------------------------------------------------------------------------
# Optional: Title extraction heuristic prompt (if used later)
# ---------------------------------------------------------------------------

EXTRACT_WEBPAGE_TITLE_PROMPT = """
You are given raw webpage content. Extract the **best possible short title** that
represents the main subject of the page.

CONTENT:
{clean_text}

Output:
Return ONLY the title as a single plain-text line. No quotes, no markdown.
"""


# ---------------------------------------------------------------------------
# Optional: Headings extraction heuristic prompt (if used later)
# ---------------------------------------------------------------------------

EXTRACT_WEBPAGE_HEADINGS_PROMPT = """
Extract the top 3–5 conceptual headings from the following webpage text.
These are not HTML headings, but human-level major themes.

CONTENT:
{clean_text}

Output:
Return a plain-text list, one heading per line, NO numbering, NO markdown.
"""
