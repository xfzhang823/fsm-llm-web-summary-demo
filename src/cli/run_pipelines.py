"""
Command-line interface (CLI) wrapper for running the FSM-aware pipelines.

This file provides a clean argparse-based interface so you can trigger:

    • Stage A: URL → WEB_PAGE
    • Stage B: WEB_PAGE → WEB_SUMMARY
    • Full run: schema + URL ingest + Stage A + Stage B

from the command line, without mixing CLI code into your pipeline modules.

Usage examples:

    # Run only Stage A (scrape webpages)
    python -m cli.run_pipeline scrape

    # Run Stage A with custom concurrency
    python -m cli.run_pipeline scrape --max 10

    # Retry rows stuck in ERROR or IN_PROGRESS
    python -m cli.run_pipeline scrape --retry-errors

    # Run only Stage B (LLM summarization)
    python -m cli.run_pipeline summarize

    # Run Stage B with Anthropic
    python -m cli.run_pipeline summarize --provider anthropic --model claude-3-haiku

    # Increase concurrency for Stage B
    python -m cli.run_pipeline summarize --max 8

    # Run the entire pipeline: schema → URLs → scrape → summarize
    python -m cli.run_pipeline all

    # Full pipeline with custom concurrency for each stage
    python -m cli.run_pipeline all --max-scrape 10 --max-summarize 6

    # Full pipeline using Anthropic for summarization
    python -m cli.run_pipeline all --provider anthropic --model claude-3-haiku

    # Full pipeline with retries enabled
    python -m cli.run_pipeline all --retry-errors
"""

import argparse
import asyncio

# Import from project
from db_io.create_db_tables import create_all_db_tables
from pipelines.update_urls_pipeline_fsm import run_update_urls_pipeline_fsm
from pipelines.sync_url_table_to_pc_pipeline_fsm import (
    run_sync_url_table_to_pc_pipeline_fsm,
)
from pipelines.urls_pipeline_fsm import run_urls_pipeline_fsm
from pipelines.web_scrape_pipeline_async_fsm import run_web_scrape_pipeline_async_fsm
from pipelines.web_summarize_pipeline_async_fsm import (
    run_web_summary_pipeline_async_fsm,
)
from pipelines.advance_completed_to_next_stage import advance_all_completed
from config.project_config import OPENAI, ANTHROPIC, GPT_4_1_NANO


def build_parser():
    """
    Build and configure the top-level argparse ArgumentParser.

    This method creates the CLI structure, including:
        • A required subcommand (scrape, summarize, all)
        • Flags specific to each stage
        • Help text and defaults

    Returns:
        argparse.ArgumentParser
            Fully configured parser ready for parse_args().
    """
    parser = argparse.ArgumentParser(description="Pipeline Runner")

    # Subcommand handler: enables "scrape", "summarize", and "all" commands
    sub = parser.add_subparsers(dest="command", required=True)

    # --- Stage A CLI ---
    scrape = sub.add_parser(
        "scrape",
        help="Run Stage A: URL → WEB_PAGE (scrape webpages via Playwright)",
    )
    scrape.add_argument(
        "--max",
        type=int,
        default=5,
        help="Maximum number of URLs to process concurrently.",
    )
    scrape.add_argument(
        "--retry-errors",
        action="store_true",
        help="Re-process rows stuck in ERROR or IN_PROGRESS states.",
    )

    # --- Stage B CLI ---
    summarize = sub.add_parser(
        "summarize",
        help="Run Stage B: WEB_PAGE → WEB_SUMMARY (LLM summarization)",
    )
    summarize.add_argument(
        "--max",
        type=int,
        default=5,
        help="Maximum number of URLs to summarize concurrently.",
    )
    summarize.add_argument(
        "--retry-errors",
        action="store_true",
        help="Re-process rows stuck in ERROR or IN_PROGRESS states.",
    )
    summarize.add_argument(
        "--provider",
        default=OPENAI,
        choices=[OPENAI, ANTHROPIC],
        help="LLM provider to use (openai or anthropic).",
    )
    summarize.add_argument(
        "--model",
        default=GPT_4_1_NANO,
        help="Model ID for the chosen provider.",
    )

    # --- Full pipeline: URLs + Stage A + Stage B ---
    full = sub.add_parser(
        "all",
        help="Run the full pipeline: schema + URL ingest + WEB_PAGE → WEB_SUMMARY",
    )
    full.add_argument(
        "--max-scrape",
        dest="max_scrape",
        type=int,
        default=5,
        help="Max concurrent tasks for Stage A (scrape).",
    )
    full.add_argument(
        "--max-summarize",
        dest="max_summarize",
        type=int,
        default=5,
        help="Max concurrent tasks for Stage B (summarize).",
    )
    full.add_argument(
        "--retry-errors",
        action="store_true",
        help="If set, also retry rows in ERROR/IN_PROGRESS in both stages.",
    )
    full.add_argument(
        "--provider",
        choices=[OPENAI, ANTHROPIC],
        help="LLM provider to use for Stage B (defaults to openai if omitted).",
    )
    full.add_argument(
        "--model",
        default=GPT_4_1_NANO,
        help="Model ID for the chosen provider in Stage B.",
    )

    advance = sub.add_parser(
        "advance-completed",
        help="Advance all COMPLETED pipeline_control rows to the next FSM stage.",
    )

    return parser


async def run(args):
    """
    Execute the correct pipeline based on user-selected CLI command.
    """
    if args.command == "scrape":
        await run_web_scrape_pipeline_async_fsm(
            max_concurrent_tasks=args.max,
            retry_errors=args.retry_errors,
        )

    elif args.command == "summarize":
        await run_web_summary_pipeline_async_fsm(
            max_concurrent_tasks=args.max,
            retry_errors=args.retry_errors,
            llm_provider=args.provider,
            model_id=args.model,
        )

    elif args.command == "all":
        # 0) Ensure schema
        await asyncio.to_thread(create_all_db_tables)

        # 1) Ingest URLs from JSON into `url`
        await asyncio.to_thread(run_update_urls_pipeline_fsm, mode="append")

        # 2) Seed pipeline_control from `url` at stage=URL
        await asyncio.to_thread(run_sync_url_table_to_pc_pipeline_fsm)

        # 3) URL / control-plane setup
        await asyncio.to_thread(run_urls_pipeline_fsm)

        # 4) Stage A – scrape
        await run_web_scrape_pipeline_async_fsm(
            max_concurrent_tasks=args.max_scrape,
            retry_errors=args.retry_errors,
        )

        # 5) Stage B – summarize
        await run_web_summary_pipeline_async_fsm(
            max_concurrent_tasks=args.max_summarize,
            retry_errors=args.retry_errors,
            llm_provider=args.provider or OPENAI,
            model_id=args.model,
        )

    elif args.command == "advance-completed":
        # Run the maintenance script in a worker thread to avoid blocking the event loop
        await asyncio.to_thread(advance_all_completed)


def main():
    """
    Entry point for CLI execution.
    """
    parser = build_parser()
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
