# fsm-llm-web-summary-demo

A **demo-only** FSM + LLM pipeline that scrapes webpages, cleans the extracted text, and generates concise summaries using OpenAI or Anthropic models.  
This project showcases a simplified, lease-aware orchestration design using **DuckDB**, async workers, and a finite state machine.

> ⚠️ **Not production code.**  
> This repository is intentionally minimal and built strictly for demonstration and educational purposes.

---

## 🚀 What This Demo Shows

- Finite state machine (FSM) pipeline orchestration  
- Lease + claim logic (`worker_id`, `is_claimed`, `lease_until`)  
- Human gate via `task_state` (READY / PAUSED / SKIP)  
- Two-stage processing:
  1. **WEB_SCRAPE** — fetch + extract text  
  2. **WEB_SUMMARIZE** — call an LLM to generate a summary  
- Async scraping with `httpx` or `aiohttp`  
- LLM abstraction layer (OpenAI or Anthropic)  
- DuckDB backend with clean schema + Pydantic models  
- Retry + reclaim after lease expiration  
- Small, clean codebase ideal for articles or walkthroughs

---

## 📦 Directory Layout

