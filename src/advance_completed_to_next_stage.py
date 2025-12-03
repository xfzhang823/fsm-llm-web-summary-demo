"""
advance_completed_to_next_stage.py

Small maintenance script:
    Move ALL pipeline_control rows whose status=COMPLETED
    to the next FSM stage by calling fsm.step().

Works for ANY stage:
    URL → WEB_PAGE
    WEB_PAGE → WEB_SUMMARY
    WEB_SUMMARY → DONE
"""

from db_io.db_utils import get_urls_by_status, get_urls_from_pipeline_control
from fsm.pipeline_enums import PipelineStatus
from fsm.pipeline_fsm_manager import PipelineFSMManager


def advance_all_completed():
    """
    Advance all COMPLETED pipeline_control rows one stage forward.
    No async, no leases, no claiming — pure admin operation.
    """
    fsm_manager = PipelineFSMManager()

    # 1) Get ALL urls whose row is COMPLETED in the current stage.
    urls = get_urls_from_pipeline_control(
        status=PipelineStatus.COMPLETED, active_urls_only=True
    )
    if not urls:
        print("No COMPLETED rows found.")
        return

    print(f"Found {len(urls)} COMPLETED rows. Advancing...")

    for url in urls:
        try:
            fsm = fsm_manager.get_fsm(url)

            # Debug
            print(f"  • {url}: current={fsm.state}")

            # 2) Advance one stage
            fsm.step()

            print(f"    → advanced to {fsm.state}")

        except Exception as e:
            print(f"⚠️ Failed advancing {url}: {e}")

    print("Done.")


if __name__ == "__main__":
    advance_all_completed()
