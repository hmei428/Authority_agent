#!/usr/bin/env python3
"""
Quick sanity check for the Direct LLM scoring endpoints.

Usage:
  python3 test_llm_scoring.py \
    --query "单位公积金显示缴存基数不在规定范围内" \
    --title "公积金缴存基数异常怎么办" \
    --content "..." \
    --host "www.example.com"
"""
import argparse
import sys
import time

from search_agent.scoring_optimized import score_both_parallel


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test LLM scoring availability")
    parser.add_argument("--query", required=True, help="Query text to evaluate relevance")
    parser.add_argument("--title", default="", help="Title text")
    parser.add_argument("--content", default="", help="Content snippet/body")
    parser.add_argument("--host", required=True, help="Host/domain for authority scoring")
    parser.add_argument("--repeat", type=int, default=1, help="Number of test iterations")
    return parser.parse_args()


def run_once(host: str, query: str, title: str, content: str) -> None:
    start = time.time()
    authority_score, authority_reason, relevance_score, relevance_reason = score_both_parallel(
        host=host,
        query=query,
        title=title,
        content=content,
    )
    elapsed = time.time() - start
    print(f"[{elapsed:.2f}s] host={host} -> authority_score={authority_score}, reason={authority_reason}")
    print(f"[{elapsed:.2f}s] query='{query[:15]}...' -> relevance_score={relevance_score}, reason={relevance_reason}")
    print("-" * 80)


def main() -> int:
    args = parse_args()
    for idx in range(1, args.repeat + 1):
        print(f"=== Run {idx}/{args.repeat} ===")
        try:
            run_once(
                host=args.host,
                query=args.query,
                title=args.title,
                content=args.content,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"Run {idx} failed: {exc}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
