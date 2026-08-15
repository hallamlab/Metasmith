#!/usr/bin/env python3
"""
mitomaster_batch.py
Batch-submit FASTA files to MITOMASTER and concatenate results with safe
resume, concurrency, and robust CLI.

Example:
  python mitomaster_batch.py \
    --data-dir /path/to/chunks \
    --glob-pattern "*.fasta" \
    --output-file /path/to/mitomaster_combined.tsv \
    --max-workers 8 --retries 4 --timeout 90 --overwrite
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Iterable, List, Set, Tuple, Optional

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


def build_session(retries: int, backoff: float, timeout: int, user_agent: str) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=None,  # retry POSTs too
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": user_agent})
    # Attach a default timeout to all requests via a wrapper
    session.request = _timeout_wrapper(session.request, timeout)  # type: ignore
    return session


def _timeout_wrapper(func, timeout: int):
    def wrapped(method, url, **kwargs):
        if "timeout" not in kwargs:
            kwargs["timeout"] = timeout
        return func(method, url, **kwargs)
    return wrapped


def load_done(checkpoint_file: Path) -> Set[str]:
    if checkpoint_file.exists():
        with checkpoint_file.open() as f:
            return {line.strip() for line in f if line.strip()}
    return set()


def write_checkpoint(checkpoint_file: Path, fasta_name: str, lock: Lock) -> None:
    with lock:
        with checkpoint_file.open("a") as f:
            f.write(fasta_name + "\n")


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def iter_fastas(data_dir: Path, glob_pattern: str, recursive: bool) -> List[Path]:
    if recursive:
        return sorted(data_dir.rglob(glob_pattern))
    return sorted(data_dir.glob(glob_pattern))


def post_one(
    session: requests.Session,
    endpoint: str,
    fasta_path: Path,
    file_type: str,
    output_format: str,
) -> str:
    with open(fasta_path, "rb") as f:
        files = {
            "file": (fasta_path.name, f, "application/octet-stream"),
            "fileType": (None, file_type),
            "output": (None, output_format),
        }
        resp = session.post(endpoint, files=files)
        resp.raise_for_status()
        return resp.text


def append_output(
    out_path: Path,
    payload: str,
    mode: str,
    lock: Lock,
) -> None:
    """
    mode: 'full' -> write as-is
          'noheader' -> drop first line before writing
    """
    if mode not in {"full", "noheader"}:
        raise ValueError("mode must be 'full' or 'noheader'")

    text = payload if mode == "full" else "\n".join(payload.splitlines()[1:])
    with lock:
        first_write = not out_path.exists() or out_path.stat().st_size == 0
        with out_path.open("a") as out_f:
            # separate blocks with newline if not the first write and file doesn't end with newline
            if not first_write:
                out_f.write("\n")
            out_f.write(text)


def process_first_then_pool(
    session: requests.Session,
    files: List[Path],
    endpoint: str,
    file_type: str,
    output_format: str,
    out_path: Path,
    chk_path: Path,
    max_workers: int,
    lock: Lock,
    header_mode: str,
    log_prefix: str = "",
) -> Tuple[int, int]:
    """
    Ensures the header is written first when header_mode='first'.
    Returns (ok_count, err_count)
    """
    ok = 0
    err = 0

    if not files:
        return ok, err

    def _submit(fasta: Path, mode: str):
        nonlocal ok, err
        try:
            txt = post_one(session, endpoint, fasta, file_type, output_format)
            append_output(out_path, txt, mode, lock)
            write_checkpoint(chk_path, fasta.name, lock)
            ok += 1
            print(f"{log_prefix}✅ Done: {fasta.name}")
        except Exception as e:
            err += 1
            print(f"{log_prefix}❌ Error: {fasta.name}: {e}")

    if header_mode == "first":
        # Process first file synchronously to guarantee header at top
        head = files[0]
        _submit(head, "full")
        rest = files[1:]
        if rest:
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futs = [ex.submit(_submit, fp, "noheader") for fp in rest]
                for f in as_completed(futs):
                    # surface exceptions (already counted inside)
                    try:
                        f.result()
                    except Exception:
                        pass
    elif header_mode == "all":
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = [ex.submit(_submit, fp, "full") for fp in files]
            for f in as_completed(futs):
                try:
                    f.result()
                except Exception:
                    pass
    elif header_mode == "none":
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = [ex.submit(_submit, fp, "noheader") for fp in files]
            for f in as_completed(futs):
                try:
                    f.result()
                except Exception:
                    pass
    else:
        raise ValueError("header_mode must be one of: first, all, none")

    return ok, err


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Batch-submit FASTA files to MITOMASTER with safe resume and concurrency.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    io = p.add_argument_group("I/O")
    io.add_argument("--data-dir", type=Path, required=True, help="Directory containing FASTA files")
    io.add_argument("--glob-pattern", default="*.fasta", help="Glob pattern for FASTA files")
    io.add_argument("--recursive", action="store_true", help="Recurse into subdirectories")
    io.add_argument("--output-file", type=Path, required=True, help="Concatenated TSV output path")
    io.add_argument("--checkpoint-file", type=Path, help="Checkpoint file (defaults to OUTPUT + .done)")
    io.add_argument("--overwrite", action="store_true", help="Overwrite output & checkpoint before starting")
    io.add_argument("--dry-run", action="store_true", help="List files to process and exit")

    net = p.add_argument_group("Network")
    net.add_argument("--endpoint", default="https://mitomap.org/mitomaster/websrvc.cgi", help="MITOMASTER endpoint")
    net.add_argument("--file-type", default="sequences", help="Value for form field 'fileType'")
    net.add_argument("--output-format", default="hsd", help="Value for form field 'output'")
    net.add_argument("--timeout", type=int, default=60, help="Per-request timeout (seconds)")
    net.add_argument("--retries", type=int, default=3, help="Total retries (includes connect/read/status)")
    net.add_argument("--backoff", type=float, default=1.0, help="Exponential backoff factor for retries")
    net.add_argument("--user-agent", default="mitomaster-batch/1.0 (+https://example.org)", help="HTTP User-Agent")

    run = p.add_argument_group("Run")
    run.add_argument("--max-workers", type=int, default=8, help="Thread pool size")
    run.add_argument("--header-mode", choices=["first", "all", "none"], default="first",
                     help="How to handle per-file headers when concatenating")
    run.add_argument("--respect-checkpoint", action="store_true", help="Skip any FASTA already listed in checkpoint")
    run.add_argument("--log-prefix", default="", help="Optional prefix for log lines (useful in controllers)")

    args = p.parse_args()

    if args.checkpoint_file is None:
        args.checkpoint_file = Path(str(args.output_file) + ".done")

    return args


def main():
    args = parse_args()
    ensure_parent(args.output_file)
    ensure_parent(args.checkpoint_file)

    if args.overwrite:
        if args.output_file.exists():
            args.output_file.unlink()
        if args.checkpoint_file.exists():
            args.checkpoint_file.unlink()

    all_fastas = iter_fastas(args.data_dir, args.glob_pattern, args.recursive)
    if not all_fastas:
        print("No FASTA files found.")
        return

    done: Set[str] = load_done(args.checkpoint_file) if args.respect_checkpoint else set()
    remaining = [fp for fp in all_fastas if fp.name not in done]

    if args.dry_run:
        print(f"Would process {len(remaining)} files:")
        for fp in remaining:
            print(fp)
        return

    print(f"Found {len(remaining)} unprocessed FASTA files")

    if not remaining:
        print("✅ All files already processed.")
        print(f"📝 Results at {args.output_file}")
        return

    lock = Lock()
    session = build_session(
        retries=args.retries,
        backoff=args.backoff,
        timeout=args.timeout,
        user_agent=args.user_agent,
    )

    ok, err = process_first_then_pool(
        session=session,
        files=remaining,
        endpoint=args.endpoint,
        file_type=args.file_type,
        output_format=args.output_format,
        out_path=args.output_file,
        chk_path=args.checkpoint_file,
        max_workers=args.max_workers,
        lock=lock,
        header_mode=args.header_mode,
        log_prefix=args.log_prefix,
    )

    print(f"✅ Success: {ok}  ❌ Failed: {err}")
    print(f"📝 Results saved to {args.output_file}")
    print(f"⏭️  Checkpoint at {args.checkpoint_file}  ({'respected' if args.respect_checkpoint else 'ignored'})")


if __name__ == "__main__":
    main()
