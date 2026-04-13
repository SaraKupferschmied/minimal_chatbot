from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any


def collapse_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def split_markdown_tables(markdown_text: str) -> list[str]:
    tables: list[str] = []
    current: list[str] = []
    for raw_line in (markdown_text or "").splitlines():
        line = raw_line.rstrip()
        is_table_like = "|" in line and len(line.strip()) > 0
        if is_table_like:
            current.append(line)
        else:
            if len(current) >= 2:
                tables.append("\n".join(current).strip())
            current = []
    if len(current) >= 2:
        tables.append("\n".join(current).strip())
    return tables


def markdown_to_plain_text(md: str) -> str:
    text = md or ""
    text = re.sub(r"```.*?```", " ", text, flags=re.S)
    text = re.sub(r"!\[[^\]]*\]\([^)]*\)", " ", text)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1", text)
    text = re.sub(r"^[#>\-\*]+\s*", "", text, flags=re.M)
    text = re.sub(r"\|", " ", text)
    text = re.sub(r"`", "", text)
    return collapse_ws(text)


def parse_pdf(pdf_path: Path, parsing_instructions: str, language_hint: str | None = None) -> dict[str, Any]:
    from llama_parse import LlamaParse

    parser = LlamaParse(
        result_type="markdown",
        parsing_instructions=parsing_instructions,
        language=language_hint,
        premium_mode=True,
        split_by_page=True,
    )

    docs = parser.load_data(str(pdf_path))
    pages = []
    for idx, doc in enumerate(docs, start=1):
        md = doc.text or ""
        pages.append(
            {
                "page": idx,
                "markdown": md,
                "text": markdown_to_plain_text(md),
                "tables": split_markdown_tables(md),
                "metadata": getattr(doc, "metadata", {}) or {},
            }
        )

    title = None
    if docs:
        first_meta = getattr(docs[0], "metadata", {}) or {}
        title = first_meta.get("document_title") or first_meta.get("title")

    return {
        "title": title,
        "pages": pages,
        "parser": "llama-parse",
        "pdf_path": str(pdf_path),
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--instructions", required=True)
    ap.add_argument("--language", default=None)
    args = ap.parse_args()

    out = parse_pdf(Path(args.pdf), args.instructions, args.language)
    Path(args.out).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(args.out)
