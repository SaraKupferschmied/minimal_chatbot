from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from langchain_core.documents import Document
from langchain_community.vectorstores import FAISS
from langchain_ollama import OllamaEmbeddings

from app.config import settings


def _chunk_dir_for(target: str, parser: str) -> Path:
    root = Path(__file__).resolve().parents[2] / "scrapy_crawler" / "outputs"
    if target == "studyplans":
        return root / ("parsed_chunks_llamaparse" if parser == "llamaparse" else "parsed_chunks")
    if target in {"regulations", "reglementations"}:
        reg_root = root / "reglementation_docs"
        return reg_root / ("parsed_chunks_llamaparse" if parser == "llamaparse" else "parsed_chunks_regulations")
    raise ValueError(f"Unknown target: {target}")


def _index_dir_for(target: str, parser: str) -> Path:
    suffix = "_llamaparse" if parser == "llamaparse" else ""
    if target == "studyplans":
        return Path(str(settings.studyplans_index) + suffix)
    if target in {"regulations", "reglementations"}:
        return Path(str(settings.reglementations_index) + suffix)
    raise ValueError(f"Unknown target: {target}")


def _iter_chunk_files(chunks_dir: Path) -> Iterable[Path]:
    if not chunks_dir.exists():
        raise FileNotFoundError(f"Chunks directory not found: {chunks_dir}")
    for path in sorted(chunks_dir.glob("*.jsonl")):
        if path.name.startswith("_"):
            continue
        yield path


def _normalize_text(text: str) -> str:
    return " ".join((text or "").split()).strip()


def _truncate_text(text: str, max_chars: int = 4500) -> str:
    return _normalize_text(text)[:max_chars]


def _is_useful_chunk(obj: dict) -> bool:
    text = _truncate_text(obj.get("text", ""))
    if not text or len(text) < 20:
        return False
    return str(obj.get("chunk_type")) in {"page", "section", "course_row", "table", "table_row"}


def _priority_for_chunk_type(chunk_type: str) -> int:
    return {
        "table_row": 5,
        "course_row": 4,
        "section": 3,
        "table": 2,
        "page": 1,
    }.get(chunk_type, 0)


def _split_large_doc(doc: Document, max_chars: int = 1100, overlap: int = 180) -> list[Document]:
    text = (doc.page_content or "").strip()
    if len(text) <= max_chars:
        return [doc]
    out: list[Document] = []
    start = 0
    while start < len(text):
        end = min(len(text), start + max_chars)
        chunk_text = text[start:end].strip()
        meta = dict(doc.metadata)
        meta["subchunk_start"] = start
        meta["subchunk_end"] = end
        out.append(Document(page_content=chunk_text, metadata=meta))
        if end >= len(text):
            break
        start = max(0, end - overlap)
    return out


def _build_semantic_text(obj: dict) -> str:
    prefix_parts = [
        obj.get("title"),
        obj.get("degree_level"),
        obj.get("program_name"),
        f"{obj.get('total_ects')} ECTS" if obj.get("total_ects") else None,
        f"Page {obj.get('page')}" if obj.get("page") else None,
        obj.get("section"),
        obj.get("subsection"),
        obj.get("chunk_type"),
    ]
    prefix = " | ".join([str(x).strip() for x in prefix_parts if x not in (None, "", "None")])
    body = _truncate_text(obj.get("text", ""))
    return f"{prefix}\n{body}" if prefix else body


def _load_documents_from_jsonl(chunks_dir: Path) -> list[Document]:
    docs: list[Document] = []
    seen_ids: set[str] = set()
    for file_path in _iter_chunk_files(chunks_dir):
        with file_path.open("r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as e:
                    print(f"[warn] invalid json in {file_path.name}:{line_no}: {e}")
                    continue
                if not _is_useful_chunk(obj):
                    continue
                chunk_id = str(obj.get("chunk_id") or "")
                if chunk_id and chunk_id in seen_ids:
                    continue
                if chunk_id:
                    seen_ids.add(chunk_id)
                metadata = {
                    "chunk_id": obj.get("chunk_id"),
                    "doc_key": obj.get("doc_key") or obj.get("reg_doc_key"),
                    "program_key": obj.get("program_key"),
                    "source_url": obj.get("source_url") or obj.get("document_page_url"),
                    "local_path": obj.get("local_path"),
                    "sha256": obj.get("sha256"),
                    "title": obj.get("title"),
                    "faculty": obj.get("faculty"),
                    "degree_level": obj.get("degree_level"),
                    "total_ects": obj.get("total_ects"),
                    "program_name": obj.get("program_name"),
                    "doc_label": obj.get("doc_label"),
                    "source_type": obj.get("source_type"),
                    "page": obj.get("page"),
                    "chunk_type": obj.get("chunk_type"),
                    "section": obj.get("section"),
                    "subsection": obj.get("subsection"),
                    "prev_chunk_id": obj.get("prev_chunk_id"),
                    "next_chunk_id": obj.get("next_chunk_id"),
                    "parent_section_id": obj.get("parent_section_id"),
                    "parser": obj.get("parser"),
                    "chunk_priority": _priority_for_chunk_type(str(obj.get("chunk_type"))),
                    "source_file": file_path.name,
                }
                docs.extend(_split_large_doc(Document(page_content=_build_semantic_text(obj), metadata=metadata)))
    return docs


def _build_faiss_index(documents: list[Document], index_dir: Path, batch_size: int = 128) -> FAISS:
    if not documents:
        raise RuntimeError("No documents found to index.")
    index_dir.mkdir(parents=True, exist_ok=True)
    embeddings = OllamaEmbeddings(model=settings.ollama_embedding_model, base_url=settings.ollama_host)
    db: FAISS | None = None
    total = len(documents)
    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        batch = documents[start:end]
        print(f"[embed] batch {start}-{end} / {total}")
        if db is None:
            db = FAISS.from_documents(batch, embeddings)
        else:
            db.add_documents(batch)
    assert db is not None
    db.save_local(str(index_dir))
    return db


def build_index_for(target: str, parser: str, force_rebuild: bool = False) -> FAISS:
    chunks_dir = _chunk_dir_for(target, parser)
    index_dir = _index_dir_for(target, parser)
    if index_dir.exists() and not force_rebuild:
        print(f"[info] index already exists: {index_dir}")
        print("[info] use --force to rebuild")
        embeddings = OllamaEmbeddings(model=settings.ollama_embedding_model, base_url=settings.ollama_host)
        return FAISS.load_local(str(index_dir), embeddings, allow_dangerous_deserialization=True)
    documents = _load_documents_from_jsonl(chunks_dir)
    print(f"[info] loaded {len(documents)} chunks from {chunks_dir}")
    type_counts: dict[str, int] = {}
    for d in documents:
        t = str(d.metadata.get("chunk_type"))
        type_counts[t] = type_counts.get(t, 0) + 1
    print("[info] chunk types:")
    for k, v in sorted(type_counts.items()):
        print(f"  - {k}: {v}")
    return _build_faiss_index(documents, index_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build FAISS indexes from parsed JSONL chunks.")
    parser.add_argument("--target", choices=["all", "studyplans", "regulations", "reglementations"], default="all")
    parser.add_argument("--parser", choices=["pdfjs", "llamaparse"], default="llamaparse")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    targets: list[str] = []
    if args.target in {"all", "studyplans"}:
        targets.append("studyplans")
    if args.target in {"all", "regulations", "reglementations"}:
        targets.append("regulations")
    for target in targets:
        print(f"\n=== Building {target} index ({args.parser}) ===")
        db = build_index_for(target, parser=args.parser, force_rebuild=args.force)
        print(f"[done] {target} -> {_index_dir_for(target, args.parser)} ({len(db.index_to_docstore_id)} vectors)")


if __name__ == "__main__":
    main()
