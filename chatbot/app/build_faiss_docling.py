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
    if parser == "pdfjs":
        if target == "studyplans":
            return root / "parsed_chunks"
        if target in {"regulations", "reglementations"}:
            return root / "parsed_chunks_regulations"
    if parser == "llamaparse":
        if target == "studyplans":
            return root / "parsed_chunks_llamaparse"
        if target in {"regulations", "reglementations"}:
            return root / "reglementation_docs" / "parsed_chunks_llamaparse"
    if parser == "docling":
        if target == "studyplans":
            return root / "parsed_chunks_docling"
        if target in {"regulations", "reglementations"}:
            return root / "reglementation_docs" / "parsed_chunks_docling"
    raise ValueError(f"Unknown target/parser combination: {target}/{parser}")


def _index_dir_for(target: str, parser: str) -> Path:
    base = settings.studyplans_index if target == "studyplans" else settings.reglementations_index
    if parser == "pdfjs":
        return base
    return base / parser


def _iter_chunk_files(chunks_dir: Path) -> Iterable[Path]:
    if not chunks_dir.exists():
        raise FileNotFoundError(f"Chunks directory not found: {chunks_dir}")
    for path in sorted(chunks_dir.glob("*.jsonl")):
        if path.name.startswith("_"):
            continue
        yield path


def _normalize_text(text: str) -> str:
    return " ".join((text or "").split()).strip()


def _truncate_text(text: str, max_chars: int = 4000) -> str:
    text = _normalize_text(text)
    return text[:max_chars]


def _is_useful_chunk(obj: dict) -> bool:
    text = _truncate_text(_normalize_text(obj.get("text", "")), max_chars=4000)
    if not text:
        return False
    chunk_type = obj.get("chunk_type")
    if chunk_type not in {"page", "section", "course_row", "table", "table_row"}:
        return False
    return len(text) >= 20


def _split_large_doc(doc: Document, max_chars: int = 1200, overlap: int = 150) -> list[Document]:
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
                text = _truncate_text(_normalize_text(obj["text"]), max_chars=4000)
                metadata = dict(obj)
                metadata["source_file"] = file_path.name
                base_doc = Document(page_content=text, metadata=metadata)
                docs.extend(_split_large_doc(base_doc))
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


def build_index_for(target: str, parser: str = "pdfjs", force_rebuild: bool = False) -> FAISS:
    chunks_dir = _chunk_dir_for(target, parser)
    index_dir = _index_dir_for(target, parser)
    embeddings = OllamaEmbeddings(model=settings.ollama_embedding_model, base_url=settings.ollama_host)
    if index_dir.exists() and not force_rebuild:
        print(f"[info] index already exists: {index_dir}")
        print("[info] use --force to rebuild")
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
    parser.add_argument("--parser", choices=["pdfjs", "llamaparse", "docling"], default="pdfjs")
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
        print(f"[done] {target}/{args.parser} -> {_index_dir_for(target, args.parser)} ({len(db.index_to_docstore_id)} vectors)")


if __name__ == "__main__":
    main()
