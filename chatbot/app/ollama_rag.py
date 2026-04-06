"""
Ollama-based RAG for regulations + study plans.

- Loads parsed PDFs from ./parsed/<subfolder>
- Splits into chunks
- Creates/loads persisted FAISS indexes
- Answers questions via ChatOllama using ONLY retrieved context
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import List, Tuple

from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_ollama import ChatOllama, OllamaEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.documents import Document

from .config import settings


_SPLITTER = RecursiveCharacterTextSplitter(
    chunk_size=900,
    chunk_overlap=150,
    separators=[
        "\nArt. ",
        "\nArtikel ",
        "\n§",
        "\n## ",
        "\n### ",
        "\n\n",
        "\n",
        " ",
        "",
    ],
)

_EMBEDDINGS = OllamaEmbeddings(
    model=settings.ollama_embedding_model,
    base_url=settings.ollama_host,
)

META_RE = re.compile(r"---METADATA_JSON---\s*(\{.*?\})\s*---/METADATA_JSON---", re.S)
PAGE_RE = re.compile(r"---PAGE\s+(\d+)---\s*(.*?)(?=---PAGE\s+\d+---|\Z)", re.S)


def _language_name(language: str | None) -> str:
    if language == "de":
        return "German"
    if language == "fr":
        return "French"
    return "English"


def _build_prompt(language: str | None) -> ChatPromptTemplate:
    target_language = _language_name(language)

    return ChatPromptTemplate.from_messages(
        [
            (
                "system",
                "You are a careful assistant for university regulations and study plans. "
                "Answer ONLY using the provided context. "
                "If the answer is not in the context, say you cannot find it in the documents. "
                f"Always answer in {target_language}. "
                f"Use natural, clear {target_language}. "
                "The retrieved documents may be in German, French, English, or Italian; use them all if relevant. "
                "Even if the documents are written in another language, the final answer must be in the requested language. "
                "Always cite sources as [filename p.X]. "
                "Do not invent requirements, module rules, deadlines, ECTS, or semester recommendations.",
            ),
            ("human", "Question: {question}\n\nContext:\n{context}\n\nAnswer with citations:"),
        ]
    )


def _load_parsed_file(path: str, category: str) -> List[Document]:
    raw = Path(path).read_text(encoding="utf-8")

    meta_match = META_RE.search(raw)
    if not meta_match:
        raise ValueError(f"Missing metadata block in {path}")

    meta = json.loads(meta_match.group(1))
    docs: List[Document] = []

    for page_num, page_text in PAGE_RE.findall(raw):
        text = page_text.strip()
        if not text:
            continue

        source_name = Path(
            meta.get("local_path")
            or meta.get("pdf_url")
            or meta.get("document_page_url")
            or path
        ).name

        docs.append(
            Document(
                page_content=text,
                metadata={
                    "sha256": meta.get("sha256"),
                    "doc_key": meta.get("doc_key") or meta.get("reg_doc_key"),
                    "source_url": meta.get("source_url") or meta.get("pdf_url") or meta.get("document_page_url"),
                    "local_path": meta.get("local_path"),
                    "source": source_name,
                    "page": int(page_num),
                    "category": category,
                    "source_type": meta.get("source_type") or "pdf",
                    "title": meta.get("title"),
                    "program_name": meta.get("program_name"),
                    "degree_level": meta.get("degree_level"),
                    "faculty": meta.get("faculty"),
                    "total_ects": meta.get("total_ects"),
                    "doc_label": meta.get("doc_label"),
                    "source_file": Path(path).name,
                },
            )
        )

    if not docs:
        raise ValueError(f"No page content found in {path}")

    return docs


def _load_parsed_documents(parsed_paths: List[str], category: str) -> List[Document]:
    docs: List[Document] = []
    failed = []

    for path in parsed_paths:
        try:
            docs.extend(_load_parsed_file(path, category))
        except Exception as e:
            failed.append((path, str(e)))
            print(f"[warn] Failed to load parsed file: {path} -> {type(e).__name__}: {e!r}")

    print(
        f"[index] category={category} pages_loaded={len(docs)} "
        f"failed_files={len(failed)} total_files={len(parsed_paths)}"
    )

    if not docs:
        raise RuntimeError(f"No readable parsed files found for category '{category}'")

    return docs


def _normalize_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _tokenize_for_match(value: str | None) -> list[str]:
    text = _normalize_text(value)
    return [t for t in re.findall(r"\w+", text) if len(t) >= 4]


def _expand_query(question: str) -> str:
    q = question.strip()
    q_low = q.lower()
    extras: list[str] = []

    if "study plan" in q_low or "studienplan" in q_low or "curriculum" in q_low:
        extras.append("study plan curriculum Studienplan")
    if "first semester" in q_low or "semester 1" in q_low:
        extras.append("semester 1 first semester")
    if "second semester" in q_low or "semester 2" in q_low:
        extras.append("semester 2 second semester")
    if "third semester" in q_low or "semester 3" in q_low:
        extras.append("semester 3 third semester")
    if "mandatory" in q_low:
        extras.append("mandatory required Pflichtfach obligatoire")
    if "elective" in q_low:
        extras.append("elective optional Wahlfach optionnel")
    if "ects" in q_low or "credits" in q_low:
        extras.append("ECTS credits credit points")

    if not extras:
        return q

    return f"{q}\n\nRelated terms: {' | '.join(extras)}"


def _all_documents(db: FAISS) -> list[Document]:
    # Uses FAISS docstore internals; acceptable for this project
    return list(db.docstore._dict.values())


def _metadata_match_score(question: str, doc: Document) -> int:
    q = _normalize_text(question)

    title = _normalize_text(doc.metadata.get("title"))
    program = _normalize_text(doc.metadata.get("program_name"))
    degree = _normalize_text(doc.metadata.get("degree_level"))
    category = _normalize_text(doc.metadata.get("category"))
    doc_label = _normalize_text(doc.metadata.get("doc_label"))
    total_ects = doc.metadata.get("total_ects")

    requested_degree = _extract_requested_degree(question)
    requested_ects = _extract_requested_ects(question)

    score = 0

    # hard constraints / very strong boosts
    if requested_degree:
        if degree == requested_degree.lower():
            score += 12
        else:
            score -= 20

    if requested_ects is not None:
        if total_ects == requested_ects:
            score += 12
        else:
            score -= 20

    # exact-ish substring matches
    if title and title in q:
        score += 8
    if program and program in q:
        score += 8

    # token overlap
    for field, weight in ((title, 2), (program, 3), (degree, 1)):
        tokens = _tokenize_for_match(field)
        overlap = sum(1 for t in tokens if t in q)
        score += min(overlap, 5) * weight

    # prefer study-plan-ish docs for course questions
    if any(term in q for term in ["course", "courses", "semester", "year", "study plan", "curriculum"]):
        if doc_label in {"study plan", "studienplan", "brochure", "broschüre"}:
            score += 4

    if "study plan" in q or "studienplan" in q or "curriculum" in q:
        if category == "studyplans":
            score += 3

    return score


def _candidate_docs_from_metadata(db: FAISS, question: str, min_score: int = 8) -> list[Document]:
    scored: list[tuple[int, Document]] = []

    for d in _all_documents(db):
        s = _metadata_match_score(question, d)
        if s >= min_score:
            scored.append((s, d))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [d for _, d in scored]


def _doc_key_set(docs: list[Document]) -> set[str]:
    keys = set()
    for d in docs:
        key = d.metadata.get("doc_key")
        if isinstance(key, str) and key.strip():
            keys.add(key)
    return keys


def _filter_docs_by_doc_keys(db: FAISS, doc_keys: set[str]) -> list[Document]:
    if not doc_keys:
        return []
    return [
        d for d in _all_documents(db)
        if d.metadata.get("doc_key") in doc_keys
    ]


def _rank_docs_by_query_overlap(docs: list[Document], query: str, limit: int) -> list[Document]:
    q_tokens = set(_tokenize_for_match(query))
    if not q_tokens:
        return docs[:limit]

    scored: list[tuple[int, Document]] = []
    for d in docs:
        text = _normalize_text(d.page_content)
        meta_blob = _normalize_text(
            " ".join(
                [
                    str(d.metadata.get("title") or ""),
                    str(d.metadata.get("program_name") or ""),
                    str(d.metadata.get("degree_level") or ""),
                    str(d.metadata.get("doc_label") or ""),
                    str(d.metadata.get("total_ects") or ""),
                ]
            )
        )

        score = 0
        for t in q_tokens:
            if t in text:
                score += 1
            if t in meta_blob:
                score += 2

        # explicit boosts
        if d.metadata.get("degree_level") == "Bachelor":
            score += 4
        if d.metadata.get("total_ects") == 180:
            score += 4

        program_name = _normalize_text(d.metadata.get("program_name"))
        title = _normalize_text(d.metadata.get("title"))
        if "business informatics" in program_name or "wirtschaftsinformatik" in program_name:
            score += 6
        elif "business informatics" in title or "wirtschaftsinformatik" in title:
            score += 4

        scored.append((score, d))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [d for _, d in scored[:limit]]


def _dedupe_docs(docs: list[Document]) -> list[Document]:
    seen = set()
    out: list[Document] = []

    for d in docs:
        key = (
            d.metadata.get("doc_key"),
            d.metadata.get("page"),
            d.page_content[:160],
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(d)

    return out


def _retrieve_docs(db: FAISS, question: str, k: int) -> list[Document]:
    expanded_query = _expand_query(question)

    matched_docs = _candidate_docs_from_metadata(db, question, min_score=8)
    matched_keys = _doc_key_set(matched_docs)
    filtered_docs = _filter_docs_by_doc_keys(db, matched_keys)

    # If we have strong metadata matches, stay inside that candidate set.
    if filtered_docs:
        filtered_ranked = _rank_docs_by_query_overlap(
            filtered_docs,
            expanded_query,
            limit=max(k * 3, 12),
        )
        return _dedupe_docs(filtered_ranked)[:k]

    # fallback to semantic retrieval only if metadata path found nothing
    semantic_docs = db.as_retriever(
        search_type="mmr",
        search_kwargs={
            "k": k,
            "fetch_k": max(20, k * 4),
            "lambda_mult": 0.25,
        },
    ).invoke(expanded_query)

    return _dedupe_docs(semantic_docs)[:k]

def _extract_requested_degree(question: str) -> str | None:
    q = _normalize_text(question)
    if "bachelor" in q:
        return "Bachelor"
    if "master" in q:
        return "Master"
    return None


def _extract_requested_ects(question: str) -> int | None:
    q = _normalize_text(question)
    m = re.search(r"\b(30|60|90|120|180)\s*ects\b", q)
    if m:
        return int(m.group(1))
    return None


def build_or_load_index_for(subfolder: str, index_dir: Path, force_rebuild: bool = False) -> FAISS:
    index_path = Path(index_dir)
    index_path.mkdir(parents=True, exist_ok=True)

    faiss_file = index_path / "index.faiss"
    pkl_file = index_path / "index.pkl"

    if not force_rebuild and faiss_file.exists() and pkl_file.exists():
        print(f"[index] Loading existing index for {subfolder} from {index_path}")
        return FAISS.load_local(
            str(index_path),
            _EMBEDDINGS,
            allow_dangerous_deserialization=True,
        )

    normalized = subfolder.strip().lower()
    if normalized in {"studyplans", "studyplan"}:
        parsed_folder = settings.studyplans_parsed
        category = "studyplans"
    elif normalized in {"reglementations", "regulations", "regulation"}:
        parsed_folder = settings.reglementations_parsed
        category = "reglementations"
    else:
        raise ValueError(f"Unknown subfolder: {subfolder}")

    parsed_paths = []
    for pattern in ("*.txt", "*.md", "*.text"):
        parsed_paths.extend(str(p) for p in parsed_folder.rglob(pattern))

    if not parsed_paths:
        raise FileNotFoundError(f"No parsed files found in {parsed_folder}")

    documents = _load_parsed_documents(parsed_paths, category=category)
    chunks = _SPLITTER.split_documents(documents)

    print(
        f"[index] Building FAISS for {category}: "
        f"parsed_folder={parsed_folder} docs={len(documents)} chunks={len(chunks)}"
    )
    db = FAISS.from_documents(chunks, _EMBEDDINGS)
    db.save_local(str(index_path))
    return db


def debug_chunk_file(path: str, category: str = "studyplans", limit: int = 10):
    docs = _load_parsed_file(path, category)
    chunks = _SPLITTER.split_documents(docs)

    print(f"pages={len(docs)} chunks={len(chunks)}")
    for i, ch in enumerate(chunks[:limit]):
        print("=" * 80)
        print(f"chunk {i}")
        print(ch.metadata)
        print(ch.page_content[:1200])


def answer_question(
    db: FAISS,
    question: str,
    k: int | None = None,
    language: str | None = None,
) -> Tuple[str, List[dict]]:
    final_k = k or settings.k or 8
    docs = _retrieve_docs(db, question, k=final_k)

    context_parts = []
    for d in docs:
        src = d.metadata.get("source", "document")
        page = d.metadata.get("page")
        cite = f"[{src} p.{page if isinstance(page, int) else '?'}]"
        context_parts.append(f"{cite}\n{d.page_content}")

    context = "\n\n".join(context_parts) if context_parts else "No relevant context found."

    llm = ChatOllama(
        model=settings.ollama_model,
        temperature=0,
        base_url=settings.ollama_host,
    )

    prompt = _build_prompt(language)
    msg = prompt.format_messages(question=question, context=context)
    resp = llm.invoke(msg)

    sources = []
    for d in docs:
        page = d.metadata.get("page")
        sources.append(
            {
                "source": d.metadata.get("source", "document"),
                "page": page if isinstance(page, int) else None,
                "snippet": (d.page_content[:350] + "…") if len(d.page_content) > 350 else d.page_content,
                "metadata": d.metadata,
                "source_type": d.metadata.get("source_type", "pdf"),
            }
        )

    return resp.content, sources


def debug_find_chunks_for_doc(db: FAISS, doc_key: str, contains: str | None = None, limit: int = 20):
    matches = []
    docs = _all_documents(db)

    for d in docs:
        if d.metadata.get("doc_key") == doc_key:
            if contains is None or contains.lower() in d.page_content.lower():
                matches.append(
                    {
                        "source": d.metadata.get("source"),
                        "page": d.metadata.get("page"),
                        "title": d.metadata.get("title"),
                        "program_name": d.metadata.get("program_name"),
                        "snippet": d.page_content[:800],
                    }
                )
            if len(matches) >= limit:
                break

    return matches


def debug_retrieve(db: FAISS, question: str, k: int = 10):
    expanded_query = _expand_query(question)
    matched_docs = _candidate_docs_from_metadata(db, question, min_score=4)
    matched_keys = _doc_key_set(matched_docs)
    retrieved = _retrieve_docs(db, question, k=k)

    return {
        "question": question,
        "expanded_query": expanded_query,
        "matched_doc_keys": sorted(matched_keys),
        "matched_titles": [
            {
                "title": d.metadata.get("title"),
                "program_name": d.metadata.get("program_name"),
                "degree_level": d.metadata.get("degree_level"),
                "source": d.metadata.get("source"),
                "page": d.metadata.get("page"),
            }
            for d in matched_docs[:10]
        ],
        "results": [
            {
                "source": d.metadata.get("source", "document"),
                "page": d.metadata.get("page"),
                "title": d.metadata.get("title"),
                "program_name": d.metadata.get("program_name"),
                "degree_level": d.metadata.get("degree_level"),
                "chunk_type": d.metadata.get("chunk_type"),
                "snippet": d.page_content[:350],
            }
            for d in retrieved
        ],
    }