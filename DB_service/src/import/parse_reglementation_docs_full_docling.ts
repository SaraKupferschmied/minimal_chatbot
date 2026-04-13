import "../environments/environment";

import fs from "fs";
import path from "path";
import crypto from "crypto";
import { spawnSync } from "child_process";
import * as pdfjsLib from "pdfjs-dist/legacy/build/pdf.mjs";

type ReglementationDocManifestItem = {
  reg_doc_key: string;
  tree: string;
  title: string;
  document_page_url: string;
  pdf_url: string | null;
  local_path: string | null;
  sha256: string | null;
  fetched_at: string | null;
  status: "downloaded" | "already_present" | "failed";
  notes?: string | null;
};

type ParsedRegDocIndexRow = {
  reg_doc_key: string;
  document_page_url: string;
  local_path: string;
  output_path: string;
  title: string | null;
  pages: number;
  sha256: string | null;
  parsed_at: string;
  parse_status: "ok" | "failed";
  parse_notes: string | null;
};

type ChunkType = "page" | "section" | "table" | "table_row";

type ParsedChunk = {
  chunk_id: string;
  reg_doc_key: string;
  document_page_url: string;
  local_path: string;
  sha256: string | null;
  title: string | null;
  tree: string | null;
  page: number;
  chunk_type: ChunkType;
  section: string | null;
  subsection: string | null;
  prev_chunk_id: string | null;
  next_chunk_id: string | null;
  parent_section_id: string | null;
  text: string;
};

type DoclingJson = {
  status: string;
  parser: string;
  title?: string | null;
  markdown: string;
  pages: string[];
  headings?: Array<{ level: number; text: string; line_index: number }>;
  tables?: Array<{
    table_index: number;
    markdown: string;
    rows?: Array<{ row_index: number; cells: Record<string, string> }>;
  }>;
};

function ensureDir(p: string) { fs.mkdirSync(p, { recursive: true }); }
function collapseWs(s: string) { return s.replace(/\s+/g, " ").trim(); }
function sha256String(s: string): string { return crypto.createHash("sha256").update(s, "utf-8").digest("hex"); }
function getArg(flag: string): string | null { const idx = process.argv.indexOf(flag); if (idx < 0) return null; const v = process.argv[idx + 1]; if (!v || v.startsWith("--")) return null; return v; }
function hasFlag(flag: string) { return process.argv.includes(flag); }
function compactContext(parts: Array<string | number | null | undefined>): string { return parts.map((x) => collapseWs(String(x ?? ""))).filter(Boolean).join(" | "); }
function chunkIdFor(docKey: string, page: number, type: ChunkType, text: string): string { return sha256String(`${docKey}|${page}|${type}|${text}`).slice(0, 24); }

async function pdfPageCount(filePath: string): Promise<number> {
  const data = new Uint8Array(fs.readFileSync(filePath));
  const loadingTask = pdfjsLib.getDocument({ data });
  const doc = await loadingTask.promise;
  return doc.numPages;
}

function parseWithDocling(pdfPath: string, helperPath: string, pythonExec: string): DoclingJson {
  const tempOut = path.join(process.cwd(), `.docling_${Date.now()}_${Math.random().toString(36).slice(2)}.json`);
  const proc = spawnSync(pythonExec, [helperPath, pdfPath, "--out", tempOut], { encoding: "utf-8", stdio: "pipe", maxBuffer: 20 * 1024 * 1024 });
  if (proc.status !== 0) throw new Error(`Docling helper failed: ${proc.stderr || proc.stdout || `exit=${proc.status}`}`);
  const raw = fs.readFileSync(tempOut, "utf-8");
  fs.unlinkSync(tempOut);
  return JSON.parse(raw) as DoclingJson;
}

function renderHeader(m: ReglementationDocManifestItem, inferredTitle: string | null, pages: number) {
  const headerObj = {
    parsed_at: new Date().toISOString(),
    title: inferredTitle,
    pages,
    parser: "docling",
    reg_doc_key: m.reg_doc_key,
    tree: m.tree,
    spider_title: m.title,
    document_page_url: m.document_page_url,
    pdf_url: m.pdf_url,
    local_path: m.local_path,
    sha256: m.sha256,
    fetched_at: m.fetched_at,
    notes: m.notes ?? null,
  };
  return `---METADATA_JSON---\n${JSON.stringify(headerObj, null, 2)}\n---/METADATA_JSON---\n\n`;
}

function buildRegChunks(m: ReglementationDocManifestItem, pdfPath: string, title: string | null, parsed: DoclingJson, totalPages: number): ParsedChunk[] {
  const chunks: ParsedChunk[] = [];
  const pages = parsed.pages?.length ? parsed.pages : [parsed.markdown];

  for (let i = 0; i < pages.length; i++) {
    const page = Math.min(i + 1, totalPages || pages.length);
    const text = collapseWs(pages[i] || "");
    if (!text) continue;
    const chunkText = [compactContext([title, `Seite ${page}`]), text].join("\n");
    chunks.push({ chunk_id: chunkIdFor(m.reg_doc_key, page, "page", chunkText), reg_doc_key: m.reg_doc_key, document_page_url: m.document_page_url, local_path: pdfPath, sha256: m.sha256, title, tree: m.tree, page, chunk_type: "page", section: null, subsection: null, prev_chunk_id: null, next_chunk_id: null, parent_section_id: null, text: chunkText });
  }

  let currentSectionId: string | null = null;
  for (const h of parsed.headings ?? []) {
    const headingText = collapseWs(h.text || "");
    if (!headingText) continue;
    const sectionText = [compactContext([title, headingText]), headingText].join("\n");
    const id = chunkIdFor(m.reg_doc_key, 1, "section", sectionText);
    currentSectionId = id;
    chunks.push({ chunk_id: id, reg_doc_key: m.reg_doc_key, document_page_url: m.document_page_url, local_path: pdfPath, sha256: m.sha256, title, tree: m.tree, page: 1, chunk_type: "section", section: headingText, subsection: null, prev_chunk_id: null, next_chunk_id: null, parent_section_id: null, text: sectionText });
  }

  for (const table of parsed.tables ?? []) {
    const tableText = [compactContext([title, `Tabelle ${table.table_index + 1}`]), table.markdown].join("\n");
    const tableId = chunkIdFor(m.reg_doc_key, 1, "table", tableText);
    chunks.push({ chunk_id: tableId, reg_doc_key: m.reg_doc_key, document_page_url: m.document_page_url, local_path: pdfPath, sha256: m.sha256, title, tree: m.tree, page: 1, chunk_type: "table", section: null, subsection: null, prev_chunk_id: null, next_chunk_id: null, parent_section_id: currentSectionId, text: tableText });
    for (const row of table.rows ?? []) {
      const cellsText = Object.entries(row.cells || {}).map(([k, v]) => `${k}: ${v}`).join(" | ");
      const rowText = [compactContext([title, `Tabelle ${table.table_index + 1}`, `Zeile ${row.row_index + 1}`]), cellsText].join("\n");
      chunks.push({ chunk_id: chunkIdFor(m.reg_doc_key, 1, "table_row", rowText), reg_doc_key: m.reg_doc_key, document_page_url: m.document_page_url, local_path: pdfPath, sha256: m.sha256, title, tree: m.tree, page: 1, chunk_type: "table_row", section: null, subsection: null, prev_chunk_id: null, next_chunk_id: null, parent_section_id: tableId, text: rowText });
    }
  }

  for (let i = 0; i < chunks.length; i++) {
    chunks[i].prev_chunk_id = i > 0 ? chunks[i - 1].chunk_id : null;
    chunks[i].next_chunk_id = i < chunks.length - 1 ? chunks[i + 1].chunk_id : null;
  }
  return chunks;
}

async function run() {
  const root = getArg("--root") ?? path.resolve(process.cwd(), "scrapy_crawler/outputs/reglementation_docs");
  const helperPath = getArg("--docling-helper") ?? path.resolve(process.cwd(), "parse_with_docling.py");
  const pythonExec = getArg("--python") ?? "python";
  const manifestPath = path.join(root, "_reglementation_docs_manifest.json");
  if (!fs.existsSync(manifestPath)) throw new Error(`Missing manifest at: ${manifestPath}`);

  const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf-8")) as ReglementationDocManifestItem[];
  const parsedDir = path.join(root, "parsed_fulltext_docling");
  const chunksDir = path.join(root, "parsed_chunks_docling");
  ensureDir(parsedDir); ensureDir(chunksDir);

  const indexPath = path.join(parsedDir, "_index.jsonl");
  const chunksIndexPath = path.join(chunksDir, "_chunks.jsonl");
  if (!hasFlag("--append-index") && fs.existsSync(indexPath)) fs.unlinkSync(indexPath);
  if (!hasFlag("--append-index") && fs.existsSync(chunksIndexPath)) fs.unlinkSync(chunksIndexPath);

  const okStatuses = new Set<ReglementationDocManifestItem["status"]>(["downloaded", "already_present"]);
  let ok = 0, fail = 0, chunkCount = 0;

  for (const m of manifest) {
    if (!okStatuses.has(m.status) || !m.local_path) continue;
    const pdfPath = path.join(root, "pdfs", path.basename(m.local_path));
    if (!fs.existsSync(pdfPath)) {
      fail++;
      const row: ParsedRegDocIndexRow = { reg_doc_key: m.reg_doc_key, document_page_url: m.document_page_url, local_path: pdfPath, output_path: "", title: null, pages: 0, sha256: m.sha256, parsed_at: new Date().toISOString(), parse_status: "failed", parse_notes: `PDF not found at ${pdfPath}` };
      fs.appendFileSync(indexPath, JSON.stringify(row) + "\n");
      continue;
    }

    try {
      const parsed = parseWithDocling(pdfPath, helperPath, pythonExec);
      const pages = await pdfPageCount(pdfPath);
      const title = collapseWs(parsed.title || "") || collapseWs(m.title || "") || null;
      const base = `${m.reg_doc_key}.txt`;
      const outputPath = path.join(parsedDir, base);
      const chunksPath = path.join(chunksDir, `${m.reg_doc_key}.jsonl`);
      const header = renderHeader(m, title, pages);
      const body = (parsed.pages?.length ? parsed.pages : [parsed.markdown]).map((t, i) => `---PAGE ${i + 1}---\n${t}\n`).join("\n");
      fs.writeFileSync(outputPath, header + body, "utf-8");

      const chunks = buildRegChunks(m, pdfPath, title, parsed, pages);
      fs.writeFileSync(chunksPath, chunks.map((c) => JSON.stringify(c)).join("\n") + "\n", "utf-8");
      fs.appendFileSync(chunksIndexPath, chunks.map((c) => JSON.stringify(c)).join("\n") + "\n");
      chunkCount += chunks.length;

      const row: ParsedRegDocIndexRow = { reg_doc_key: m.reg_doc_key, document_page_url: m.document_page_url, local_path: pdfPath, output_path: outputPath, title, pages, sha256: m.sha256, parsed_at: new Date().toISOString(), parse_status: "ok", parse_notes: null };
      fs.appendFileSync(indexPath, JSON.stringify(row) + "\n");
      ok++;
      console.log(`✅ Parsed ${path.basename(pdfPath)} -> ${path.basename(outputPath)} | ${path.basename(chunksPath)} (pages=${pages}, chunks=${chunks.length})`);
    } catch (e: any) {
      fail++;
      const row: ParsedRegDocIndexRow = { reg_doc_key: m.reg_doc_key, document_page_url: m.document_page_url, local_path: pdfPath, output_path: "", title: null, pages: 0, sha256: m.sha256, parsed_at: new Date().toISOString(), parse_status: "failed", parse_notes: e?.message ?? String(e) };
      fs.appendFileSync(indexPath, JSON.stringify(row) + "\n");
      console.warn(`⚠️ Failed parsing ${pdfPath}: ${e?.message ?? e}`);
    }
  }

  console.log(`\nDone. ok=${ok} failed=${fail} total_chunks=${chunkCount}`);
  console.log(`Parsed files: ${parsedDir}`);
  console.log(`Chunks dir: ${chunksDir}`);
}

run().catch((e) => { console.error("❌ Parse failed:", e); process.exit(1); });
