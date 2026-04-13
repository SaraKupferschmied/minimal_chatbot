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

type ParsedChunk = {
  chunk_id: string;
  reg_doc_key: string;
  title: string | null;
  document_page_url: string;
  local_path: string;
  sha256: string | null;
  page: number;
  chunk_type: "page" | "section" | "table";
  section: string | null;
  subsection: string | null;
  prev_chunk_id: string | null;
  next_chunk_id: string | null;
  parent_section_id: string | null;
  parser: "pdfjs" | "llamaparse";
  text: string;
};

type LlamaParsePage = { page: number; markdown: string; text: string; tables?: string[] };

type LlamaParseOutput = { title?: string | null; pages: LlamaParsePage[]; parser: "llama-parse"; pdf_path: string };

function ensureDir(p: string) { fs.mkdirSync(p, { recursive: true }); }
function collapseWs(s: string) { return s.replace(/\s+/g, " ").trim(); }
function sha256String(s: string): string { return crypto.createHash("sha256").update(s, "utf-8").digest("hex"); }
function getArg(flag: string): string | null { const i = process.argv.indexOf(flag); return i >= 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith("--") ? process.argv[i + 1] : null; }
function hasFlag(flag: string) { return process.argv.includes(flag); }
function chunkIdFor(docKey: string, page: number, type: string, text: string): string { return sha256String(`${docKey}|${page}|${type}|${text}`).slice(0, 24); }

function buildParsingInstructions(m: ReglementationDocManifestItem): string {
  return [
    `This PDF is a regulation document from the University of Fribourg.`,
    `Preserve bilingual French/German legal structure.`,
    `Keep article numbers, headings, enumerations, ECTS values, and amendment tables.`,
    `Represent legal sections and tables as markdown.`,
    `Do not merge the left and right bilingual columns into one corrupted sentence if the page is laid out in two columns.`
  ].join(" ");
}

function renderHeader(m: ReglementationDocManifestItem, inferredTitle: string | null, pages: number, parser: string) {
  const headerObj = { parsed_at: new Date().toISOString(), title: inferredTitle, pages, parser, reg_doc_key: m.reg_doc_key, tree: m.tree, spider_title: m.title, document_page_url: m.document_page_url, pdf_url: m.pdf_url, local_path: m.local_path, sha256: m.sha256, fetched_at: m.fetched_at, notes: m.notes ?? null };
  return `---METADATA_JSON---\n${JSON.stringify(headerObj, null, 2)}\n---/METADATA_JSON---\n\n`;
}

function parseMarkdownSections(markdown: string): { section: string | null; subsection: string | null; body: string }[] {
  const lines = (markdown || "").split(/\r?\n/);
  const out: { section: string | null; subsection: string | null; body: string }[] = [];
  let currentSection: string | null = null; let currentSubsection: string | null = null; let buf: string[] = [];
  const flush = () => { const body = buf.join("\n").trim(); if (body) out.push({ section: currentSection, subsection: currentSubsection, body }); buf = []; };
  for (const raw of lines) {
    const line = raw.trimEnd();
    const heading = line.match(/^(#{1,6})\s+(.+?)\s*$/);
    if (heading) { flush(); if (heading[1].length <= 2) { currentSection = heading[2].trim(); currentSubsection = null; } else { currentSubsection = heading[2].trim(); } continue; }
    buf.push(line);
  }
  flush();
  return out;
}

function splitMarkdownTables(markdown: string): string[] {
  const tables: string[] = [];
  let buf: string[] = [];
  for (const raw of (markdown || "").split(/\r?\n/)) {
    const line = raw.trimEnd();
    if (line.includes("|")) buf.push(line);
    else { if (buf.length >= 2) tables.push(buf.join("\n").trim()); buf = []; }
  }
  if (buf.length >= 2) tables.push(buf.join("\n").trim());
  return tables;
}

async function extractPdfText(filePath: string): Promise<string[]> {
  const data = new Uint8Array(fs.readFileSync(filePath));
  const loadingTask = pdfjsLib.getDocument({ data });
  const doc = await loadingTask.promise;
  const pagesText: string[] = [];
  for (let pageNo = 1; pageNo <= doc.numPages; pageNo++) {
    const page = await doc.getPage(pageNo);
    const content = await page.getTextContent();
    const strings = content.items.map((it: any) => (it?.str ?? "").toString());
    pagesText.push(collapseWs(strings.join(" ")));
  }
  return pagesText;
}

function buildChunksFromLlamaParse(m: ReglementationDocManifestItem, pdfPath: string, title: string | null, pages: LlamaParsePage[]): ParsedChunk[] {
  const chunks: ParsedChunk[] = [];
  for (const p of pages) {
    if (collapseWs(p.text)) chunks.push({ chunk_id: chunkIdFor(m.reg_doc_key, p.page, "page", p.text), reg_doc_key: m.reg_doc_key, title, document_page_url: m.document_page_url, local_path: pdfPath, sha256: m.sha256, page: p.page, chunk_type: "page", section: null, subsection: null, prev_chunk_id: null, next_chunk_id: null, parent_section_id: null, parser: "llamaparse", text: p.text });
    let currentTopId: string | null = null;
    for (const sec of parseMarkdownSections(p.markdown)) {
      const id = chunkIdFor(m.reg_doc_key, p.page, "section", sec.body);
      if (sec.section && !sec.subsection) currentTopId = id;
      chunks.push({ chunk_id: id, reg_doc_key: m.reg_doc_key, title, document_page_url: m.document_page_url, local_path: pdfPath, sha256: m.sha256, page: p.page, chunk_type: "section", section: sec.section, subsection: sec.subsection, prev_chunk_id: null, next_chunk_id: null, parent_section_id: sec.subsection ? currentTopId : null, parser: "llamaparse", text: sec.body });
    }
    for (const tableMd of (p.tables && p.tables.length ? p.tables : splitMarkdownTables(p.markdown))) {
      chunks.push({ chunk_id: chunkIdFor(m.reg_doc_key, p.page, "table", tableMd), reg_doc_key: m.reg_doc_key, title, document_page_url: m.document_page_url, local_path: pdfPath, sha256: m.sha256, page: p.page, chunk_type: "table", section: null, subsection: null, prev_chunk_id: null, next_chunk_id: null, parent_section_id: null, parser: "llamaparse", text: tableMd });
    }
  }
  for (let i = 0; i < chunks.length; i++) { chunks[i].prev_chunk_id = i > 0 ? chunks[i - 1].chunk_id : null; chunks[i].next_chunk_id = i < chunks.length - 1 ? chunks[i + 1].chunk_id : null; }
  return chunks;
}

function runLlamaParse(pdfPath: string, m: ReglementationDocManifestItem): LlamaParseOutput {
  const helper = getArg("--llamaparse-helper") ?? path.resolve(process.cwd(), "parse_with_llamaparse.py");
  const tmpOut = path.join(process.cwd(), `.llamaparse_${m.reg_doc_key}.json`);
  const res = spawnSync("python", [helper, "--pdf", pdfPath, "--out", tmpOut, "--instructions", buildParsingInstructions(m), "--language", "de"], { encoding: "utf-8", stdio: "pipe", env: process.env });
  if (res.status !== 0) throw new Error(`LlamaParse failed for ${pdfPath}: ${res.stderr || res.stdout}`);
  const parsed = JSON.parse(fs.readFileSync(tmpOut, "utf-8")) as LlamaParseOutput;
  fs.unlinkSync(tmpOut);
  return parsed;
}

async function run() {
  const root = getArg("--root") ?? path.resolve(process.cwd(), "scrapy_crawler/outputs/reglementation_docs");
  const parserMode = (getArg("--parser") ?? "pdfjs") as "pdfjs" | "llamaparse";
  const manifestPath = path.join(root, "_reglementation_docs_manifest.json");
  if (!fs.existsSync(manifestPath)) throw new Error(`Missing manifest at: ${manifestPath}`);
  const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf-8")) as ReglementationDocManifestItem[];

  const parsedDir = path.join(root, parserMode === "llamaparse" ? "parsed_fulltext_llamaparse" : "parsed_fulltext_shorttitles");
  const chunksDir = path.join(root, parserMode === "llamaparse" ? "parsed_chunks_llamaparse" : "parsed_chunks_regulations");
  ensureDir(parsedDir); ensureDir(chunksDir);
  const indexPath = path.join(parsedDir, "_index.jsonl");
  const chunksIndexPath = path.join(chunksDir, "_chunks.jsonl");
  if (!hasFlag("--append-index") && fs.existsSync(indexPath)) fs.unlinkSync(indexPath);
  if (!hasFlag("--append-index") && fs.existsSync(chunksIndexPath)) fs.unlinkSync(chunksIndexPath);

  let ok = 0; let fail = 0;
  const okStatuses = new Set<ReglementationDocManifestItem["status"]>(["downloaded", "already_present"]);
  for (const m of manifest) {
    if (!okStatuses.has(m.status) || !m.local_path) continue;
    const pdfPath = path.join(root, "pdfs", path.basename(m.local_path));
    if (!fs.existsSync(pdfPath)) { fail++; continue; }
    try {
      let title: string | null = null;
      let pagesText: string[] = [];
      let chunks: ParsedChunk[] = [];
      if (parserMode === "llamaparse") {
        const parsed = runLlamaParse(pdfPath, m);
        title = parsed.title ?? m.title ?? null;
        pagesText = parsed.pages.map((p) => p.markdown);
        chunks = buildChunksFromLlamaParse(m, pdfPath, title, parsed.pages);
      } else {
        pagesText = await extractPdfText(pdfPath);
        title = m.title ?? null;
        chunks = pagesText.map((text, i) => ({ chunk_id: chunkIdFor(m.reg_doc_key, i + 1, "page", text), reg_doc_key: m.reg_doc_key, title, document_page_url: m.document_page_url, local_path: pdfPath, sha256: m.sha256, page: i + 1, chunk_type: "page", section: null, subsection: null, prev_chunk_id: null, next_chunk_id: null, parent_section_id: null, parser: "pdfjs", text }));
      }
      for (let i = 0; i < chunks.length; i++) { chunks[i].prev_chunk_id = i > 0 ? chunks[i - 1].chunk_id : null; chunks[i].next_chunk_id = i < chunks.length - 1 ? chunks[i + 1].chunk_id : null; }
      const outputPath = path.join(parsedDir, `${m.reg_doc_key}.txt`);
      const chunksPath = path.join(chunksDir, `${m.reg_doc_key}.jsonl`);
      fs.writeFileSync(outputPath, renderHeader(m, title, pagesText.length, parserMode) + pagesText.map((t, i) => `---PAGE ${i + 1}---\n${t}\n`).join("\n"), "utf-8");
      fs.writeFileSync(chunksPath, chunks.map((c) => JSON.stringify(c)).join("\n") + "\n", "utf-8");
      fs.appendFileSync(chunksIndexPath, chunks.map((c) => JSON.stringify(c)).join("\n") + "\n");
      const row: ParsedRegDocIndexRow = { reg_doc_key: m.reg_doc_key, document_page_url: m.document_page_url, local_path: pdfPath, output_path: outputPath, title, pages: pagesText.length, sha256: m.sha256, parsed_at: new Date().toISOString(), parse_status: "ok", parse_notes: null };
      fs.appendFileSync(indexPath, JSON.stringify(row) + "\n");
      ok++;
      console.log(`✅ Parsed ${path.basename(pdfPath)} with ${parserMode}`);
    } catch (e: any) {
      fail++;
      const row: ParsedRegDocIndexRow = { reg_doc_key: m.reg_doc_key, document_page_url: m.document_page_url, local_path: pdfPath, output_path: "", title: null, pages: 0, sha256: m.sha256, parsed_at: new Date().toISOString(), parse_status: "failed", parse_notes: e?.message ?? String(e) };
      fs.appendFileSync(indexPath, JSON.stringify(row) + "\n");
      console.warn(`⚠️ Failed parsing ${pdfPath}: ${e?.message ?? e}`);
    }
  }
  console.log(`Done. parser=${parserMode} ok=${ok} failed=${fail}`);
}

run().catch((e) => { console.error("❌ Parse failed:", e); process.exit(1); });
