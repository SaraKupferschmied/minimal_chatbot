import "../environments/environment";

import fs from "fs";
import path from "path";
import crypto from "crypto";
import { spawnSync } from "child_process";
import * as pdfjsLib from "pdfjs-dist/legacy/build/pdf.mjs";

type ProgramDocManifestItem = {
  program_key: string;
  doc_key: string;
  faculty: string | null;
  degree_level: "Bachelor" | "Master" | "Doctorate" | null;
  total_ects: number | null;
  program_name: string | null;
  programme_url: string | null;
  curriculum_url: string | null;
  doc_label: string | null;
  source_url: string;
  source_type: "pdf" | "calameo" | "unknown";
  local_path: string | null;
  sha256: string | null;
  fetched_at: string | null;
  status: "downloaded" | "already_present" | "skipped_non_pdf" | "calameo_no_direct_pdf" | "failed";
  notes?: string | null;
};

type ParsedDocIndexRow = {
  doc_key: string;
  program_key: string;
  source_url: string;
  local_path: string;
  output_path: string;
  chunks_path: string;
  title: string | null;
  pages: number;
  sha256: string | null;
  parsed_at: string;
  parse_status: "ok" | "failed";
  parse_notes: string | null;
};

type TextItemLike = {
  str?: string;
  transform?: number[];
  width?: number;
  height?: number;
};

type PageLine = {
  page: number;
  y: number;
  text: string;
  items: { x: number; y: number; text: string; width: number; height: number }[];
};

type ChunkType = "page" | "section" | "course_row" | "table" | "table_row";

type ParsedChunk = {
  chunk_id: string;
  doc_key: string;
  program_key: string;
  source_url: string;
  local_path: string;
  sha256: string | null;
  title: string | null;
  faculty: string | null;
  degree_level: string | null;
  total_ects: number | null;
  program_name: string | null;
  doc_label: string | null;
  source_type: string;
  page: number;
  chunk_type: ChunkType;
  section: string | null;
  subsection: string | null;
  prev_chunk_id: string | null;
  next_chunk_id: string | null;
  parent_section_id: string | null;
  parser: "pdfjs" | "llamaparse";
  text: string;
};

type LlamaParsePage = {
  page: number;
  markdown: string;
  text: string;
  tables?: string[];
};

type LlamaParseOutput = {
  title?: string | null;
  pages: LlamaParsePage[];
  parser: "llama-parse";
  pdf_path: string;
};

function ensureDir(p: string) { fs.mkdirSync(p, { recursive: true }); }
function safeFileName(s: string) { return s.replace(/[^a-z0-9._-]+/gi, "_").slice(0, 180); }
function collapseWs(s: string) { return s.replace(/\s+/g, " ").trim(); }
function sha256String(s: string): string { return crypto.createHash("sha256").update(s, "utf-8").digest("hex"); }
function getArg(flag: string): string | null { const i = process.argv.indexOf(flag); return i >= 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith("--") ? process.argv[i + 1] : null; }
function hasFlag(flag: string) { return process.argv.includes(flag); }
function compactContext(parts: Array<string | number | null | undefined>): string { return parts.map((x) => collapseWs(String(x ?? ""))).filter(Boolean).join(" | "); }
function chunkIdFor(docKey: string, page: number, type: ChunkType, text: string): string { return sha256String(`${docKey}|${page}|${type}|${text}`).slice(0, 24); }

function buildParsingInstructions(m: ProgramDocManifestItem): string {
  const bits = [
    `This PDF is a University of Fribourg study document for ${m.program_name ?? "an academic program"}.`,
    `Degree level: ${m.degree_level ?? "unknown"}.`,
    m.total_ects ? `Nominal size: ${m.total_ects} ECTS.` : null,
    `The document may contain bilingual French/German text, tables, study-year blocks, course lists, ECTS values, explanatory sidebars, and brochure-style layouts.`,
    `Preserve structure faithfully as markdown.`,
    `Do not merge columns into one sentence if they are visually separate.`,
    `Represent tables as markdown tables whenever possible.`,
    `Keep course names, year headings, ECTS values, and notes in reading order within each visual block.`,
    `If a page contains sidebars or callouts such as access to master, further info, mobility, language, or professional perspectives, keep them as separate markdown sections instead of blending them into the study plan table.`,
    `Output text only from the PDF; do not invent missing values.`
  ].filter(Boolean);
  return bits.join(" ");
}

function renderHeader(m: ProgramDocManifestItem, title: string | null, pages: number, parser: string) {
  const headerObj = { parsed_at: new Date().toISOString(), title, pages, parser, doc_key: m.doc_key, program_key: m.program_key, faculty: m.faculty, degree_level: m.degree_level, total_ects: m.total_ects, program_name: m.program_name, doc_label: m.doc_label, source_url: m.source_url, source_type: m.source_type, programme_url: m.programme_url, curriculum_url: m.curriculum_url, local_path: m.local_path, sha256: m.sha256, fetched_at: m.fetched_at, notes: m.notes ?? null };
  return `---METADATA_JSON---\n${JSON.stringify(headerObj, null, 2)}\n---/METADATA_JSON---\n\n`;
}

function parseMarkdownSections(markdown: string): { section: string | null; subsection: string | null; body: string }[] {
  const lines = (markdown || "").split(/\r?\n/);
  const out: { section: string | null; subsection: string | null; body: string }[] = [];
  let currentSection: string | null = null;
  let currentSubsection: string | null = null;
  let buf: string[] = [];

  const flush = () => {
    const body = buf.join("\n").trim();
    if (body) out.push({ section: currentSection, subsection: currentSubsection, body });
    buf = [];
  };

  for (const raw of lines) {
    const line = raw.trimEnd();
    const heading = line.match(/^(#{1,6})\s+(.+?)\s*$/);
    if (heading) {
      flush();
      if (heading[1].length <= 2) { currentSection = heading[2].trim(); currentSubsection = null; }
      else { currentSubsection = heading[2].trim(); }
      continue;
    }
    if (/^[-]{3,}$/.test(line.trim())) continue;
    buf.push(line);
  }
  flush();
  return out;
}

function splitMarkdownTables(markdown: string): string[] {
  const tables: string[] = [];
  const lines = (markdown || "").split(/\r?\n/);
  let buf: string[] = [];
  for (const raw of lines) {
    const line = raw.trimEnd();
    if (line.includes("|")) buf.push(line);
    else {
      if (buf.length >= 2) tables.push(buf.join("\n").trim());
      buf = [];
    }
  }
  if (buf.length >= 2) tables.push(buf.join("\n").trim());
  return tables;
}

function parseMarkdownTableRows(tableMd: string): string[] {
  return tableMd.split(/\r?\n/).map((x) => x.trim()).filter((x) => /^\|.+\|$/.test(x) && !/^\|?\s*[-: ]+\|/.test(x));
}

function buildChunksFromLlamaParse(manifest: ProgramDocManifestItem, pdfPath: string, title: string | null, pages: LlamaParsePage[]): ParsedChunk[] {
  const chunks: ParsedChunk[] = [];
  for (const p of pages) {
    const pageText = [compactContext([manifest.degree_level, manifest.program_name, manifest.total_ects ? `${manifest.total_ects} ECTS` : null, `Seite ${p.page}`]), p.text].filter(Boolean).join("\n");
    if (collapseWs(pageText)) {
      chunks.push({ chunk_id: chunkIdFor(manifest.doc_key, p.page, "page", pageText), doc_key: manifest.doc_key, program_key: manifest.program_key, source_url: manifest.source_url, local_path: pdfPath, sha256: manifest.sha256, title, faculty: manifest.faculty, degree_level: manifest.degree_level, total_ects: manifest.total_ects, program_name: manifest.program_name, doc_label: manifest.doc_label, source_type: manifest.source_type, page: p.page, chunk_type: "page", section: null, subsection: null, prev_chunk_id: null, next_chunk_id: null, parent_section_id: null, parser: "llamaparse", text: pageText });
    }

    let lastSectionChunkId: string | null = null;
    for (const sec of parseMarkdownSections(p.markdown)) {
      const text = [compactContext([manifest.degree_level, manifest.program_name, manifest.total_ects ? `${manifest.total_ects} ECTS` : null, sec.section, sec.subsection]), sec.body].filter(Boolean).join("\n");
      if (!collapseWs(text)) continue;
      const chunkId = chunkIdFor(manifest.doc_key, p.page, "section", text);
      if (sec.section && !sec.subsection) lastSectionChunkId = chunkId;
      chunks.push({ chunk_id: chunkId, doc_key: manifest.doc_key, program_key: manifest.program_key, source_url: manifest.source_url, local_path: pdfPath, sha256: manifest.sha256, title, faculty: manifest.faculty, degree_level: manifest.degree_level, total_ects: manifest.total_ects, program_name: manifest.program_name, doc_label: manifest.doc_label, source_type: manifest.source_type, page: p.page, chunk_type: "section", section: sec.section, subsection: sec.subsection, prev_chunk_id: null, next_chunk_id: null, parent_section_id: sec.subsection ? lastSectionChunkId : null, parser: "llamaparse", text });
    }

    const tables = (p.tables && p.tables.length ? p.tables : splitMarkdownTables(p.markdown));
    for (const tableMd of tables) {
      const tableText = [compactContext([manifest.degree_level, manifest.program_name, manifest.total_ects ? `${manifest.total_ects} ECTS` : null, `Table page ${p.page}`]), tableMd].join("\n");
      const tableChunkId = chunkIdFor(manifest.doc_key, p.page, "table", tableText);
      chunks.push({ chunk_id: tableChunkId, doc_key: manifest.doc_key, program_key: manifest.program_key, source_url: manifest.source_url, local_path: pdfPath, sha256: manifest.sha256, title, faculty: manifest.faculty, degree_level: manifest.degree_level, total_ects: manifest.total_ects, program_name: manifest.program_name, doc_label: manifest.doc_label, source_type: manifest.source_type, page: p.page, chunk_type: "table", section: null, subsection: null, prev_chunk_id: null, next_chunk_id: null, parent_section_id: null, parser: "llamaparse", text: tableText });

      for (const row of parseMarkdownTableRows(tableMd)) {
        const rowText = [compactContext([manifest.degree_level, manifest.program_name, manifest.total_ects ? `${manifest.total_ects} ECTS` : null, `Table row page ${p.page}`]), row.replace(/\|/g, " ")].join("\n");
        chunks.push({ chunk_id: chunkIdFor(manifest.doc_key, p.page, "table_row", rowText), doc_key: manifest.doc_key, program_key: manifest.program_key, source_url: manifest.source_url, local_path: pdfPath, sha256: manifest.sha256, title, faculty: manifest.faculty, degree_level: manifest.degree_level, total_ects: manifest.total_ects, program_name: manifest.program_name, doc_label: manifest.doc_label, source_type: manifest.source_type, page: p.page, chunk_type: "table_row", section: null, subsection: null, prev_chunk_id: null, next_chunk_id: null, parent_section_id: tableChunkId, parser: "llamaparse", text: collapseWs(rowText) });
      }
    }
  }

  for (let i = 0; i < chunks.length; i++) {
    chunks[i].prev_chunk_id = i > 0 ? chunks[i - 1].chunk_id : null;
    chunks[i].next_chunk_id = i < chunks.length - 1 ? chunks[i + 1].chunk_id : null;
  }
  return chunks;
}

function normalizeLine(s: string): string { return collapseWs(s.replace(/[ \t]+/g, " ").replace(/\u00a0/g, " ").replace(/\s+([,.;:!?])/g, "$1")); }
function itemX(it: TextItemLike): number { return Array.isArray(it.transform) ? Number(it.transform[4] ?? 0) : 0; }
function itemY(it: TextItemLike): number { return Array.isArray(it.transform) ? Number(it.transform[5] ?? 0) : 0; }
function itemHeight(it: TextItemLike): number { const h = Number(it.height ?? 0); return Number.isFinite(h) && h > 0 ? h : 10; }
function itemWidth(it: TextItemLike): number { const w = Number(it.width ?? 0); return Number.isFinite(w) && w > 0 ? w : Math.max(4, String(it.str ?? "").length * 4); }

function groupItemsIntoLines(items: TextItemLike[], pageNumber: number): PageLine[] {
  const filtered = items.map((it) => ({ x: itemX(it), y: itemY(it), width: itemWidth(it), height: itemHeight(it), text: String(it.str ?? "").trim() })).filter((it) => it.text.length > 0);
  filtered.sort((a, b) => { const dy = Math.abs(a.y - b.y); if (dy > 2) return b.y - a.y; return a.x - b.x; });
  const buckets: { y: number; items: typeof filtered }[] = [];
  for (const it of filtered) {
    let found = false;
    for (const bucket of buckets) {
      const tol = Math.max(2.5, Math.min(6, it.height * 0.5));
      if (Math.abs(bucket.y - it.y) <= tol) { bucket.items.push(it); bucket.y = (bucket.y * (bucket.items.length - 1) + it.y) / bucket.items.length; found = true; break; }
    }
    if (!found) buckets.push({ y: it.y, items: [it] });
  }
  return buckets.map((bucket) => {
    const lineItems = [...bucket.items].sort((a, b) => a.x - b.x);
    const parts: string[] = [];
    let prevRight: number | null = null;
    for (const it of lineItems) {
      if (prevRight !== null) { const gap = it.x - prevRight; if (gap > 14) parts.push("   "); else if (gap > 2) parts.push(" "); }
      parts.push(it.text); prevRight = it.x + it.width;
    }
    return { page: pageNumber, y: bucket.y, text: normalizeLine(parts.join("")), items: lineItems };
  }).filter((line) => line.text.length > 0).sort((a, b) => b.y - a.y);
}

async function extractPdfStructuredPdfJs(filePath: string, sourceUrl?: string): Promise<{ title: string | null; pagesText: string[]; pageLines: PageLine[][] }> {
  const data = new Uint8Array(fs.readFileSync(filePath));
  const loadingTask = pdfjsLib.getDocument({ data });
  const doc = await loadingTask.promise;
  const pagesText: string[] = [];
  const pageLines: PageLine[][] = [];
  for (let pageNo = 1; pageNo <= doc.numPages; pageNo++) {
    const page = await doc.getPage(pageNo);
    const content = await page.getTextContent();
    const lines = groupItemsIntoLines((content.items ?? []) as TextItemLike[], pageNo);
    pagesText.push(lines.map((l) => l.text).join("\n"));
    pageLines.push(lines);
  }
  const title = sourceUrl ? path.basename(sourceUrl).replace(/\.pdf$/i, "") : null;
  return { title, pagesText, pageLines };
}

function buildChunksPdfJs(manifest: ProgramDocManifestItem, pdfPath: string, title: string | null, pageLines: PageLine[][]): ParsedChunk[] {
  const chunks: ParsedChunk[] = [];
  for (let pageIdx = 0; pageIdx < pageLines.length; pageIdx++) {
    const page = pageIdx + 1;
    const lines = pageLines[pageIdx];
    const pageText = lines.map((l) => l.text).join("\n").trim();
    if (pageText) chunks.push({ chunk_id: chunkIdFor(manifest.doc_key, page, "page", pageText), doc_key: manifest.doc_key, program_key: manifest.program_key, source_url: manifest.source_url, local_path: pdfPath, sha256: manifest.sha256, title, faculty: manifest.faculty, degree_level: manifest.degree_level, total_ects: manifest.total_ects, program_name: manifest.program_name, doc_label: manifest.doc_label, source_type: manifest.source_type, page, chunk_type: "page", section: null, subsection: null, prev_chunk_id: null, next_chunk_id: null, parent_section_id: null, parser: "pdfjs", text: pageText });
  }
  for (let i = 0; i < chunks.length; i++) { chunks[i].prev_chunk_id = i > 0 ? chunks[i - 1].chunk_id : null; chunks[i].next_chunk_id = i < chunks.length - 1 ? chunks[i + 1].chunk_id : null; }
  return chunks;
}

function runLlamaParse(pdfPath: string, m: ProgramDocManifestItem): LlamaParseOutput {
  const helper = getArg("--llamaparse-helper") ?? path.resolve(process.cwd(), "parse_with_llamaparse.py");
  const tmpOut = path.join(process.cwd(), `.llamaparse_${m.doc_key}.json`);
  const args = [helper, "--pdf", pdfPath, "--out", tmpOut, "--instructions", buildParsingInstructions(m), "--language", "de"];
  const res = spawnSync("python", args, { encoding: "utf-8", stdio: "pipe", env: process.env });
  if (res.status !== 0) throw new Error(`LlamaParse failed for ${pdfPath}: ${res.stderr || res.stdout}`);
  const parsed = JSON.parse(fs.readFileSync(tmpOut, "utf-8")) as LlamaParseOutput;
  fs.unlinkSync(tmpOut);
  return parsed;
}

async function run() {
  const outRoot = getArg("--root") ?? path.resolve(process.cwd(), "scrapy_crawler/outputs");
  const parserMode = (getArg("--parser") ?? "pdfjs") as "pdfjs" | "llamaparse";
  const manifestPath = path.join(outRoot, "_program_docs_manifest.json");
  if (!fs.existsSync(manifestPath)) throw new Error(`Missing manifest at: ${manifestPath}`);
  const manifest: ProgramDocManifestItem[] = JSON.parse(fs.readFileSync(manifestPath, "utf-8"));
  const parsedDir = path.join(outRoot, parserMode === "llamaparse" ? "parsed_fulltext_llamaparse" : "parsed_fulltext");
  const chunksDir = path.join(outRoot, parserMode === "llamaparse" ? "parsed_chunks_llamaparse" : "parsed_chunks");
  ensureDir(parsedDir); ensureDir(chunksDir);
  const indexPath = path.join(parsedDir, "_index.jsonl");
  const chunksIndexPath = path.join(chunksDir, "_chunks.jsonl");
  if (!hasFlag("--append-index") && fs.existsSync(indexPath)) fs.unlinkSync(indexPath);
  if (!hasFlag("--append-index") && fs.existsSync(chunksIndexPath)) fs.unlinkSync(chunksIndexPath);

  let ok = 0; let fail = 0; let chunkCount = 0;
  const okStatuses = new Set(["downloaded", "already_present"]);
  for (const m of manifest) {
    if (!okStatuses.has(m.status) || !m.local_path) continue;
    const pdfPath = path.join(outRoot, "pdfs", path.basename(m.local_path));
    if (!fs.existsSync(pdfPath)) { fail++; continue; }
    try {
      let title: string | null = null;
      let pagesText: string[] = [];
      let chunks: ParsedChunk[] = [];
      if (parserMode === "llamaparse") {
        const parsed = runLlamaParse(pdfPath, m);
        title = parsed.title ?? null;
        pagesText = parsed.pages.map((p) => p.markdown);
        chunks = buildChunksFromLlamaParse(m, pdfPath, title, parsed.pages);
      } else {
        const parsed = await extractPdfStructuredPdfJs(pdfPath, m.source_url);
        title = parsed.title;
        pagesText = parsed.pagesText;
        chunks = buildChunksPdfJs(m, pdfPath, title, parsed.pageLines);
      }

      const baseName = safeFileName(`${m.degree_level ?? "UNK"}_${m.total_ects ?? "UNK"}_${m.doc_key}`);
      const outputPath = path.join(parsedDir, `${baseName}.txt`);
      const chunksPath = path.join(chunksDir, `${baseName}.jsonl`);
      fs.writeFileSync(outputPath, renderHeader(m, title, pagesText.length, parserMode) + pagesText.map((t, i) => `---PAGE ${i + 1}---\n${t}\n`).join("\n"), "utf-8");
      fs.writeFileSync(chunksPath, chunks.map((c) => JSON.stringify(c)).join("\n") + "\n", "utf-8");
      fs.appendFileSync(chunksIndexPath, chunks.map((c) => JSON.stringify(c)).join("\n") + "\n");
      const row: ParsedDocIndexRow = { doc_key: m.doc_key, program_key: m.program_key, source_url: m.source_url, local_path: pdfPath, output_path: outputPath, chunks_path: chunksPath, title, pages: pagesText.length, sha256: m.sha256, parsed_at: new Date().toISOString(), parse_status: "ok", parse_notes: null };
      fs.appendFileSync(indexPath, JSON.stringify(row) + "\n");
      ok++; chunkCount += chunks.length;
      console.log(`✅ Parsed ${path.basename(pdfPath)} with ${parserMode} -> ${path.basename(chunksPath)} (chunks=${chunks.length})`);
    } catch (e: any) {
      fail++;
      const row: ParsedDocIndexRow = { doc_key: m.doc_key, program_key: m.program_key, source_url: m.source_url, local_path: pdfPath, output_path: "", chunks_path: "", title: null, pages: 0, sha256: m.sha256, parsed_at: new Date().toISOString(), parse_status: "failed", parse_notes: e?.message ?? String(e) };
      fs.appendFileSync(indexPath, JSON.stringify(row) + "\n");
      console.warn(`⚠️ Failed parsing ${pdfPath}: ${e?.message ?? e}`);
    }
  }

  console.log(`\nDone. parser=${parserMode} ok=${ok} failed=${fail} total_chunks=${chunkCount}`);
}

run().catch((e) => { console.error("❌ Parse failed:", e); process.exit(1); });
