import "../environments/environment";

import fs from "fs";
import path from "path";
import crypto from "crypto";
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
  items: {
    x: number;
    y: number;
    text: string;
    width: number;
    height: number;
  }[];
};

type ChunkType = "page" | "section" | "course_row";

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

  text: string;
};

function ensureDir(p: string) {
  fs.mkdirSync(p, { recursive: true });
}

function safeFileName(s: string) {
  return s.replace(/[^a-z0-9._-]+/gi, "_").slice(0, 180);
}

function collapseWs(s: string) {
  return s.replace(/\s+/g, " ").trim();
}

function sha256String(s: string): string {
  return crypto.createHash("sha256").update(s, "utf-8").digest("hex");
}

function isJunkTitle(t: string) {
  const s = collapseWs(t).toLowerCase();
  if (!s) return true;
  if (s.length <= 5) return true;
  if (/^plan[_-]?[a-z0-9-]+$/i.test(s)) return true;
  if (/^(bsc|msc|ba|ma|phd|dr)\b/.test(s) && s.length <= 10) return true;
  if (/^untitled$/i.test(s)) return true;
  if (/microsoft word/i.test(s)) return true;
  return false;
}

function titleFromCoverText(page1: string): string | null {
  const t = collapseWs(page1);
  if (!t || t.length < 20) return null;

  const patterns: RegExp[] = [
    /(Studienplan[^.]{0,250}?\b(Bachelor|Master|Doctorate|Doktorat|Doktor)\b[^.]{0,250}?\bin\s+[A-Za-zÀ-ÿ0-9ÄÖÜäöüß \-\/]+)\b/i,
    /(Studienplan[^.]{0,250}?\b(Zusatzfach|Nebenfach|Minor)\b[^.]{0,250})\b/i,
    /(Studienplan[^.]{0,250}?\b(propädeut|propaedeut)\w*[^.]{0,250})\b/i,
    /(Plan d[’']études[^.]{0,250}?\b(Bachelor|Master|Doctorat)\b[^.]{0,250}?\ben\s+[A-Za-zÀ-ÿ0-9 \-\/]+)\b/i,
    /(Plan d[’']études[^.]{0,250}?\b(mineure|min[eé]or|minor)\b[^.]{0,250})\b/i,
    /((R[èe]glement|Reglement|Regulation)[^.]{0,300})\b/i,
  ];

  for (const rx of patterns) {
    const m = t.match(rx);
    if (m?.[1]) return collapseWs(m[1]).slice(0, 220);
  }

  const parts = t
    .split(/(?<=[.!?])\s+|\n+|\s{2,}/g)
    .map((x) => collapseWs(x))
    .filter((x) => x.length >= 20);

  if (!parts.length) return null;

  const score = (p: string) => {
    const s = p.toLowerCase();
    let k = 0;
    if (s.includes("studienplan") || s.includes("plan d")) k += 5;
    if (/(bachelor|master|doctor|doktor)/i.test(p)) k += 4;
    if (/(zusatzfach|nebenfach|minor|mineure)/i.test(p)) k += 3;
    if (/(reglement|règlement|regulation)/i.test(p)) k += 3;
    if (/(informatik|computer|science|médecine|medizin|physik|mathematik|biologie|wirtschaft)/i.test(p)) k += 1;
    if (p.length >= 40 && p.length <= 220) k += 2;
    return k;
  };

  parts.sort((a, b) => score(b) - score(a));
  return parts[0].slice(0, 220);
}

function titleFromSourceUrl(url: string): string | null {
  try {
    const u = new URL(url);
    const base = path.basename(u.pathname);
    const cleaned = base.replace(/\.pdf$/i, "").replace(/[_-]+/g, " ").trim();
    return cleaned || null;
  } catch {
    try {
      const base = path.basename(url);
      const cleaned = base.replace(/\.pdf$/i, "").replace(/[_-]+/g, " ").trim();
      return cleaned || null;
    } catch {
      return null;
    }
  }
}

function getArg(flag: string): string | null {
  const idx = process.argv.indexOf(flag);
  if (idx < 0) return null;
  const v = process.argv[idx + 1];
  if (!v || v.startsWith("--")) return null;
  return v;
}

function hasFlag(flag: string) {
  return process.argv.includes(flag);
}

function normalizeLine(s: string): string {
  return collapseWs(
    s
      .replace(/[ \t]+/g, " ")
      .replace(/\u00a0/g, " ")
      .replace(/\s+([,.;:!?])/g, "$1")
  );
}

function itemX(it: TextItemLike): number {
  return Array.isArray(it.transform) ? Number(it.transform[4] ?? 0) : 0;
}

function itemY(it: TextItemLike): number {
  return Array.isArray(it.transform) ? Number(it.transform[5] ?? 0) : 0;
}

function itemHeight(it: TextItemLike): number {
  const h = Number(it.height ?? 0);
  return Number.isFinite(h) && h > 0 ? h : 10;
}

function itemWidth(it: TextItemLike): number {
  const w = Number(it.width ?? 0);
  return Number.isFinite(w) && w > 0 ? w : Math.max(4, String(it.str ?? "").length * 4);
}

function groupItemsIntoLines(items: TextItemLike[], pageNumber: number): PageLine[] {
  const filtered = items
    .map((it) => ({
      x: itemX(it),
      y: itemY(it),
      width: itemWidth(it),
      height: itemHeight(it),
      text: String(it.str ?? "").trim(),
    }))
    .filter((it) => it.text.length > 0);

  if (!filtered.length) return [];

  filtered.sort((a, b) => {
    const dy = Math.abs(a.y - b.y);
    if (dy > 2) return b.y - a.y; // PDF coordinates: higher y usually visually higher
    return a.x - b.x;
  });

  const lineBuckets: {
    y: number;
    items: typeof filtered;
  }[] = [];

  for (const it of filtered) {
    let found = false;
    for (const bucket of lineBuckets) {
      const tolerance = Math.max(2.5, Math.min(6, it.height * 0.5));
      if (Math.abs(bucket.y - it.y) <= tolerance) {
        bucket.items.push(it);
        bucket.y = (bucket.y * (bucket.items.length - 1) + it.y) / bucket.items.length;
        found = true;
        break;
      }
    }
    if (!found) {
      lineBuckets.push({ y: it.y, items: [it] });
    }
  }

  const lines: PageLine[] = lineBuckets
    .map((bucket) => {
      const lineItems = [...bucket.items].sort((a, b) => a.x - b.x);

      const parts: string[] = [];
      let prevRight: number | null = null;

      for (const it of lineItems) {
        if (prevRight !== null) {
          const gap = it.x - prevRight;
          if (gap > 14) parts.push("   ");
          else if (gap > 2) parts.push(" ");
        }
        parts.push(it.text);
        prevRight = it.x + it.width;
      }

      return {
        page: pageNumber,
        y: bucket.y,
        text: normalizeLine(parts.join("")),
        items: lineItems.map((it) => ({
          x: it.x,
          y: it.y,
          text: it.text,
          width: it.width,
          height: it.height,
        })),
      };
    })
    .filter((line) => line.text.length > 0)
    .sort((a, b) => b.y - a.y);

  return lines;
}

function isLikelyYearHeading(line: string): boolean {
  const s = line.trim();
  return (
    /^\d+\.\s*(Jahr|Studienjahr)\b/i.test(s) ||
    /^\d+[.]\s*année\b/i.test(s) ||
    /\b(1\. Jahr|2\. Studienjahr|3\. Studienjahr)\b/i.test(s) ||
    /\b60\s*ECTS\b/i.test(s)
  );
}

function isLikelySubsectionHeading(line: string): boolean {
  const s = line.trim();
  return (
    /^Pflichtkurse\b/i.test(s) ||
    /^Wahlkurse\b/i.test(s) ||
    /^Bachelorarbeit\b/i.test(s) ||
    /^Informationskompetenz/i.test(s) ||
    /^Compétences documentaires/i.test(s) ||
    /^Wahlkurse\s*-\s*Typus\s*[AB]\b/i.test(s)
  );
}

function isLikelyCourseRow(line: string): boolean {
  const s = collapseWs(line);
  return /^[A-Z]{3}\.\d{5}\b/.test(s);
}

function compactContext(parts: Array<string | null | undefined>): string {
  return parts.map((x) => collapseWs(x ?? "")).filter(Boolean).join(" | ");
}

function chunkIdFor(docKey: string, page: number, type: ChunkType, text: string): string {
  return sha256String(`${docKey}|${page}|${type}|${text}`).slice(0, 24);
}

function renderHeader(m: ProgramDocManifestItem, title: string | null, pages: number) {
  const headerObj = {
    parsed_at: new Date().toISOString(),
    title,
    pages,
    doc_key: m.doc_key,
    program_key: m.program_key,
    faculty: m.faculty,
    degree_level: m.degree_level,
    total_ects: m.total_ects,
    program_name: m.program_name,
    doc_label: m.doc_label,
    source_url: m.source_url,
    source_type: m.source_type,
    programme_url: m.programme_url,
    curriculum_url: m.curriculum_url,
    local_path: m.local_path,
    sha256: m.sha256,
    fetched_at: m.fetched_at,
    notes: m.notes ?? null,
  };

  return `---METADATA_JSON---\n${JSON.stringify(headerObj, null, 2)}\n---/METADATA_JSON---\n\n`;
}

async function extractPdfStructured(
  filePath: string,
  sourceUrl?: string
): Promise<{
  title: string | null;
  pagesText: string[];
  pageLines: PageLine[][];
}> {
  const data = new Uint8Array(fs.readFileSync(filePath));
  const loadingTask = pdfjsLib.getDocument({ data });
  const doc = await loadingTask.promise;

  let metaTitle: string | null = null;
  try {
    const meta = await doc.getMetadata();
    const infoAny = (meta as any)?.info ?? {};
    const pdfTitle = (infoAny.Title ?? "") as string;
    const dcTitle = (meta as any)?.metadata?.get?.("dc:title") ?? "";
    const t = String(pdfTitle || dcTitle).trim();
    if (t && !isJunkTitle(t)) metaTitle = collapseWs(t);
  } catch {
    // ignore
  }

  const pagesText: string[] = [];
  const pageLines: PageLine[][] = [];

  for (let pageNo = 1; pageNo <= doc.numPages; pageNo++) {
    const page = await doc.getPage(pageNo);
    const content = await page.getTextContent();
    const items = (content.items ?? []) as TextItemLike[];
    const lines = groupItemsIntoLines(items, pageNo);

    const pageText = lines.map((l) => l.text).join("\n");
    pagesText.push(pageText);
    pageLines.push(lines);
  }

  const coverTitle = titleFromCoverText(pagesText[0] ?? "");

  let title: string | null = null;
  if (coverTitle) title = coverTitle;
  else if (metaTitle) title = metaTitle;
  else if (sourceUrl) title = titleFromSourceUrl(sourceUrl);

  return { title, pagesText, pageLines };
}

function buildChunks(
  manifest: ProgramDocManifestItem,
  pdfPath: string,
  title: string | null,
  pageLines: PageLine[][]
): ParsedChunk[] {
  const chunks: ParsedChunk[] = [];

  for (let pageIdx = 0; pageIdx < pageLines.length; pageIdx++) {
    const page = pageIdx + 1;
    const lines = pageLines[pageIdx];
    const pageText = lines.map((l) => l.text).join("\n").trim();

    if (pageText) {
      const pageChunkText = [
        compactContext([
          manifest.degree_level,
          manifest.program_name,
          manifest.total_ects ? `${manifest.total_ects} ECTS` : null,
          `Seite ${page}`,
        ]),
        pageText,
      ]
        .filter(Boolean)
        .join("\n");

      chunks.push({
        chunk_id: chunkIdFor(manifest.doc_key, page, "page", pageChunkText),
        doc_key: manifest.doc_key,
        program_key: manifest.program_key,
        source_url: manifest.source_url,
        local_path: pdfPath,
        sha256: manifest.sha256,

        title,
        faculty: manifest.faculty,
        degree_level: manifest.degree_level,
        total_ects: manifest.total_ects,
        program_name: manifest.program_name,
        doc_label: manifest.doc_label,
        source_type: manifest.source_type,

        page,
        chunk_type: "page",

        section: null,
        subsection: null,

        prev_chunk_id: null,
        next_chunk_id: null,
        parent_section_id: null,

        text: pageChunkText,
      });
    }

    let currentSection: string | null = null;
    let currentSubsection: string | null = null;
    let currentSectionChunkId: string | null = null;

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].text;
      if (!line) continue;

      if (isLikelyYearHeading(line)) {
        currentSection = line;
        currentSubsection = null;

        const windowLines = lines
          .slice(i, Math.min(i + 8, lines.length))
          .map((x) => x.text)
          .filter(Boolean);

        const sectionText = [
          compactContext([
            manifest.degree_level,
            manifest.program_name,
            manifest.total_ects ? `${manifest.total_ects} ECTS` : null,
            currentSection,
          ]),
          windowLines.join("\n"),
        ]
          .filter(Boolean)
          .join("\n");

        const sectionChunkId = chunkIdFor(manifest.doc_key, page, "section", sectionText);
        currentSectionChunkId = sectionChunkId;

        chunks.push({
          chunk_id: sectionChunkId,
          doc_key: manifest.doc_key,
          program_key: manifest.program_key,
          source_url: manifest.source_url,
          local_path: pdfPath,
          sha256: manifest.sha256,

          title,
          faculty: manifest.faculty,
          degree_level: manifest.degree_level,
          total_ects: manifest.total_ects,
          program_name: manifest.program_name,
          doc_label: manifest.doc_label,
          source_type: manifest.source_type,

          page,
          chunk_type: "section",

          section: currentSection,
          subsection: null,

          prev_chunk_id: null,
          next_chunk_id: null,
          parent_section_id: null,

          text: sectionText,
        });

        continue;
      }

      if (isLikelySubsectionHeading(line)) {
        currentSubsection = line;

        const windowLines = lines
          .slice(i, Math.min(i + 8, lines.length))
          .map((x) => x.text)
          .filter(Boolean);

        const subsectionText = [
          compactContext([
            manifest.degree_level,
            manifest.program_name,
            manifest.total_ects ? `${manifest.total_ects} ECTS` : null,
            currentSection,
            currentSubsection,
          ]),
          windowLines.join("\n"),
        ]
          .filter(Boolean)
          .join("\n");

        const subSectionChunkId = chunkIdFor(manifest.doc_key, page, "section", subsectionText);

        chunks.push({
          chunk_id: subSectionChunkId,
          doc_key: manifest.doc_key,
          program_key: manifest.program_key,
          source_url: manifest.source_url,
          local_path: pdfPath,
          sha256: manifest.sha256,

          title,
          faculty: manifest.faculty,
          degree_level: manifest.degree_level,
          total_ects: manifest.total_ects,
          program_name: manifest.program_name,
          doc_label: manifest.doc_label,
          source_type: manifest.source_type,

          page,
          chunk_type: "section",

          section: currentSection,
          subsection: currentSubsection,

          prev_chunk_id: null,
          next_chunk_id: null,
          parent_section_id: currentSectionChunkId,

          text: subsectionText,
        });

        continue;
      }

      if (isLikelyCourseRow(line)) {
        const contextualText = compactContext([
          manifest.degree_level,
          manifest.program_name,
          manifest.total_ects ? `${manifest.total_ects} ECTS` : null,
          currentSection,
          currentSubsection,
          line,
        ]);

        chunks.push({
          chunk_id: chunkIdFor(manifest.doc_key, page, "course_row", contextualText),
          doc_key: manifest.doc_key,
          program_key: manifest.program_key,
          source_url: manifest.source_url,
          local_path: pdfPath,
          sha256: manifest.sha256,

          title,
          faculty: manifest.faculty,
          degree_level: manifest.degree_level,
          total_ects: manifest.total_ects,
          program_name: manifest.program_name,
          doc_label: manifest.doc_label,
          source_type: manifest.source_type,

          page,
          chunk_type: "course_row",

          section: currentSection,
          subsection: currentSubsection,

          prev_chunk_id: null,
          next_chunk_id: null,
          parent_section_id: currentSectionChunkId,

          text: contextualText,
        });
      }
    }
  }

  for (let i = 0; i < chunks.length; i++) {
    chunks[i].prev_chunk_id = i > 0 ? chunks[i - 1].chunk_id : null;
    chunks[i].next_chunk_id = i < chunks.length - 1 ? chunks[i + 1].chunk_id : null;
  }

  return chunks;
}

async function run() {
  const outRoot = getArg("--root") ?? path.resolve(process.cwd(), "scrapy_crawler/outputs");

  const manifestPath = path.join(outRoot, "_program_docs_manifest.json");
  if (!fs.existsSync(manifestPath)) {
    throw new Error(`Missing manifest at: ${manifestPath}\nPass --root <folderContainingManifest>`);
  }

  const manifest: ProgramDocManifestItem[] = JSON.parse(fs.readFileSync(manifestPath, "utf-8"));

  const parsedDir = path.join(outRoot, "parsed_fulltext");
  const chunksDir = path.join(outRoot, "parsed_chunks");
  ensureDir(parsedDir);
  ensureDir(chunksDir);

  const indexPath = path.join(parsedDir, "_index.jsonl");
  const chunksIndexPath = path.join(chunksDir, "_chunks.jsonl");

  if (!hasFlag("--append-index") && fs.existsSync(indexPath)) {
    fs.unlinkSync(indexPath);
  }
  if (!hasFlag("--append-index") && fs.existsSync(chunksIndexPath)) {
    fs.unlinkSync(chunksIndexPath);
  }

  const okStatuses = new Set(["downloaded", "already_present"]);

  let ok = 0;
  let fail = 0;
  let chunkCount = 0;

  for (const m of manifest) {
    if (!okStatuses.has(m.status)) continue;
    if (!m.local_path) continue;

    const pdfPath = path.join(outRoot, "pdfs", path.basename(m.local_path));

    if (!fs.existsSync(pdfPath)) {
      fail++;
      const row: ParsedDocIndexRow = {
        doc_key: m.doc_key,
        program_key: m.program_key,
        source_url: m.source_url,
        local_path: pdfPath,
        output_path: "",
        chunks_path: "",
        title: null,
        pages: 0,
        sha256: m.sha256,
        parsed_at: new Date().toISOString(),
        parse_status: "failed",
        parse_notes: `PDF not found at ${pdfPath}`,
      };
      fs.appendFileSync(indexPath, JSON.stringify(row) + "\n");
      console.warn(`⚠️ Missing PDF: ${pdfPath}`);
      continue;
    }

    try {
      const { title, pagesText, pageLines } = await extractPdfStructured(pdfPath, m.source_url);

      const baseName = safeFileName(
        `${m.degree_level ?? "UNK"}_${m.total_ects ?? "UNK"}_${m.doc_key}`
      );

      const outputPath = path.join(parsedDir, `${baseName}.txt`);
      const chunksPath = path.join(chunksDir, `${baseName}.jsonl`);

      const header = renderHeader(m, title, pagesText.length);

      const body = pagesText.map((t, i) => `---PAGE ${i + 1}---\n${t}\n`).join("\n");
      fs.writeFileSync(outputPath, header + body, "utf-8");

      const chunks = buildChunks(m, pdfPath, title, pageLines);
      fs.writeFileSync(chunksPath, chunks.map((c) => JSON.stringify(c)).join("\n") + "\n", "utf-8");
      fs.appendFileSync(chunksIndexPath, chunks.map((c) => JSON.stringify(c)).join("\n") + "\n");

      chunkCount += chunks.length;

      const row: ParsedDocIndexRow = {
        doc_key: m.doc_key,
        program_key: m.program_key,
        source_url: m.source_url,
        local_path: pdfPath,
        output_path: outputPath,
        chunks_path: chunksPath,
        title,
        pages: pagesText.length,
        sha256: m.sha256,
        parsed_at: new Date().toISOString(),
        parse_status: "ok",
        parse_notes: null,
      };
      fs.appendFileSync(indexPath, JSON.stringify(row) + "\n");

      ok++;
      console.log(
        `✅ Parsed ${path.basename(pdfPath)} -> ${path.basename(outputPath)} | ${path.basename(chunksPath)} (pages=${pagesText.length}, chunks=${chunks.length})`
      );
    } catch (e: any) {
      fail++;
      const row: ParsedDocIndexRow = {
        doc_key: m.doc_key,
        program_key: m.program_key,
        source_url: m.source_url,
        local_path: pdfPath,
        output_path: "",
        chunks_path: "",
        title: null,
        pages: 0,
        sha256: m.sha256,
        parsed_at: new Date().toISOString(),
        parse_status: "failed",
        parse_notes: e?.message ?? String(e),
      };
      fs.appendFileSync(indexPath, JSON.stringify(row) + "\n");
      console.warn(`⚠️ Failed parsing ${pdfPath}: ${e?.message ?? e}`);
    }
  }

  console.log(`\nDone. ok=${ok} failed=${fail} total_chunks=${chunkCount}`);
  console.log(`Parsed files: ${parsedDir}`);
  console.log(`Chunks dir: ${chunksDir}`);
  console.log(`Doc index: ${indexPath}`);
  console.log(`Chunk index: ${chunksIndexPath}`);
}

run().catch((e) => {
  console.error("❌ Parse failed:", e);
  process.exit(1);
});