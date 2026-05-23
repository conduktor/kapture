#!/usr/bin/env node
import { mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { marked } from "marked";

const here = dirname(fileURLToPath(import.meta.url));
const root = dirname(here);
const BLOG_DIR = join(root, "docs", "blog");
const SITE_ORIGIN = "https://kapturekafka.dev";

// ---------- frontmatter ----------

function parseFrontmatter(raw) {
  if (!raw.startsWith("---\n")) {
    throw new Error("missing frontmatter");
  }
  const end = raw.indexOf("\n---\n", 4);
  if (end === -1) throw new Error("unterminated frontmatter");
  const block = raw.slice(4, end);
  const body = raw.slice(end + 5);
  const meta = {};
  for (const line of block.split("\n")) {
    const m = /^([a-zA-Z_][a-zA-Z0-9_-]*):\s*(.*)$/.exec(line);
    if (!m) continue;
    const [, key, rawVal] = m;
    let val = rawVal.trim();
    if (val.startsWith("[") && val.endsWith("]")) {
      val = val
        .slice(1, -1)
        .split(",")
        .map((s) => s.trim().replace(/^["']|["']$/g, ""))
        .filter(Boolean);
    } else if (
      (val.startsWith('"') && val.endsWith('"')) ||
      (val.startsWith("'") && val.endsWith("'"))
    ) {
      val = val.slice(1, -1);
    }
    meta[key] = val;
  }
  return { meta, body };
}

// ---------- markdown preprocess ----------

function preprocess(body, posts) {
  const slugByFilename = new Map(posts.map((p) => [p.filename, p.meta.slug]));

  // Drop "> **Visual:** ..." blockquote lines so editorial notes don't leak to readers.
  // A visual block is a contiguous run of blockquote lines that starts with "> **Visual:**".
  const lines = body.split("\n");
  const out = [];
  let inVisual = false;
  for (const line of lines) {
    if (!inVisual && /^>\s*\*\*Visual:\*\*/.test(line)) {
      inVisual = true;
      continue;
    }
    if (inVisual) {
      if (line.startsWith(">") || line.trim() === "") {
        if (line.trim() === "") inVisual = false;
        continue;
      }
      inVisual = false;
    }
    out.push(line);
  }
  let processed = out.join("\n");

  // Rewrite intra-series links: ./02-foo.md → /blog/<slug>/
  processed = processed.replace(/\.\/(\d+-[a-z0-9-]+)\.md/g, (m, base) => {
    const slug = slugByFilename.get(base + ".md");
    return slug ? `/blog/${slug}/` : m;
  });

  // Drop the body's leading H1 — the page renders its own title in the hero.
  processed = processed.replace(/^\s*#\s+.+\n+/, "");

  // Drop the trailing "Next in this series" footer — the page renders its own next-card.
  processed = processed.replace(
    /\n+(?:---\s*\n+)?_Next in this series:[\s\S]*$/,
    "\n",
  );

  return processed.trim() + "\n";
}

function readingMinutes(body) {
  const words = body
    .replace(/```[\s\S]*?```/g, " ")
    .replace(/`[^`]*`/g, " ")
    .replace(/[#>*_-]/g, " ")
    .split(/\s+/)
    .filter(Boolean).length;
  return Math.max(1, Math.round(words / 200));
}

// ---------- HTML ----------

const escapeHtml = (s) =>
  String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");

const formatDate = (iso) => {
  const d = new Date(iso + "T00:00:00Z");
  return d.toLocaleDateString("en-US", {
    timeZone: "UTC",
    year: "numeric",
    month: "short",
    day: "numeric",
  });
};

const ANALYTICS = `<script async src="https://www.googletagmanager.com/gtag/js?id=G-F508MVXD3Z"></script>
    <script>
      window.dataLayer = window.dataLayer || [];
      function gtag() {
        dataLayer.push(arguments);
      }
      gtag("js", new Date());
      gtag("config", "G-F508MVXD3Z");
    </script>`;

const NAV_HTML = `<header class="nav" id="nav">
      <div class="wrap nav-inner">
        <a class="brand" href="/">
          <img src="/favicon.png" alt="" class="brand-icon" />
          <span class="brand-text">
            <span class="name">Kapture</span>
            <span class="by">by conduktor</span>
          </span>
        </a>
        <nav class="nav-links">
          <a href="/#why">Why</a>
          <a href="/#how">How it works</a>
          <a href="/#features">Features</a>
          <a href="/#faq">FAQ</a>
          <a href="/#install">Install</a>
          <a href="/blog/" class="current">Blog</a>
          <a class="nav-star" href="https://github.com/conduktor/kapture" target="_blank" rel="noopener" aria-label="Star Kapture on GitHub">
            <svg viewBox="0 0 16 16" fill="currentColor" aria-hidden="true"><path d="M8 .25a.75.75 0 0 1 .673.418l1.882 3.815 4.21.612a.75.75 0 0 1 .416 1.279l-3.046 2.97.719 4.192a.75.75 0 0 1-1.088.791L8 12.347l-3.766 1.98a.75.75 0 0 1-1.088-.79l.72-4.194L.818 6.374a.75.75 0 0 1 .416-1.28l4.21-.611L7.327.668A.75.75 0 0 1 8 .25Z"/></svg>
            <span>Star</span>
            <span class="count" id="gh-stars">—</span>
          </a>
          <a class="btn btn-primary" href="https://github.com/conduktor/kapture/releases/latest" target="_blank" rel="noopener">Download</a>
        </nav>
      </div>
    </header>`;

const FOOTER_HTML = `<footer>
      <div class="wrap foot">
        <div>
          <strong style="color: var(--text)">Kapture</strong> — built by the Conduktor team. Apache-2.0.
        </div>
        <div>
          <a href="https://github.com/conduktor/kapture" target="_blank" rel="noopener">GitHub</a>
          <a href="https://github.com/conduktor/kapture/issues" target="_blank" rel="noopener">Issues</a>
          <a href="https://github.com/conduktor/kapture/blob/main/CHANGELOG.md" target="_blank" rel="noopener">Changelog</a>
          <a href="mailto:kapture@conduktor.io">Feedback</a>
        </div>
      </div>
    </footer>`;

const NAV_SCRIPT = `<script>
      const nav = document.getElementById("nav");
      const onScroll = () => nav.classList.toggle("scrolled", window.scrollY > 4);
      document.addEventListener("scroll", onScroll, { passive: true });
      onScroll();
      fetch("https://api.github.com/repos/conduktor/kapture")
        .then((r) => (r.ok ? r.json() : null))
        .then((d) => {
          if (!d || d.stargazers_count == null) return;
          const n = d.stargazers_count;
          const fmt = n >= 1000 ? (n / 1000).toFixed(1).replace(/\\.0$/, "") + "k" : String(n);
          const el = document.getElementById("gh-stars");
          if (el) el.textContent = fmt;
        })
        .catch(() => {});
    </script>`;

const SHARED_STYLES = `:root {
        --bg: #ffffff;
        --bg-soft: #fafafa;
        --bg-code: #0b1020;
        --text: #0f172a;
        --muted: #64748b;
        --border: #e5e7eb;
        --border-strong: #d1d5db;
        --accent: #2563eb;
        --accent-hover: #1d4ed8;
        --accent-soft: #eff6ff;
        --shadow-sm: 0 1px 2px rgba(15, 23, 42, 0.04), 0 1px 1px rgba(15, 23, 42, 0.03);
        --shadow-md: 0 8px 24px -8px rgba(15, 23, 42, 0.12), 0 2px 6px rgba(15, 23, 42, 0.04);
        --radius: 10px;
        --radius-lg: 16px;
        --maxw: 880px;
        --font: -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
        --mono: ui-monospace, SFMono-Regular, "JetBrains Mono", Menlo, Consolas, monospace;
      }
      * { box-sizing: border-box; }
      html, body { margin: 0; padding: 0; }
      body {
        font-family: var(--font);
        color: var(--text);
        background: var(--bg);
        line-height: 1.6;
        -webkit-font-smoothing: antialiased;
        text-rendering: optimizeLegibility;
      }
      a { color: var(--accent); text-decoration: none; }
      a:hover { color: var(--accent-hover); }
      .wrap { max-width: var(--maxw); margin: 0 auto; padding: 0 24px; }

      header.nav {
        position: sticky;
        top: 0;
        z-index: 50;
        background: rgba(255, 255, 255, 0.78);
        backdrop-filter: saturate(160%) blur(10px);
        -webkit-backdrop-filter: saturate(160%) blur(10px);
        border-bottom: 1px solid transparent;
        transition: border-color 0.2s, background 0.2s;
      }
      header.nav.scrolled { border-color: var(--border); }
      .nav-inner {
        display: flex;
        align-items: center;
        justify-content: space-between;
        height: 64px;
        max-width: 1120px;
        margin: 0 auto;
        padding: 0 24px;
      }
      .brand { display: flex; align-items: center; gap: 10px; color: var(--text); font-weight: 700; }
      .brand-icon {
        width: 32px; height: 32px; border-radius: 8px;
        flex-shrink: 0; box-shadow: var(--shadow-sm);
      }
      .brand-text { display: flex; flex-direction: column; line-height: 1.05; }
      .brand-text .name { font-weight: 650; font-size: 15px; letter-spacing: -0.01em; }
      .brand-text .by { font-size: 10.5px; color: var(--muted); font-weight: 500; margin-top: 2px; letter-spacing: 0.01em; }
      .nav-star {
        display: inline-flex; align-items: center; gap: 7px;
        padding: 6px 10px 6px 12px;
        background: #fff;
        border: 1px solid var(--border-strong);
        border-radius: 999px;
        font-size: 13px; font-weight: 500;
        color: var(--text) !important;
        transition: border-color 0.15s, transform 0.15s, box-shadow 0.15s;
      }
      .nav-star:hover { border-color: var(--text); transform: translateY(-1px); box-shadow: var(--shadow-sm); }
      .nav-star svg { width: 14px; height: 14px; color: #eab308; transition: transform 0.2s; }
      .nav-star:hover svg { transform: rotate(15deg) scale(1.1); }
      .nav-star .count {
        padding: 1px 7px;
        background: var(--bg-soft);
        border: 1px solid var(--border);
        border-radius: 999px;
        font-size: 11.5px;
        color: var(--muted);
        font-variant-numeric: tabular-nums;
        min-width: 18px; text-align: center;
      }
      .nav-links { display: flex; align-items: center; gap: 22px; }
      .nav-links a { color: var(--muted); font-size: 14px; font-weight: 500; }
      .nav-links a:hover { color: var(--text); }
      .nav-links a.current { color: var(--text); }
      .btn {
        display: inline-flex; align-items: center; gap: 8px;
        height: 40px; padding: 0 16px;
        border-radius: var(--radius);
        font-size: 14px; font-weight: 550;
        border: 1px solid transparent;
        cursor: pointer;
        transition: background 0.15s, border-color 0.15s, transform 0.15s, box-shadow 0.15s;
      }
      .btn-primary, .nav-links a.btn-primary { background: var(--text); color: #fff; }
      .btn-primary:hover, .nav-links a.btn-primary:hover {
        background: #000; color: #fff;
        transform: translateY(-1px);
        box-shadow: var(--shadow-md);
      }
      @media (max-width: 720px) {
        .nav-links a:not(.btn):not(.nav-star) { display: none; }
        .nav-star .count { display: none; }
      }

      footer {
        border-top: 1px solid var(--border);
        padding: 40px 0;
        color: var(--muted);
        font-size: 14px;
      }
      .foot { display: flex; justify-content: space-between; gap: 24px; flex-wrap: wrap; max-width: 1120px; margin: 0 auto; padding: 0 24px; }
      .foot a { color: var(--muted); margin-right: 18px; }
      .foot a:hover { color: var(--text); }`;

const POST_STYLES = `${SHARED_STYLES}

      .post-hero {
        padding: 56px 0 28px;
      }
      .post-hero .back {
        display: inline-block;
        font-size: 13px;
        color: var(--muted);
        margin-bottom: 16px;
      }
      .post-hero .back:hover { color: var(--text); }
      .post-hero h1 {
        font-size: clamp(32px, 4.5vw, 44px);
        line-height: 1.15;
        letter-spacing: -0.02em;
        margin: 0 0 14px;
        font-weight: 700;
      }
      .post-hero .meta {
        font-size: 14px;
        color: var(--muted);
      }
      .post-hero .meta time {
        font-variant-numeric: tabular-nums;
      }
      .post-hero .meta .dot { margin: 0 8px; opacity: 0.5; }
      .post-hero .tags { margin-top: 16px; display: flex; flex-wrap: wrap; gap: 6px; }
      .post-hero .tags .tag {
        font-size: 12px;
        color: var(--muted);
        background: var(--bg-soft);
        border: 1px solid var(--border);
        border-radius: 6px;
        padding: 2px 8px;
        font-family: var(--mono);
      }

      main.post-main { padding: 8px 0 64px; border-top: 1px solid var(--border); margin-top: 16px; }
      .prose { font-size: 16.5px; }
      .prose h2 {
        font-size: 26px;
        font-weight: 650;
        letter-spacing: -0.015em;
        margin: 48px 0 12px;
        line-height: 1.25;
      }
      .prose h3 {
        font-size: 19px;
        font-weight: 600;
        margin: 32px 0 10px;
      }
      .prose p { margin: 0 0 18px; }
      .prose ul, .prose ol { margin: 0 0 18px; padding-left: 24px; }
      .prose li { margin-bottom: 6px; }
      .prose li > p { margin: 0 0 8px; }
      .prose blockquote {
        margin: 0 0 18px;
        padding: 12px 18px;
        border-left: 3px solid var(--border-strong);
        background: var(--bg-soft);
        color: var(--text);
        border-radius: 0 8px 8px 0;
      }
      .prose blockquote p:last-child { margin-bottom: 0; }
      .prose code {
        font-family: var(--mono);
        font-size: 0.88em;
        background: var(--bg-soft);
        border: 1px solid var(--border);
        border-radius: 5px;
        padding: 1px 6px;
        color: var(--text);
      }
      .prose pre {
        margin: 0 0 22px;
        padding: 16px 18px;
        background: var(--bg-code);
        color: #e2e8f0;
        border-radius: 10px;
        overflow-x: auto;
        font-family: var(--mono);
        font-size: 13px;
        line-height: 1.55;
      }
      .prose pre code {
        background: transparent;
        border: 0;
        padding: 0;
        color: inherit;
        font-size: inherit;
      }
      .prose hr {
        border: 0;
        border-top: 1px solid var(--border);
        margin: 36px 0;
      }
      .prose em { color: var(--muted); }
      .prose a { border-bottom: 1px solid transparent; transition: border-color 0.15s; }
      .prose a:hover { border-bottom-color: var(--accent); }
      .prose strong { font-weight: 650; }

      .post-nav-next {
        margin-top: 48px;
        padding: 20px 22px;
        border: 1px solid var(--border);
        border-radius: var(--radius-lg);
        background: var(--bg-soft);
      }
      .post-nav-next .label {
        font-size: 12px;
        font-weight: 600;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        color: var(--muted);
        margin-bottom: 6px;
      }
      .post-nav-next a { font-weight: 600; color: var(--text); }
      .post-nav-next a:hover { color: var(--accent); }
      .post-nav-next .next-desc { color: var(--muted); font-size: 14px; margin-top: 4px; }`;

const INDEX_STYLES = `${SHARED_STYLES}

      .hero {
        padding: 64px 0 32px;
        border-bottom: 1px solid var(--border);
      }
      .hero h1 {
        font-size: clamp(36px, 5vw, 46px);
        line-height: 1.15;
        margin: 0 0 14px;
        letter-spacing: -0.02em;
        font-weight: 700;
      }
      .hero p {
        margin: 0;
        font-size: 17px;
        color: var(--muted);
        max-width: 640px;
      }
      main.blog-main { padding: 32px 0 80px; }
      .posts {
        display: flex;
        flex-direction: column;
        gap: 0;
        list-style: none;
        margin: 0;
        padding: 0;
      }
      .post {
        display: grid;
        grid-template-columns: 56px 1fr;
        gap: 20px;
        padding: 26px 0;
        border-bottom: 1px solid var(--border);
      }
      .post:last-child { border-bottom: 0; }
      .post-number {
        font-family: var(--mono);
        font-size: 14px;
        color: var(--muted);
        font-weight: 500;
        padding-top: 2px;
      }
      .post-body h2 {
        font-size: 22px;
        margin: 0 0 6px;
        letter-spacing: -0.01em;
        font-weight: 650;
      }
      .post-body h2 a { color: var(--text); }
      .post-body h2 a:hover { color: var(--accent); }
      .post-meta {
        font-size: 13px;
        color: var(--muted);
        margin-bottom: 10px;
      }
      .post-meta time { font-variant-numeric: tabular-nums; }
      .post-meta .dot { margin: 0 8px; opacity: 0.5; }
      .post-body p { margin: 0 0 12px; color: var(--text); }
      .post-tags { display: flex; flex-wrap: wrap; gap: 6px; }
      .post-tags .tag {
        font-size: 12px;
        color: var(--muted);
        background: var(--bg-soft);
        border: 1px solid var(--border);
        border-radius: 6px;
        padding: 2px 8px;
        font-family: var(--mono);
      }
      @media (max-width: 600px) {
        .post { grid-template-columns: 1fr; gap: 6px; }
        .post-number { padding-top: 0; }
      }`;

function renderPostPage(post, posts) {
  const i = posts.indexOf(post);
  const next = posts[i + 1] || null;
  const html = marked.parse(post.processedBody, { mangle: false, headerIds: false });
  const tags = Array.isArray(post.meta.keywords) ? post.meta.keywords : [];
  const url = `${SITE_ORIGIN}/blog/${post.meta.slug}/`;
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    ${ANALYTICS}
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${escapeHtml(post.meta.title)} — Kapture blog</title>
    <meta name="description" content="${escapeHtml(post.meta.description)}" />
    <meta name="author" content="Conduktor" />
    <meta name="theme-color" content="#0f172a" />
    <meta name="color-scheme" content="light" />
    <link rel="canonical" href="${url}" />
    <link rel="icon" type="image/png" href="/favicon.png" />
    <link rel="apple-touch-icon" href="/apple-touch-icon.png" />
    <meta property="og:type" content="article" />
    <meta property="og:title" content="${escapeHtml(post.meta.title)}" />
    <meta property="og:description" content="${escapeHtml(post.meta.description)}" />
    <meta property="og:url" content="${url}" />
    <meta property="og:image" content="${SITE_ORIGIN}/images/og-card.png" />
    <meta property="article:published_time" content="${post.meta.date}" />
    <meta name="twitter:card" content="summary_large_image" />
    <meta name="twitter:title" content="${escapeHtml(post.meta.title)}" />
    <meta name="twitter:description" content="${escapeHtml(post.meta.description)}" />
    <meta name="twitter:image" content="${SITE_ORIGIN}/images/og-card.png" />
    <script type="application/ld+json">
${JSON.stringify(
  {
    "@context": "https://schema.org",
    "@type": "BlogPosting",
    headline: post.meta.title,
    description: post.meta.description,
    author: { "@type": "Organization", name: "Conduktor", url: "https://www.conduktor.io" },
    publisher: {
      "@type": "Organization",
      name: "Conduktor",
      url: "https://www.conduktor.io",
      logo: { "@type": "ImageObject", url: `${SITE_ORIGIN}/apple-touch-icon.png` },
    },
    datePublished: post.meta.date,
    dateModified: post.meta.date,
    mainEntityOfPage: { "@type": "WebPage", "@id": url },
    image: `${SITE_ORIGIN}/images/og-card.png`,
    keywords: tags.join(", "),
  },
  null,
  2,
)}
    </script>
    <style>${POST_STYLES}</style>
  </head>
  <body>
    ${NAV_HTML}

    <article>
      <section class="post-hero">
        <div class="wrap">
          <a class="back" href="/blog/">← Back to blog</a>
          <h1>${escapeHtml(post.meta.title)}</h1>
          <div class="meta">
            <time datetime="${post.meta.date}">${formatDate(post.meta.date)}</time>
            <span class="dot">·</span>
            <span>~${post.readingMin} min read</span>
          </div>
          ${
            tags.length
              ? `<div class="tags">${tags.map((t) => `<span class="tag">${escapeHtml(t)}</span>`).join("")}</div>`
              : ""
          }
        </div>
      </section>

      <main class="post-main">
        <div class="wrap prose">
${html}
${
  next
    ? `          <div class="post-nav-next">
            <div class="label">Next in this series</div>
            <a href="/blog/${next.meta.slug}/">${escapeHtml(next.meta.title)}</a>
            <div class="next-desc">${escapeHtml(next.meta.description)}</div>
          </div>`
    : ""
}
        </div>
      </main>
    </article>

    ${FOOTER_HTML}
    ${NAV_SCRIPT}
  </body>
</html>
`;
}

function renderIndexPage(posts) {
  const items = posts
    .map((p, i) => {
      const num = String(i + 1).padStart(2, "0");
      const tags = Array.isArray(p.meta.keywords) ? p.meta.keywords : [];
      return `          <li class="post">
            <div class="post-number">${num}</div>
            <div class="post-body">
              <h2><a href="/blog/${p.meta.slug}/">${escapeHtml(p.meta.title)}</a></h2>
              <div class="post-meta">
                <time datetime="${p.meta.date}">${formatDate(p.meta.date)}</time>
                <span class="dot">·</span>
                <span>~${p.readingMin} min read</span>
              </div>
              <p>${escapeHtml(p.meta.description)}</p>
              ${
                tags.length
                  ? `<div class="post-tags">${tags
                      .slice(0, 3)
                      .map((t) => `<span class="tag">${escapeHtml(t)}</span>`)
                      .join("")}</div>`
                  : ""
              }
            </div>
          </li>`;
    })
    .join("\n");

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    ${ANALYTICS}
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Kapture blog — deep dives on Kafka wire debugging</title>
    <meta name="description" content="Long-form deep dives on Kafka wire-level debugging, TLS visibility, and the design behind Kapture's tap mode." />
    <meta name="author" content="Conduktor" />
    <meta name="theme-color" content="#0f172a" />
    <meta name="color-scheme" content="light" />
    <link rel="canonical" href="${SITE_ORIGIN}/blog/" />
    <link rel="icon" type="image/png" href="/favicon.png" />
    <link rel="apple-touch-icon" href="/apple-touch-icon.png" />
    <meta property="og:type" content="website" />
    <meta property="og:title" content="Kapture blog" />
    <meta property="og:description" content="Deep dives on Kafka wire-level debugging, TLS visibility, and Kapture's tap mode." />
    <meta property="og:url" content="${SITE_ORIGIN}/blog/" />
    <meta property="og:image" content="${SITE_ORIGIN}/images/og-card.png" />
    <meta name="twitter:card" content="summary_large_image" />
    <meta name="twitter:title" content="Kapture blog" />
    <meta name="twitter:description" content="Deep dives on Kafka wire-level debugging, TLS visibility, and Kapture's tap mode." />
    <meta name="twitter:image" content="${SITE_ORIGIN}/images/og-card.png" />
    <style>${INDEX_STYLES}</style>
  </head>
  <body>
    ${NAV_HTML}

    <section class="hero">
      <div class="wrap">
        <h1>Notes from the wire</h1>
        <p>Long-form deep dives on Kafka wire-level debugging, TLS visibility, and the design behind Kapture's tap mode.</p>
      </div>
    </section>

    <main class="blog-main">
      <div class="wrap">
        <ol class="posts">
${items}
        </ol>
      </div>
    </main>

    ${FOOTER_HTML}
    ${NAV_SCRIPT}
  </body>
</html>
`;
}

// ---------- main ----------

function main() {
  const filenames = readdirSync(BLOG_DIR)
    .filter((f) => f.endsWith(".md"))
    .sort();

  const raw = filenames.map((filename) => {
    const text = readFileSync(join(BLOG_DIR, filename), "utf8");
    const { meta, body } = parseFrontmatter(text);
    return { filename, meta, body };
  });

  const posts = raw.map((p) => {
    const processedBody = preprocess(p.body, raw);
    const readingMin = readingMinutes(processedBody);
    return { ...p, processedBody, readingMin };
  });

  for (const post of posts) {
    const dir = join(BLOG_DIR, post.meta.slug);
    const out = join(dir, "index.html");
    mkdirSync(dir, { recursive: true });
    writeFileSync(out, renderPostPage(post, posts));
    console.log(`  → ${out}`);
  }

  const indexPath = join(BLOG_DIR, "index.html");
  writeFileSync(indexPath, renderIndexPage(posts));
  console.log(`  → ${indexPath}`);

  // sitemap entries
  const sitemapEntries = posts
    .map(
      (p) => `  <url>
    <loc>${SITE_ORIGIN}/blog/${p.meta.slug}/</loc>
    <lastmod>${p.meta.date}</lastmod>
    <priority>0.7</priority>
  </url>`,
    )
    .join("\n");
  console.log("\nSitemap additions:\n" + sitemapEntries);
}

main();
