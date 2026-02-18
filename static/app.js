let all = [];

const els = {
  q: document.getElementById("q"),
  sort: document.getElementById("sort"),
  showHidden: document.getElementById("showHidden"),
  refresh: document.getElementById("refresh"),
  status: document.getElementById("status"),
  count: document.getElementById("count"),
  grid: document.getElementById("grid"),
};

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, c => ({
    "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"
  }[c]));
}

async function load() {
  els.status.textContent = "Chargement…";
  els.grid.innerHTML = "";
  try {
    const res = await fetch("/api/achievements", { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    all = await res.json();
    els.status.textContent = "OK";
    render();
  } catch (e) {
    els.status.textContent = "Erreur: " + e.message;
  }
}

function applyFilters(items) {
  const q = els.q.value.trim().toLowerCase();
  const showHidden = els.showHidden.checked;

  let out = items.filter(a => showHidden || !a.hidden);

  if (q) {
    out = out.filter(a => {
      const hay = `${a.name} ${a.description} ${a.apiName}`.toLowerCase();
      return hay.includes(q);
    });
  }

  const mode = els.sort.value;
  out.sort((a, b) => {
    if (mode === "pct_desc") return (b.globalPct ?? 0) - (a.globalPct ?? 0) || a.name.localeCompare(b.name);
    if (mode === "pct_asc")  return (a.globalPct ?? 0) - (b.globalPct ?? 0) || a.name.localeCompare(b.name);
    if (mode === "name_desc") return b.name.localeCompare(a.name);
    return a.name.localeCompare(b.name);
  });

  return out;
}

function render() {
  const items = applyFilters(all);
  els.count.textContent = `${items.length} / ${all.length}`;

  els.grid.innerHTML = items.map(a => {
    const pct = (a.globalPct ?? 0).toFixed(2) + "%";
    const badge = a.hidden ? `<span class="badge">hidden</span>` : "";
    const desc = a.description?.trim() ? esc(a.description) : "<em class='muted'>Pas de description (souvent le cas sur certains hidden)</em>";

    return `
      <article class="card">
        <div class="iconWrap">
          <img class="icon" src="${esc(a.icon || a.iconGray || "")}" alt="" loading="lazy" />
        </div>
        <div class="body">
          <div class="topline">
            <h3 class="title">${esc(a.name || a.apiName)}</h3>
            ${badge}
          </div>
          <p class="desc">${desc}</p>
          <div class="foot">
            <code class="api">${esc(a.apiName)}</code>
            <span class="pct">${pct}</span>
          </div>
        </div>
      </article>
    `;
  }).join("");
}

["input", "change"].forEach(evt => {
  els.q.addEventListener(evt, render);
  els.sort.addEventListener(evt, render);
  els.showHidden.addEventListener(evt, render);
});
els.refresh.addEventListener("click", load);

load();