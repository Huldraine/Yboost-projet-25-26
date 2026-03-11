let currentSteamID = "";
let allGames = [];
let allAchievements = [];
let currentGameName = "";

const els = {
  steamForm: document.getElementById("steamForm"),
  steamId: document.getElementById("steamId"),
  status: document.getElementById("status"),
  count: document.getElementById("count"),
  errorBox: document.getElementById("errorBox"),

  gamesControls: document.getElementById("gamesControls"),
  gameQ: document.getElementById("gameQ"),
  gameSort: document.getElementById("gameSort"),
  refreshGames: document.getElementById("refreshGames"),
  gamesGrid: document.getElementById("gamesGrid"),

  achievementControls: document.getElementById("achievementControls"),
  achQ: document.getElementById("achQ"),
  achSort: document.getElementById("achSort"),
  showLocked: document.getElementById("showLocked"),
  backToGames: document.getElementById("backToGames"),
  achievementsGrid: document.getElementById("achievementsGrid"),
};

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, c => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  }[c]));
}

function setError(message) {
  if (!message) {
    els.errorBox.textContent = "";
    els.errorBox.classList.add("hidden");
    return;
  }
  els.errorBox.textContent = message;
  els.errorBox.classList.remove("hidden");
}

function isValidSteamID64(v) {
  return /^\d{17}$/.test(v);
}

async function getJSON(url) {
  const res = await fetch(url, { cache: "no-store" });
  const body = await res.json().catch(() => null);
  if (!res.ok) {
    const details = body?.details || `HTTP ${res.status}`;
    throw new Error(details);
  }
  return body;
}

function showGamesView() {
  els.gamesControls.classList.remove("hidden");
  els.gamesGrid.classList.remove("hidden");
  els.achievementControls.classList.add("hidden");
  els.achievementsGrid.classList.add("hidden");
}

function showAchievementsView() {
  els.gamesControls.classList.add("hidden");
  els.gamesGrid.classList.add("hidden");
  els.achievementControls.classList.remove("hidden");
  els.achievementsGrid.classList.remove("hidden");
}

async function loadGames(forceRefresh = false) {
  if (!isValidSteamID64(currentSteamID)) {
    setError("SteamID64 invalide (17 chiffres attendus)");
    return;
  }

  setError("");
  els.status.textContent = "Chargement des jeux...";
  els.count.textContent = "";
  if (forceRefresh) {
    // Bust browser cache only; backend cache still controls Steam sync frequency.
    currentSteamID = currentSteamID.trim();
  }

  try {
    const games = await getJSON(`/api/users/games?steamId=${encodeURIComponent(currentSteamID)}`);
    allGames = Array.isArray(games) ? games : [];
    renderGames();
    els.status.textContent = "Jeux charges";
  } catch (err) {
    els.status.textContent = "Erreur";
    setError(err.message || "Erreur inconnue");
    allGames = [];
    renderGames();
  }
}

async function loadAchievements(appId, gameName) {
  setError("");
  els.status.textContent = "Chargement des achievements...";
  els.count.textContent = "";

  try {
    const rows = await getJSON(`/api/users/achievements?steamId=${encodeURIComponent(currentSteamID)}&appId=${encodeURIComponent(appId)}`);
    allAchievements = Array.isArray(rows) ? rows : [];
    currentGameName = gameName;
    showAchievementsView();
    renderAchievements();
    els.status.textContent = `Achievements: ${gameName}`;
  } catch (err) {
    els.status.textContent = "Erreur";
    setError(err.message || "Erreur inconnue");
  }
}

function applyGameFilters(items) {
  const q = els.gameQ.value.trim().toLowerCase();
  let out = items.filter(g => (g.name || "").toLowerCase().includes(q));

  const mode = els.gameSort.value;
  out.sort((a, b) => {
    if (mode === "completion_desc") {
      return (b.completionPct ?? 0) - (a.completionPct ?? 0) || (a.name || "").localeCompare(b.name || "");
    }
    if (mode === "completion_asc") {
      return (a.completionPct ?? 0) - (b.completionPct ?? 0) || (a.name || "").localeCompare(b.name || "");
    }
    if (mode === "name_desc") {
      return (b.name || "").localeCompare(a.name || "");
    }
    return (a.name || "").localeCompare(b.name || "");
  });

  return out;
}

function renderGames() {
  showGamesView();
  const games = applyGameFilters(allGames);
  els.count.textContent = `${games.length} / ${allGames.length}`;

  if (games.length === 0) {
    els.gamesGrid.innerHTML = "<p class='muted'>Aucun jeu avec achievements trouve pour ce profil.</p>";
    return;
  }

  els.gamesGrid.innerHTML = games.map(g => {
    const pct = (g.completionPct ?? 0).toFixed(2);
    const unlocked = g.unlockedAchievements ?? 0;
    const total = g.totalAchievements ?? 0;

    return `
      <article class="card gameCard">
        <div class="body">
          <div class="topline">
            <h3 class="title">${esc(g.name || `App ${g.appId}`)}</h3>
            <span class="pct">${pct}%</span>
          </div>
          <p class="desc">${unlocked} / ${total} achievements debloques</p>
          <div class="progress"><span style="width:${Math.max(0, Math.min(100, g.completionPct ?? 0))}%"></span></div>
          <div class="foot">
            <code class="api">AppID ${esc(g.appId)}</code>
            <button class="smallBtn" data-appid="${esc(g.appId)}" data-name="${esc(g.name || "")}">Voir les achievements</button>
          </div>
        </div>
      </article>
    `;
  }).join("");

  els.gamesGrid.querySelectorAll("button[data-appid]").forEach(btn => {
    btn.addEventListener("click", () => {
      loadAchievements(Number(btn.dataset.appid), btn.dataset.name || "Jeu");
    });
  });
}

function applyAchievementFilters(items) {
  const q = els.achQ.value.trim().toLowerCase();
  const showLocked = els.showLocked.checked;

  let out = items.filter(a => showLocked || a.achieved);
  if (q) {
    out = out.filter(a => `${a.name} ${a.description} ${a.apiName}`.toLowerCase().includes(q));
  }

  const mode = els.achSort.value;
  out.sort((a, b) => {
    if (mode === "unlock_desc") {
      return Number(Boolean(b.achieved)) - Number(Boolean(a.achieved)) || (b.globalPct ?? 0) - (a.globalPct ?? 0);
    }
    if (mode === "pct_desc") {
      return (b.globalPct ?? 0) - (a.globalPct ?? 0);
    }
    if (mode === "pct_asc") {
      return (a.globalPct ?? 0) - (b.globalPct ?? 0);
    }
    return (a.name || "").localeCompare(b.name || "");
  });

  return out;
}

function renderAchievements() {
  const items = applyAchievementFilters(allAchievements);
  els.count.textContent = `${items.length} / ${allAchievements.length}`;

  if (items.length === 0) {
    els.achievementsGrid.innerHTML = `<p class='muted'>Aucun achievement a afficher pour ${esc(currentGameName)}.</p>`;
    return;
  }

  els.achievementsGrid.innerHTML = items.map(a => {
    const pct = (a.globalPct ?? 0).toFixed(2) + "%";
    const status = a.achieved ? "debloque" : "verrouille";
    const statusClass = a.achieved ? "unlocked" : "locked";
    const desc = a.description?.trim() ? esc(a.description) : "<em class='muted'>Pas de description</em>";

    return `
      <article class="card">
        <div class="iconWrap ${statusClass}">
          <img class="icon" src="${esc(a.icon || a.iconGray || "")}" alt="" loading="lazy" />
        </div>
        <div class="body">
          <div class="topline">
            <h3 class="title">${esc(a.name || a.apiName)}</h3>
            <span class="badge ${statusClass}">${status}</span>
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

els.steamForm.addEventListener("submit", (ev) => {
  ev.preventDefault();
  currentSteamID = els.steamId.value.trim();
  loadGames();
});

els.refreshGames.addEventListener("click", () => loadGames(true));
els.backToGames.addEventListener("click", () => {
  showGamesView();
  renderGames();
  els.status.textContent = "Jeux charges";
});

["input", "change"].forEach(evt => {
  els.gameQ.addEventListener(evt, renderGames);
  els.gameSort.addEventListener(evt, renderGames);
  els.achQ.addEventListener(evt, renderAchievements);
  els.achSort.addEventListener(evt, renderAchievements);
  els.showLocked.addEventListener(evt, renderAchievements);
});

els.status.textContent = "Pret";