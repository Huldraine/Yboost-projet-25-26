let currentSteamID = "";
let allGames = [];
let allAchievements = [];
let currentGameName = "";
let suggestionByValue = new Map();
let suggestionsByName = new Map();
let suggestionsTimer = null;
let lastSuggestionQuery = "";
let suggestionRequestSeq = 0;
const API_FALLBACKS = ["http://127.0.0.1:8099", "http://localhost:8099"];

const els = {
  steamForm: document.getElementById("steamForm"),
  steamId: document.getElementById("steamId"),
  userSuggestions: document.getElementById("userSuggestions"),
  profileCard: document.getElementById("profileCard"),
  profileAvatar: document.getElementById("profileAvatar"),
  profileName: document.getElementById("profileName"),
  profileSteamId: document.getElementById("profileSteamId"),
  supabaseActions: document.getElementById("supabaseActions"),
  saveProfileStats: document.getElementById("saveProfileStats"),
  clearSupabaseCache: document.getElementById("clearSupabaseCache"),
  refreshLeaderboard: document.getElementById("refreshLeaderboard"),
  leaderboardBox: document.getElementById("leaderboardBox"),
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

function normalizeName(v) {
  return String(v || "").trim().toLowerCase();
}

function formatSuggestionLabel(item) {
  const name = (item.displayName || "").trim();
  if (!name || name === item.steamId) {
    return item.steamId;
  }
  return `${name} (${item.steamId})`;
}

function getSteamGameImageURL(appId) {
  const id = Number(appId);
  if (!Number.isFinite(id) || id <= 0) {
    return "";
  }
  return `https://cdn.cloudflare.steamstatic.com/steam/apps/${id}/header.jpg`;
}

async function requestJSON(url, method = "GET", payload) {
  const isApiPath = typeof url === "string" && url.startsWith("/api/");
  const options = {
    method,
    cache: "no-store",
    headers: {
      "Content-Type": "application/json"
    }
  };
  if (payload !== undefined) {
    options.body = JSON.stringify(payload);
  }

  if (!isApiPath) {
    const res = await fetch(url, options);
    const body = await res.json().catch(() => null);
    if (!res.ok) {
      const details = body?.details || `HTTP ${res.status}`;
      throw new Error(details);
    }
    return body ?? null;
  }

  const bases = [];
  const origin = window.location.origin;
  if (origin && !origin.startsWith("file://")) {
    bases.push(origin);
  }
  for (const fb of API_FALLBACKS) {
    if (!bases.includes(fb)) {
      bases.push(fb);
    }
  }

  let lastError = null;
  for (const base of bases) {
    const fullURL = `${base}${url}`;
    try {
      const res = await fetch(fullURL, options);
      const body = await res.json().catch(() => null);
      if (res.ok) {
        return body ?? null;
      }
      // On a static server (Five Server), /api/* usually returns 404 HTML.
      if (res.status === 404) {
        lastError = new Error("HTTP 404");
        continue;
      }
      const details = body?.details || `HTTP ${res.status}`;
      throw new Error(details);
    } catch (err) {
      lastError = err;
    }
  }

  throw lastError || new Error("API indisponible");
}

async function getJSON(url) {
  return requestJSON(url, "GET");
}

async function loadUserSuggestions(query) {
  const q = String(query || "").trim();
  const requestSeq = ++suggestionRequestSeq;
  if (q.length > 0 && q.length < 2) {
    return;
  }
  if (q === lastSuggestionQuery) {
    return;
  }
  lastSuggestionQuery = q;

  try {
    const rows = await getJSON(`/api/users/suggestions?q=${encodeURIComponent(q)}`);
    if (requestSeq !== suggestionRequestSeq) {
      return;
    }
    const list = Array.isArray(rows) ? rows : [];

    suggestionByValue = new Map();
    suggestionsByName = new Map();

    const options = list.map((item) => {
      const value = formatSuggestionLabel(item);
      suggestionByValue.set(value, item.steamId);

      const normalized = normalizeName(item.displayName);
      if (normalized) {
        const bucket = suggestionsByName.get(normalized) || [];
        bucket.push(item.steamId);
        suggestionsByName.set(normalized, bucket);
      }

      if (isValidSteamID64(item.steamId)) {
        suggestionByValue.set(item.steamId, item.steamId);
      }

      return `<option value="${esc(value)}"></option>`;
    });

    els.userSuggestions.innerHTML = options.join("");
  } catch (_) {
    // Ignore suggestion errors to keep main flow responsive.
  }
}

async function resolveSteamIDFromInput(rawValue) {
  const v = String(rawValue || "").trim();
  if (isValidSteamID64(v)) {
    return v;
  }

  const byValue = suggestionByValue.get(v);
  if (byValue && isValidSteamID64(byValue)) {
    return byValue;
  }

  const matches = suggestionsByName.get(normalizeName(v)) || [];
  if (matches.length === 1 && isValidSteamID64(matches[0])) {
    return matches[0];
  }

  // Last chance: query backend directly for an exact pseudo match.
  try {
    const rows = await getJSON(`/api/users/suggestions?q=${encodeURIComponent(v)}`);
    const list = Array.isArray(rows) ? rows : [];
    const exact = list.filter((item) => normalizeName(item.displayName) === normalizeName(v));
    if (exact.length === 1 && isValidSteamID64(exact[0].steamId)) {
      return exact[0].steamId;
    }
  } catch (_) {
    // Keep graceful failure below.
  }

  return "";
}

function renderProfile(profile) {
  const steamId = String(profile?.steamId || "").trim();
  if (!steamId) {
    els.profileCard.classList.add("hidden");
    return;
  }

  const displayName = String(profile?.displayName || steamId).trim();
  const avatarUrl = String(profile?.avatarUrl || "").trim();

  els.profileName.textContent = displayName;
  els.profileSteamId.textContent = steamId;

  if (avatarUrl) {
    els.profileAvatar.src = avatarUrl;
    els.profileAvatar.alt = `Avatar de ${displayName}`;
  } else {
    els.profileAvatar.src = "";
    els.profileAvatar.alt = "Avatar indisponible";
  }

  els.profileCard.classList.remove("hidden");
  els.supabaseActions.classList.remove("hidden");
}

async function resolveProfileFromInput(rawValue) {
  const v = String(rawValue || "").trim();
  if (!v) {
    throw new Error("Saisis un pseudo ou un SteamID64");
  }
  const profile = await getJSON(`/api/users/profile?steamId=${encodeURIComponent(v)}`);
  if (!isValidSteamID64(profile?.steamId || "")) {
    throw new Error("Profil introuvable");
  }
  return profile;
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
    // Keep SteamID normalized when forcing refresh.
    currentSteamID = currentSteamID.trim();
  }

  try {
    const qs = new URLSearchParams({ steamId: currentSteamID });
    if (forceRefresh) {
      qs.set("refresh", "1");
    }
    const games = await getJSON(`/api/users/games?${qs.toString()}`);
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

function collectCurrentStats() {
  if (!Array.isArray(allGames) || allGames.length === 0) {
    return null;
  }

  return allGames.reduce((acc, game) => {
    acc.gamesCount += 1;
    acc.totalAchievements += Number(game.totalAchievements || 0);
    acc.unlockedAchievements += Number(game.unlockedAchievements || 0);
    return acc;
  }, { gamesCount: 0, totalAchievements: 0, unlockedAchievements: 0 });
}

async function saveCurrentProfileStats() {
  if (!isValidSteamID64(currentSteamID)) {
    setError("Charge d'abord un profil Steam valide.");
    return;
  }

  setError("");
  const payload = {
    steamId: currentSteamID,
    nickname: String(els.profileName.textContent || currentSteamID).trim() || currentSteamID
  };
  const stats = collectCurrentStats();
  if (stats) {
    payload.gamesCount = stats.gamesCount;
    payload.totalAchievements = stats.totalAchievements;
    payload.unlockedAchievements = stats.unlockedAchievements;
  }

  try {
    const saved = await requestJSON("/api/crud/users/save", "POST", payload);
    els.status.textContent = `Profil sauvegarde: ${saved.nickname}`;
    await loadLeaderboard();
  } catch (err) {
    setError(err.message || "Impossible de sauvegarder le profil dans Supabase.");
  }
}

function renderLeaderboard(items) {
  const rows = Array.isArray(items) ? items : [];
  if (rows.length === 0) {
    els.leaderboardBox.className = "leaderboardBox muted";
    els.leaderboardBox.textContent = "Aucun profil sauvegarde pour le moment.";
    return;
  }

  const lines = rows.map((item, idx) => {
    const rank = idx + 1;
    const name = esc(item.nickname || item.steamId || "Inconnu");
    const unlocked = Number(item.unlockedAchievements || 0);
    const total = Number(item.totalAchievements || 0);
    const games = Number(item.gamesCount || 0);
    return `
      <li>
        <span class="rank">#${rank}</span>
        <span class="name">${name}</span>
        <span class="stats">${unlocked} / ${total} debloques • ${games} jeux</span>
      </li>
    `;
  }).join("");

  els.leaderboardBox.className = "leaderboardBox";
  els.leaderboardBox.innerHTML = `<ol class="leaderboardList">${lines}</ol>`;
}

async function loadLeaderboard() {
  try {
    const rows = await getJSON("/api/crud/leaderboard?limit=50");
    renderLeaderboard(rows);
  } catch (err) {
    els.leaderboardBox.className = "leaderboardBox muted";
    els.leaderboardBox.textContent = err.message || "Impossible de charger le classement.";
  }
}

async function clearSupabaseCache() {
  if (!window.confirm("Supprimer tous les profils sauvegardes du classement Supabase ?")) {
    return;
  }
  setError("");
  try {
    await requestJSON("/api/crud/cache", "DELETE");
    els.status.textContent = "Cache Supabase vide";
    await loadLeaderboard();
  } catch (err) {
    setError(err.message || "Impossible de vider le cache Supabase.");
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
    const cover = getSteamGameImageURL(g.appId);

    return `
      <article class="card gameCard">
        <div class="gameMediaWrap">
          <img class="gameMedia" src="${esc(cover)}" alt="Illustration du jeu ${esc(g.name || `App ${g.appId}`)}" loading="lazy" onerror="this.style.display='none'; this.parentElement.classList.add('noImage');" />
        </div>
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

els.steamForm.addEventListener("submit", async (ev) => {
  ev.preventDefault();
  try {
    const profile = await resolveProfileFromInput(els.steamId.value);
    currentSteamID = profile.steamId;
    renderProfile(profile);
    setError("");
    loadGames();
  } catch (err) {
    currentSteamID = "";
    els.profileCard.classList.add("hidden");
    els.supabaseActions.classList.add("hidden");
    setError(err.message || "Pseudo non trouve en base. Choisis une proposition ou saisis un SteamID64 valide.");
    return;
  }
});

els.steamId.addEventListener("input", () => {
  if (suggestionsTimer) {
    clearTimeout(suggestionsTimer);
  }
  suggestionsTimer = setTimeout(() => {
    loadUserSuggestions(els.steamId.value);
  }, 320);
});

els.steamId.addEventListener("focus", () => {
  loadUserSuggestions(els.steamId.value);
});

els.refreshGames.addEventListener("click", () => loadGames(true));
els.saveProfileStats.addEventListener("click", saveCurrentProfileStats);
els.clearSupabaseCache.addEventListener("click", clearSupabaseCache);
els.refreshLeaderboard.addEventListener("click", loadLeaderboard);
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
loadLeaderboard();