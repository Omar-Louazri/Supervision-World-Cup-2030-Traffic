import { renderMap } from "./10_map.js";

const sourceLabels = {
  stadium_access: "Accès stade",
  transit_validation: "Transport",
};
const stageTitles = {
  1: "Premier événement",
  20: "Arrivées régulières",
  100: "Montée en charge",
  500: "Pic avant le match",
};
const stageLabels = {
  publishing: "Envoi vers Kafka",
  processing: "Traitement Spark",
  complete: "Confirmé dans Redis",
  failed: "Étape interrompue",
};
const connectionLabels = {
  connecting: "Connexion au direct…",
  live: "En direct",
  offline: "Direct interrompu · reconnexion…",
};

const byId = id => document.getElementById(id);
const citySelect = byId("city");
const stageSteps = document.querySelectorAll("[data-stage]");
const integer = new Intl.NumberFormat("fr-FR");
const percent = new Intl.NumberFormat("fr-FR", { style: "percent" });
const seconds = new Intl.NumberFormat("fr-FR", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
const listContents = new Map();
const socketUrl = new URL("/ws", location.href);
socketUrl.protocol = location.protocol === "https:" ? "wss:" : "ws:";

let socket;
let subscribed = false;
let reconnectDelay = 1000;
let city = "";
let mapConfig = null;
let displayedView = null;

function setText(id, value) {
  const target = byId(id);
  if (target.textContent !== value) target.textContent = value;
}

function element(tag, text, className) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text) node.textContent = text;
  return node;
}

function listItem(...children) {
  const result = element("article", "", "list-item");
  result.append(...children);
  return result;
}

function renderList(id, rows, buildRow, fallback) {
  const contents = JSON.stringify(rows.length ? rows : fallback);
  if (listContents.get(id) === contents) return;
  byId(id).replaceChildren(...(rows.length ? rows.map(buildRow) : [element("p", fallback, "muted")]));
  listContents.set(id, contents);
}

function renderStage(stage) {
  const active = Boolean(stage && stageTitles[stage.requested]);
  setText("stage-title", active ? stageTitles[stage.requested] : "Simulation d’avant-match");
  byId("stage-state").hidden = !active;
  byId("stage-prompt").hidden = active;
  byId("stage-progress").hidden = !active;
  for (const step of stageSteps) {
    if (active && Number(step.dataset.stage) === stage.requested) step.setAttribute("aria-current", "step");
    else step.removeAttribute("aria-current");
  }
  if (!active) return;

  const { requested, published, processed, elapsed_s, status } = stage;
  byId("stage-state").className = `stage-state ${status}`;
  setText("stage-state", stageLabels[status] || "État du palier indisponible");
  const bar = byId("stage-bar");
  bar.max = requested;
  bar.value = Math.max(0, Math.min(processed, requested));
  setText("stage-published", `${published} / ${requested}`);
  setText("stage-processed", `${processed} / ${requested}`);
  setText("stage-elapsed", `${seconds.format(elapsed_s)} s`);
}

function renderView(view, available) {
  const unavailable = available === false;
  displayedView = view ?? (unavailable ? displayedView : null);
  const venues = new Map((displayedView?.venues || []).map(row => [row.source, row]));
  for (const source of Object.keys(sourceLabels)) {
    const venue = venues.get(source);
    const forecast = displayedView?.forecasts?.[source];
    let pressure = "";
    if (venue) pressure = venue.pressure == null ? "Capacité inconnue" : `Pression ${percent.format(venue.pressure)}`;
    setText(`${source}-count`, venue ? integer.format(venue.event_count) : "—");
    setText(`${source}-pressure`, pressure);
    setText(`${source}-forecast`, forecast == null ? "" : `Prévision · ${integer.format(forecast)}`);
  }

  const hotels = displayedView?.hotels || [];
  const hotel = hotels.find(row => row.area_id === "citywide");
  setText("hotel-occupancy", hotel && !hotel.stale && hotel.occupancy != null ? percent.format(hotel.occupancy) : "—");
  setText("hotel-note", hotel ? (hotel.stale ? "Données périmées" : "Données disponibles") : "");

  const alerts = (displayedView?.alerts || []).map(row => {
    const source = sourceLabels[row.source] || row.source;
    return [`${source} · ${percent.format(row.pressure)}`, row.venue];
  });
  renderList("alerts", alerts, ([pressure, venue]) => {
    const item = listItem(element("strong", pressure), element("span", venue));
    item.classList.add("alert");
    return item;
  }, unavailable ? "Données temporairement indisponibles." : (view ? "Aucune alerte active." : "En attente des données."));

  const recommendations = (displayedView?.recommendations || []).map(route => [
    route.route, `${integer.format(route.available_seats)} places · ${integer.format(route.travel_minutes)} min`, route.reason,
  ]);
  renderList("recommendations", recommendations, ([route, capacity, reason]) => listItem(
    element("strong", route), element("span", capacity), element("small", reason),
  ), "Aucune suggestion en cours.");

  const hotelRows = hotels.map(hotel => {
    const area = hotel.area_id === "citywide" ? "Toute la ville" : hotel.area_id;
    const occupancy = `${integer.format(hotel.rooms_occupied)} / ${integer.format(hotel.rooms_occupied + hotel.rooms_available)}`;
    return `${area} · ${occupancy} chambres · ${hotel.stale ? "périmé" : "à jour"}`;
  });
  renderList("hotels", hotelRows, row => listItem(row), "Aucune donnée hôtelière.");

  const updated = displayedView?.latest_update;
  let freshness = view ? "En attente d’événements" : "";
  if (updated) freshness = `Dernière mise à jour à ${updated.slice(11, 19)} UTC`;
  if (unavailable) freshness = updated ? `Redis indisponible · données de ${updated.slice(11, 19)} UTC` : "Redis indisponible";
  setText("freshness", freshness);
  renderMap(displayedView, mapConfig);
}

function showConnection(state) {
  byId("connection").dataset.state = state;
  setText("connection", connectionLabels[state]);
}

function subscribe() {
  if (subscribed && socket?.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify({ type: "subscribe", city }));
  }
}

function configure(message) {
  const previousCity = city;
  city = message.cities.includes(city) ? city : message.city;
  mapConfig = message.map;

  const presentation = Boolean(message.presentation);
  document.querySelector(".shell").classList.toggle("presentation", presentation);
  byId("presentation-links").hidden = !presentation;
  byId("stage-panel").hidden = !presentation;
  setText("subtitle", presentation ? "Simulation de l’arrivée des spectateurs à Rabat" :
    "Supervision des accès, des transports et des capacités hôtelières.");

  const names = Array.from(citySelect.options, option => option.value);
  if (JSON.stringify(names) !== JSON.stringify(message.cities)) {
    citySelect.replaceChildren(...message.cities.map(name => {
      const option = element("option", name);
      option.value = name;
      return option;
    }));
  }
  citySelect.value = city;
  byId("city-picker").hidden = message.cities.length === 1;
  byId("fixed-city").hidden = message.cities.length !== 1;
  setText("fixed-city", city);

  if (city === previousCity) renderMap(displayedView, mapConfig);
  else renderView(null);
  if (city !== message.city) subscribe();
}

function connect() {
  socket = new WebSocket(socketUrl);
  const current = socket;
  const handshakeDeadline = setTimeout(() => current.close(), 10000);

  current.addEventListener("message", event => {
    let message;
    try {
      message = JSON.parse(event.data);
    } catch {
      return;
    }
    if (message?.type === "config") {
      clearTimeout(handshakeDeadline);
      subscribed = true;
      reconnectDelay = 1000;
      configure(message);
      showConnection("live");
    } else if (message?.type === "view" && message.city === city) {
      renderView(message.view, message.available);
    } else if (message?.type === "stage") {
      renderStage(message.stage);
    }
  });

  current.addEventListener("close", () => {
    clearTimeout(handshakeDeadline);
    subscribed = false;
    showConnection("offline");
    setTimeout(connect, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, 15000);
  });
}

citySelect.addEventListener("change", () => {
  city = citySelect.value;
  renderView(null);
  subscribe();
});

renderView(null);
connect();
