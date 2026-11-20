import { describeSite, field, fieldLines, geographicPoint, localPoint, passageColors, rectangle } from "./site-geometry.js";

const empty = { type: "FeatureCollection", features: [] };
const densityColors = ["#2ac0d1", "#f3bc57", "#ee694b"];
const integer = new Intl.NumberFormat("fr-FR");
const element = id => document.getElementById(id);
const container = element("density-map");
const canvas = element("density-canvas");
const sceneCanvas = element("site-canvas");
const mapElement = element("map");
const labels = element("site-labels");
const mode = element("map-mode");
const choice = element("map-choice");
const view = {
  geojson: empty, densityKey: "", passages: empty, passageKey: "", site: null, siteKey: "", center: null,
  configKey: "", mapConfig: null, map: null, stopMap: null, mapStatus: "", scene: null,
  sceneLoading: false, sceneFailed: false,
};

function setText(target, text) {
  if (target.textContent !== text) target.textContent = text;
}

function setSurface(surface) {
  container.classList.toggle("is-mapbox", surface === "mapbox");
  container.classList.toggle("is-site-3d", surface === "scene");
  canvas.setAttribute("aria-hidden", String(surface !== "canvas"));
  sceneCanvas.setAttribute("aria-hidden", String(surface !== "scene"));
  mapElement.setAttribute("aria-hidden", String(surface !== "mapbox"));
}

function sceneLabels(project) {
  const places = [{ kind: "pitch", label: "Terrain de football", point: [0, -91] }, ...view.site.places];
  if (!labels.childElementCount) {
    labels.replaceChildren(...places.map(place => {
      const label = document.createElement("span");
      label.className = "site-label " + place.kind;
      label.textContent = place.label;
      return label;
    }));
  }
  labels.hidden = false;
  const width = labels.clientWidth;
  places.forEach(({ kind, point }, i) => {
    const [x, y] = project(point, kind === "pitch" ? 0 : 13);
    const label = labels.children[i];
    const inset = label.offsetWidth / 2 + 4;
    label.style.left = Math.max(inset, Math.min(width - inset, x)) + "px";
    label.style.top = Math.max(label.offsetHeight + 3, y - 5) + "px";
  });
}

function canvasContext(width, height) {
  const scale = Math.min(window.devicePixelRatio || 1, 2);
  canvas.width = Math.round(width * scale);
  canvas.height = Math.round(height * scale);
  const ctx = canvas.getContext("2d");
  ctx?.scale(scale, scale);
  return ctx;
}

function drawPath(ctx, points, project) {
  ctx.beginPath();
  points.forEach((point, i) => {
    const [x, y] = project(point);
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
}

function fillPath(ctx, points, project, color) {
  drawPath(ctx, points, project);
  ctx.fillStyle = color;
  ctx.fill();
}

function strokePath(ctx, points, project, color, width) {
  drawPath(ctx, points, project);
  ctx.strokeStyle = color;
  ctx.lineWidth = width;
  ctx.stroke();
}

function drawTerrain(ctx, site, project, scale) {
  fillPath(ctx, rectangle(-85, -84, 85, 84), project, "#42545b");
  ctx.lineCap = "round";
  strokePath(ctx, site.corridor, project, "#385e69", 22 * scale);
  strokePath(ctx, site.corridor, project, "#60a9bb", Math.max(1, 2 * scale));
  for (let strip = 0; strip < 10; strip++) {
    const top = -52.5 + strip * 10.5;
    fillPath(ctx, rectangle(-34, top, 34, top + 10.5), project, strip % 2 ? "#228553" : "#34985c");
  }
  for (const line of fieldLines) strokePath(ctx, line, project, "#e1f4e8", Math.max(0.8, scale));
  for (const sign of [-1, 1]) {
    for (let row = 0; row < 5; row++) {
      const x = sign * (46 + row * 6);
      const z = sign * (63 + row * 3.5);
      strokePath(ctx, [[x, -59], [x, 59]], project, row % 2 ? "#39738c" : "#6f9bad", 4 * scale);
      strokePath(ctx, [[-40, z], [40, z]], project, row % 2 ? "#39738c" : "#6f9bad", 2.3 * scale);
    }
  }
  for (const { kind, point } of site.places) {
    const [x, z] = point;
    if (kind === "gate") {
      const extent = Math.max(Math.abs(x), Math.abs(z));
      strokePath(ctx, [[x * 83 / extent, z * 83 / extent], point], project, "#657884", 15 * scale);
      fillPath(ctx, rectangle(x - 9, z - 4, x + 9, z + 4), project, "#edb562");
    } else {
      fillPath(ctx, rectangle(x - 16, z - 8, x + 16, z + 8), project, "#5fb8d0");
      strokePath(ctx, [[x - 12, z], [x + 12, z]], project, "#b0e6ec", 2 * scale);
    }
  }
}

function drawPassages(ctx, width, height) {
  ctx.fillStyle = "#19373d";
  ctx.fillRect(0, 0, width, height);
  const { site, passages, center } = view;
  const origin = site?.origin || center;
  if (!origin) return;

  const [cx, cz] = site?.center || [0, 0];
  const scale = Math.min(width / (site?.width ?? 3500), height * 0.95 / (site?.depth ?? 1800));
  const project = ([x, z]) => [width / 2 + (x - cx) * scale, height / 2 + (z - cz) * scale];
  if (site) {
    drawTerrain(ctx, site, project, scale);
    sceneLabels(project);
  }
  const sparse = passages.features.length <= 20;
  for (const { geometry, properties } of passages.features) {
    const color = passageColors[properties.source];
    if (!color) continue;
    const [x, y] = project(localPoint(geometry.coordinates, origin));
    if (sparse) {
      ctx.beginPath();
      ctx.arc(x, y, 7.5, 0, 2 * Math.PI);
      ctx.strokeStyle = color;
      ctx.lineWidth = 1.8;
      ctx.stroke();
    }
    ctx.beginPath();
    ctx.arc(x, y, sparse ? 3.4 : 2.5, 0, 2 * Math.PI);
    ctx.fillStyle = color;
    ctx.fill();
    ctx.strokeStyle = "#10282e";
    ctx.lineWidth = 0.75;
    ctx.stroke();
  }
  if (!site && !passages.features.length) {
    ctx.fillStyle = "#c1d9dd";
    ctx.font = "14px system-ui";
    ctx.textAlign = "center";
    ctx.fillText("En attente de passages géolocalisés", width / 2, height / 2, width - 24);
  }
}

function drawDensity(ctx, width, height) {
  ctx.fillStyle = "#142a36";
  ctx.fillRect(0, 0, width, height);
  const features = view.geojson.features.slice().sort((a, b) => a.properties.count - b.properties.count);
  if (!features.length) {
    ctx.fillStyle = "#a9bfca";
    ctx.font = "15px system-ui";
    ctx.textAlign = "center";
    ctx.fillText("En attente des premières cellules H3", width / 2, height / 2, width - 24);
    return;
  }
  const rings = features.map(feature => feature.geometry.coordinates[0].map(point => localPoint(point, view.center)));
  let left = Infinity;
  let right = -Infinity;
  let top = Infinity;
  let bottom = -Infinity;
  for (const ring of rings) {
    for (const [x, z] of ring) {
      left = Math.min(left, x);
      right = Math.max(right, x);
      top = Math.min(top, z);
      bottom = Math.max(bottom, z);
    }
  }
  const scale = Math.min(width * 0.84 / (right - left), height * 0.78 / (bottom - top), 0.23);
  const project = ([x, z]) => [width / 2 + (x - (left + right) / 2) * scale, height / 2 + (z - (top + bottom) / 2) * scale];
  rings.forEach((ring, i) => {
    const count = features[i].properties.count;
    fillPath(ctx, ring, project, densityColors[count >= 100 ? 2 : count >= 30 ? 1 : 0] + "d9");
    strokePath(ctx, ring, project, "#102131", 2);
  });
}

function mapSiteFeatures(site) {
  if (!site) return empty;
  const coordinates = points => points.map(point => geographicPoint(point, site.origin));
  const polygon = (ring, kind) => ({ type: "Feature", geometry: { type: "Polygon", coordinates: [coordinates(ring)] }, properties: { kind } });
  const line = (points, kind) => ({ type: "Feature", geometry: { type: "LineString", coordinates: coordinates(points) }, properties: { kind } });
  const places = [{ kind: "pitch", label: "Terrain de football", point: [0, -76] }, ...site.places];
  return { type: "FeatureCollection", features: [
    polygon(field, "field"), polygon(rectangle(-72, -60, -44, 60), "stand"),
    polygon(rectangle(44, -60, 72, 60), "stand"),
    ...fieldLines.map(points => line(points, "marking")), line(site.corridor, "corridor"),
    ...places.map(({ kind, label, point }) => ({
      type: "Feature", geometry: { type: "Point", coordinates: geographicPoint(point, site.origin) }, properties: { kind, label },
    })),
  ] };
}

function addMapLayers(map) {
  const source = "simulation-site";
  map.addSource(source, { type: "geojson", data: mapSiteFeatures(view.site) });
  map.addLayer({ id: "site-ground", source, type: "fill", filter: ["==", "$type", "Polygon"],
    paint: { "fill-color": ["match", ["get", "kind"], "field", "#3d9b63", "#547d91"], "fill-opacity": 0.92 } });
  map.addLayer({ id: "site-lines", source, type: "line", filter: ["==", "$type", "LineString"],
    paint: { "line-color": ["match", ["get", "kind"], "corridor", "#62b4cc", "#e2f7e9"],
      "line-width": ["match", ["get", "kind"], "corridor", 5, 1.3] } });
  map.addLayer({ id: "site-markers", source, type: "circle", filter: ["all",
    ["==", "$type", "Point"], ["!=", "kind", "pitch"]],
    paint: { "circle-color": ["match", ["get", "kind"], "gate", "#ffcd76", "#71e5ff"],
      "circle-radius": 6, "circle-stroke-width": 2, "circle-stroke-color": "#132a33" } });
  map.addLayer({ id: "site-labels", source, type: "symbol", filter: ["==", "$type", "Point"],
    layout: { "text-field": ["get", "label"], "text-size": 12, "text-offset": [0, -1.15], "text-allow-overlap": true },
    paint: { "text-color": "#f5ffff", "text-halo-color": "#172b33", "text-halo-width": 2 } });
  map.addSource("passage-points", { type: "geojson", data: view.passages });
  map.addLayer({ id: "passage-points", source: "passage-points", type: "circle", paint: {
    "circle-radius": 4, "circle-color": ["match", ["get", "source"], "stadium_access", passageColors.stadium_access, passageColors.transit_validation],
    "circle-stroke-width": 1, "circle-stroke-color": "#132a33",
  } });
}

function mapCenter() {
  if (view.site) return geographicPoint(view.site.center, view.site.origin);
  return [view.center[1], view.center[0]];
}

function startMap(config) {
  view.mapStatus = "Chargement de Mapbox…";
  let stopped = false;
  const timeout = setTimeout(() => fallback("délai de chargement dépassé"), 8000);
  const stylesheet = document.createElement("link");
  const script = document.createElement("script");

  function stop() {
    if (stopped) return;
    stopped = true;
    clearTimeout(timeout);
    view.map?.remove();
    view.map = null;
    stylesheet.remove();
    script.remove();
  }

  function fallback(reason) {
    if (stopped) return;
    stop();
    view.mapStatus = "Mapbox · " + reason;
    renderActive();
  }

  stylesheet.rel = "stylesheet";
  stylesheet.href = config.cdn + "/mapbox-gl.css";
  stylesheet.onerror = () => fallback("style indisponible");
  script.src = config.cdn + "/mapbox-gl.js";
  script.onerror = () => fallback("indisponible");
  script.onload = () => {
    if (stopped) return;
    try {
      const gl = window.mapboxgl;
      if (!gl?.supported()) {
        fallback("WebGL indisponible");
        return;
      }
      const map = new gl.Map({ accessToken: config.token, container: mapElement, style: config.style,
        center: mapCenter(), zoom: view.site ? 16.2 : 12, antialias: true,
      });
      view.map = map;
      map.addControl(new gl.NavigationControl(), "top-right");
      map.on("error", ({ error }) => {
        if ([401, 403].includes(error?.status)) fallback("accès refusé");
      });
      map.on("webglcontextlost", () => fallback("WebGL indisponible"));
      map.on("load", () => {
        if (stopped) return;
        try {
          addMapLayers(map);
          clearTimeout(timeout);
          view.mapStatus = "";
          if (choice.value === "mapbox") renderActive();
        } catch {
          fallback("couches indisponibles");
        }
      });
    } catch {
      fallback("indisponible");
    }
  };
  document.head.append(stylesheet, script);
  return stop;
}

function ensureScene() {
  if (!view.site || view.scene || view.sceneLoading || view.sceneFailed) return;
  view.sceneLoading = true;
  import("./site-scene.js").then(({ createSiteScene }) => {
    if (!view.site) return;
    view.scene = createSiteScene(sceneCanvas, view.site, event => {
      event.preventDefault();
      view.scene?.dispose();
      view.scene = null;
      view.sceneFailed = true;
      renderActive();
    });
    view.scene.setPassages(view.passages.features);
  }).catch(() => { view.sceneFailed = true; }).finally(() => {
    view.sceneLoading = false;
    renderActive();
  });
}

function renderActive() {
  const density = choice.value === "density";
  const mapbox = choice.value === "mapbox";
  element("density-legend").hidden = !density;
  element("passage-legend").hidden = density;
  setText(element("map-title"), density ? "Densité des passages" : view.site ? "Terrain et passages" : "Passages");
  labels.hidden = true;
  const { width, height } = container.getBoundingClientRect();
  if (!width || !height) return;

  if (density) {
    setSurface("canvas");
    canvas.setAttribute("aria-label", "Carte locale de densité des passages par cellules H3");
    setText(mode, "Densité locale");
    const cells = new Set(view.geojson.features.map(feature => JSON.stringify(feature.geometry.coordinates[0]))).size;
    setText(element("map-render-status"), cells + (cells === 1 ? " cellule H3" : " cellules H3"));
    const ctx = canvasContext(width, height);
    if (ctx) drawDensity(ctx, width, height);
    return;
  }

  const passages = view.passages.features.length;
  setText(element("map-render-status"), integer.format(passages) + (passages === 1 ? " passage affiché" : " passages affichés"));
  if (mapbox && view.center && view.mapConfig?.token && !view.stopMap) view.stopMap = startMap(view.mapConfig);
  if (mapbox && view.map && !view.mapStatus) {
    if (!container.classList.contains("is-mapbox")) {
      setSurface("mapbox");
      view.map.resize();
    }
    setText(mode, view.site ? "Mapbox · site simulé" : "Mapbox");
    return;
  }

  ensureScene();
  const status = view.scene ? "Site simulé en 3D" : view.site ? "Plan du site simulé" : "Carte locale des passages";
  setText(mode, mapbox && view.mapStatus ? view.mapStatus + " · " + status : status);
  if (view.scene && view.site) {
    setSurface("scene");
    view.scene.resize(width, height);
    view.scene.paint();
    sceneLabels(view.scene.project);
  } else {
    setSurface("canvas");
    canvas.setAttribute("aria-label", view.site ?
      "Plan du site simulé avec terrain de football, tribunes, entrées, arrêt de transport et passages" : "Carte locale des passages");
    const ctx = canvasContext(width, height);
    if (ctx) drawPassages(ctx, width, height);
  }
}

choice.addEventListener("change", renderActive);
new ResizeObserver(() => {
  if (choice.value === "mapbox" && view.map && !view.mapStatus) view.map.resize();
  renderActive();
}).observe(container);

export function renderMap(payload, config) {
  const center = payload?.center;
  const centerChanged = center && (view.center?.[0] !== center[0] || view.center?.[1] !== center[1]);
  const geojson = payload?.geojson || empty;
  const passages = payload?.passages || empty;
  const densityKey = JSON.stringify(geojson.features.map(({ geometry, properties }) => [
    geometry, properties.city, properties.source, properties.count, properties.height,
  ]));
  const passageKey = JSON.stringify(passages.features.map(({ geometry, properties }) => [geometry.coordinates, properties.source]));
  const siteKey = JSON.stringify(payload?.site || null);
  const configKey = JSON.stringify(config ? [config.token, config.cdn, config.style, config.offline] : []);
  const densityChanged = densityKey !== view.densityKey;
  const passagesChanged = passageKey !== view.passageKey;
  const siteChanged = payload && siteKey !== view.siteKey;
  const configChanged = configKey !== view.configKey;

  if (centerChanged) view.center = center;
  if (densityChanged) Object.assign(view, { geojson, densityKey });
  if (passagesChanged) Object.assign(view, { passages, passageKey });
  if (siteChanged) {
    view.scene?.dispose();
    view.scene = null;
    view.site = payload.site ? describeSite(payload.site) : null;
    view.siteKey = siteKey;
    labels.replaceChildren();
    view.map?.getSource("simulation-site")?.setData(mapSiteFeatures(view.site));
  }
  if (configChanged) {
    view.stopMap?.();
    view.stopMap = null;
    view.mapStatus = "";
    view.configKey = configKey;
    view.mapConfig = config;
    const enabled = Boolean(config?.token && !config.offline);
    const option = element("mapbox-option");
    option.disabled = option.hidden = !enabled;
    if (!enabled && choice.value === "mapbox") choice.value = "passages";
  }

  if ((centerChanged || siteChanged) && view.map) {
    view.map.jumpTo({ center: mapCenter(), zoom: view.site ? 16.2 : 12 });
  }
  if (passagesChanged) {
    view.scene?.setPassages(passages.features);
    view.map?.getSource("passage-points")?.setData(passages);
  }

  const dataChanged = choice.value === "density" ? densityChanged : passagesChanged;
  if (centerChanged || siteChanged || configChanged || dataChanged) renderActive();
}
