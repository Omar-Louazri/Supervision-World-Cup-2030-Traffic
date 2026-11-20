const metersPerDegree = 111_195;

export const passageColors = {
  stadium_access: "#ffcd76",
  transit_validation: "#71e5ff",
};

export function localPoint([lon, lat], [originLat, originLon]) {
  const longitudeScale = Math.cos(originLat * Math.PI / 180);
  return [(lon - originLon) * metersPerDegree * longitudeScale, (originLat - lat) * metersPerDegree];
}

export function geographicPoint([x, z], [lat, lon]) {
  const longitudeScale = Math.cos(lat * Math.PI / 180);
  return [lon + x / (metersPerDegree * longitudeScale), lat - z / metersPerDegree];
}

export function rectangle(x1, z1, x2, z2) {
  return [[x1, z1], [x2, z1], [x2, z2], [x1, z2], [x1, z1]];
}

export const field = rectangle(-34, -52.5, 34, 52.5);
export const fieldLines = [field, [[-34, 0], [34, 0]],
  rectangle(-20.16, -52.5, 20.16, -36), rectangle(-20.16, 36, 20.16, 52.5),
  rectangle(-9.16, -52.5, 9.16, -47), rectangle(-9.16, 47, 9.16, 52.5),
  Array.from({ length: 33 }, (_, i) => {
    const angle = i * Math.PI / 16;
    return [9.15 * Math.cos(angle), 9.15 * Math.sin(angle)];
  }),
];

export function describeSite(site) {
  const point = ([lat, lon]) => localPoint([lon, lat], site.pitch);
  const gates = site.gates.map(point);
  const transit = point(site.transit);
  const distanceToTransit = ([x, z]) => Math.hypot(x - transit[0], z - transit[1]);
  const closestGate = gates.reduce((best, gate) => distanceToTransit(gate) < distanceToTransit(best) ? gate : best);
  const places = gates.map(([x, z]) => ({
    kind: "gate", point: [x, z],
    label: `Entrée ${Math.abs(x) >= Math.abs(z) ? (x < 0 ? "ouest" : "est") : (z < 0 ? "nord" : "sud")}`,
  }));
  places.push({ kind: "transit", label: "Arrêt de transport", point: transit });
  const bounds = [...gates, transit];
  const left = Math.min(-120, ...bounds.map(([x]) => x)) - 48;
  const right = Math.max(120, ...bounds.map(([x]) => x)) + 60;
  const top = Math.min(-120, ...bounds.map(([, z]) => z)) - 30;
  const bottom = Math.max(120, ...bounds.map(([, z]) => z)) + 24;
  return { origin: site.pitch, places, corridor: [closestGate, transit],
    width: right - left, depth: bottom - top, center: [(left + right) / 2, (top + bottom) / 2] };
}
