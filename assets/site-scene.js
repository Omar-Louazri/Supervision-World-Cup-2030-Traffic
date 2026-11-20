import * as THREE from "/vendor/three.module.js";
import { fieldLines, localPoint, passageColors } from "./site-geometry.js";

export function createSiteScene(canvas, site, onContextLost) {
  const renderer = new THREE.WebGLRenderer({ canvas, antialias: true, powerPreference: "low-power" });
  const scene = new THREE.Scene();
  scene.background = new THREE.Color(0x10292e);
  const camera = new THREE.OrthographicCamera(-1, 1, 1, -1, 1, 1600);
  const ground = new THREE.Group();
  const people = new THREE.Group();
  scene.add(ground, people);
  scene.add(new THREE.HemisphereLight(0xe1f6f3, 0x23343f, 2.1));
  const light = new THREE.DirectionalLight(0xffead0, 2.0);
  light.position.set(-260, 420, 140);
  scene.add(light);

  const materials = new Map();
  function material(color) {
    if (!materials.has(color)) materials.set(color, new THREE.MeshLambertMaterial({ color }));
    return materials.get(color);
  }

  function block(x, y, z, width, height, depth, color) {
    const mesh = new THREE.Mesh(new THREE.BoxGeometry(width, height, depth), material(color));
    mesh.position.set(x, y, z);
    ground.add(mesh);
    return mesh;
  }

  function road(start, end, width, height, color) {
    const dx = end[0] - start[0];
    const dz = end[1] - start[1];
    const mesh = block((start[0] + end[0]) / 2, height / 2, (start[1] + end[1]) / 2,
      width, height, Math.hypot(dx, dz), color);
    mesh.rotation.y = Math.atan2(dx, dz);
  }

  const [cx, cz] = site.center;
  block(cx, -2.5, cz, site.width + 350, 4, site.depth + 450, 0x18333a);
  block(0, 0, 0, 170, 2, 168, 0x47565a);
  block(0, 1, 0, 82, 2, 120, 0x357459);
  for (let strip = 0; strip < 10; strip++) {
    block(0, 2, -47.25 + strip * 10.5, 68, 0.35, 10.5, strip % 2 ? 0x278955 : 0x40a161);
  }

  const markings = new THREE.LineBasicMaterial({ color: 0xf6fff4 });
  for (const shape of fieldLines) {
    const geometry = new THREE.BufferGeometry().setFromPoints(shape.map(([x, z]) => new THREE.Vector3(x, 2.25, z)));
    ground.add(new THREE.Line(geometry, markings));
  }
  for (const side of [-1, 1]) {
    for (let row = 0; row < 5; row++) {
      const height = 4 + row * 3.4;
      const x = side * (45 + row * 6);
      block(x, height / 2 + 1, 0, 6, height, 119, 0x516474);
      block(x, height + 1.6, 0, 4, 1.1, 108, row % 2 ? 0x1a5272 : 0x2c6d86);
    }
    for (let row = 0; row < 3; row++) {
      const height = 3 + row * 2.7;
      block(0, height / 2 + 1, side * (62 + row * 5), 77, height, 5, row % 2 ? 0x316581 : 0x477383);
    }
    const goal = side * 54;
    block(-3.66, 3.5, goal, 0.5, 3, 0.5, 0xf2f5ed);
    block(3.66, 3.5, goal, 0.5, 3, 0.5, 0xf2f5ed);
    block(0, 5, goal, 7.8, 0.5, 0.5, 0xf2f5ed);
  }

  road(...site.corridor, 24, 0.8, 0x31626d);
  road(...site.corridor, 3.2, 1.15, 0x559aac);
  for (const { kind, point: [x, z] } of site.places) {
    if (kind === "gate") {
      const square = Math.max(Math.abs(x), Math.abs(z));
      road([x * 83 / square, z * 83 / square], [x, z], 19, 0.7, 0x627177);
      block(x - 7, 5, z, 2.5, 10, 4, 0xe1a455);
      block(x + 7, 5, z, 2.5, 10, 4, 0xe1a455);
      block(x, 10, z, 19, 2.3, 8, 0xffcc76);
    } else {
      block(x, 0.4, z, 42, 0.7, 27, 0x376477);
      block(x - 12, 5, z, 2.5, 10, 5, 0x67c8e3);
      block(x + 12, 5, z, 2.5, 10, 5, 0x67c8e3);
      block(x, 10, z, 30, 2.5, 13, 0x70d8ed);
    }
  }

  const body = new THREE.CapsuleGeometry(2.1, 3.8, 3, 7);
  const head = new THREE.SphereGeometry(2.25, 8, 5);
  const halo = new THREE.RingGeometry(6.0, 8.8, 24).rotateX(-Math.PI / 2);
  const matrix = new THREE.Matrix4();
  let width = 0;
  let height = 0;
  let pixelRatio = 0;

  function paint() {
    renderer.render(scene, camera);
  }

  function setPassages(features) {
    for (const mesh of [...people.children]) {
      people.remove(mesh);
      mesh.dispose();
    }
    for (const [source, color] of Object.entries(passageColors)) {
      const points = features.filter(feature => feature.properties.source === source);
      if (!points.length) continue;
      const bodies = new THREE.InstancedMesh(body, material(color), points.length);
      const heads = new THREE.InstancedMesh(head, material(color), points.length);
      const rings = features.length <= 20 ? new THREE.InstancedMesh(halo, material(color), points.length) : null;
      points.forEach(({ geometry }, index) => {
        const [x, z] = localPoint(geometry.coordinates, site.origin);
        bodies.setMatrixAt(index, matrix.makeTranslation(x, 4.8, z));
        heads.setMatrixAt(index, matrix.makeTranslation(x, 9.1, z));
        rings?.setMatrixAt(index, matrix.makeTranslation(x, 2.5, z));
      });
      people.add(bodies, heads);
      if (rings) people.add(rings);
    }
  }

  function resize(w, h) {
    const ratio = Math.min(window.devicePixelRatio || 1, 2);
    if (w === width && h === height && ratio === pixelRatio) return;
    width = w;
    height = h;
    pixelRatio = ratio;
    const aspect = w / h;
    const span = Math.max(site.width / aspect, site.depth * 0.89);
    camera.left = -span * aspect / 2;
    camera.right = span * aspect / 2;
    camera.top = span / 2;
    camera.bottom = -span / 2;
    camera.position.set(cx, 670, cz + 410);
    camera.lookAt(cx, 0, cz);
    camera.updateProjectionMatrix();
    camera.updateMatrixWorld();
    renderer.setPixelRatio(pixelRatio);
    renderer.setSize(w, h, false);
  }

  function project([x, z], y = 0) {
    const screen = new THREE.Vector3(x, y, z).project(camera);
    return [(screen.x + 1) * width / 2, (1 - screen.y) * height / 2];
  }

  canvas.addEventListener("webglcontextlost", onContextLost, { once: true });
  return { setPassages, resize, project, paint, dispose() {
    canvas.removeEventListener("webglcontextlost", onContextLost);
    for (const mesh of ground.children) mesh.geometry.dispose();
    for (const mesh of people.children) mesh.dispose();
    for (const item of materials.values()) item.dispose();
    markings.dispose();
    body.dispose();
    head.dispose();
    halo.dispose();
    renderer.dispose();
  } };
}
