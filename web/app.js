const DATA_PATHS = {
  tracts: "../data/boundaries/tracts_dfw.geojson",
  redev: "../data/boundaries/redev_orgs.geojson",
  acs: "../data/processed/acs_panel.csv",
  lodesDominant: "../data/processed/lodes_dominant_sector.csv",
  lodesDots: "../data/processed/lodes_dots.geojson",
  lodesSector: "../data/processed/lodes_tract_sector.csv",
};

const ACS_FIELDS = [
  { id: "total_pop", label: "Total population" },
  { id: "median_income", label: "Median household income" },
  { id: "poverty_rate", label: "Poverty rate" },
  { id: "ba_plus_rate", label: "BA+ rate (25+)" },
  { id: "owner_rate", label: "Owner-occupied rate" },
  { id: "hispanic_rate", label: "Hispanic/Latino share" },
  { id: "age_under18", label: "Population under 18" },
  { id: "age_65_plus", label: "Population 65+" },
  { id: "delta_total_pop", label: "Change in total population" },
  { id: "delta_median_income", label: "Change in median income" },
  { id: "delta_poverty_rate", label: "Change in poverty rate" },
  { id: "delta_ba_plus_rate", label: "Change in BA+ rate" },
];

const SECTOR_COLORS = {
  Agriculture_Forestry_Fishing: "#4b8b3b",
  Mining: "#8c564b",
  Utilities: "#1f77b4",
  Construction: "#ff7f0e",
  Manufacturing: "#bcbd22",
  Wholesale_Trade: "#9467bd",
  Retail_Trade: "#d62728",
  Transportation_Warehousing: "#7f7f7f",
  Information: "#17becf",
  Finance_Insurance: "#2ca02c",
  Real_Estate_Rental: "#c7c7c7",
  Professional_Scientific_Technical: "#8c6bb1",
  Management_Companies: "#9edae5",
  Admin_Waste_Services: "#e377c2",
  Educational_Services: "#bc80bd",
  Health_Care_Social_Assistance: "#fb8072",
  Arts_Entertainment_Recreation: "#fdb462",
  Accommodation_Food_Services: "#80b1d3",
  Other_Services: "#b3de69",
  Public_Administration: "#f781bf",
};

const STATE = {
  viewMode: "acs",
  year: null,
  acsField: "total_pop",
  selectedRedev: "All",
  selectedGeoid: null,
};

const elements = {
  yearSlider: document.getElementById("yearSlider"),
  yearLabel: document.getElementById("yearLabel"),
  acsField: document.getElementById("acsField"),
  viewMode: document.getElementById("viewMode"),
  redevSelect: document.getElementById("redevSelect"),
  selectionLabel: document.getElementById("selectionLabel"),
  downloadAcs: document.getElementById("downloadAcs"),
  downloadLodes: document.getElementById("downloadLodes"),
  legend: document.getElementById("legend"),
};

let map;
let tractsData;
let redevData;
let acsData;
let lodesDominant;
let lodesDots;
let lodesSector;
let acsLookup = {};
let years = [];
let popup;

const formatNumber = d3.format(",.0f");
const formatPct = d3.format(".1%");

function buildAcsLookup() {
  acsLookup = {};
  acsData.forEach((row) => {
    const year = +row.year;
    if (!acsLookup[year]) {
      acsLookup[year] = {};
    }
    acsLookup[year][row.geoid] = row;
  });
}

function initializeUI() {
  ACS_FIELDS.forEach((field) => {
    const option = document.createElement("option");
    option.value = field.id;
    option.textContent = field.label;
    elements.acsField.appendChild(option);
  });

  years = Array.from(new Set(acsData.map((d) => +d.year))).sort();
  elements.yearSlider.min = 0;
  elements.yearSlider.max = years.length - 1;
  elements.yearSlider.value = years.length - 1;
  STATE.year = years[years.length - 1];
  elements.yearLabel.textContent = STATE.year;

  const redevNames = Array.from(
    new Set(tractsData.features.map((f) => f.properties.redev_name))
  )
    .filter((d) => d && d !== "None")
    .sort();
  elements.redevSelect.innerHTML = "";
  const allOption = document.createElement("option");
  allOption.value = "All";
  allOption.textContent = "All";
  elements.redevSelect.appendChild(allOption);
  redevNames.forEach((name) => {
    const option = document.createElement("option");
    option.value = name;
    option.textContent = name;
    elements.redevSelect.appendChild(option);
  });

  elements.yearSlider.addEventListener("input", () => {
    STATE.year = years[+elements.yearSlider.value];
    elements.yearLabel.textContent = STATE.year;
    updateAcsData();
  });

  elements.acsField.addEventListener("change", () => {
    STATE.acsField = elements.acsField.value;
    updateAcsStyle();
  });

  elements.viewMode.addEventListener("change", () => {
    STATE.viewMode = elements.viewMode.value;
    updateViewMode();
  });

  elements.redevSelect.addEventListener("change", () => {
    STATE.selectedRedev = elements.redevSelect.value;
    updateFilters();
  });

  elements.downloadAcs.addEventListener("click", () => {
    downloadAcsCsv();
  });

  elements.downloadLodes.addEventListener("click", () => {
    downloadLodesCsv();
  });
}

function buildMap() {
  map = new maplibregl.Map({
    container: "map",
    style: "https://demotiles.maplibre.org/style.json",
    center: [-97.3, 32.75],
    zoom: 9.5,
  });

  map.addControl(new maplibregl.NavigationControl(), "top-right");

  map.on("load", () => {
    map.addSource("tracts", {
      type: "geojson",
      data: tractsData,
    });

    map.addSource("redev", {
      type: "geojson",
      data: redevData,
    });

    map.addSource("lodesDots", {
      type: "geojson",
      data: lodesDots,
    });

    map.addLayer({
      id: "tract-fill-acs",
      type: "fill",
      source: "tracts",
      paint: {
        "fill-color": "#cbd2d9",
        "fill-opacity": 0.75,
      },
    });

    map.addLayer({
      id: "tract-fill-lodes",
      type: "fill",
      source: "tracts",
      layout: {
        visibility: "none",
      },
      paint: {
        "fill-color": "#cbd2d9",
        "fill-opacity": 0.75,
      },
    });

    map.addLayer({
      id: "tract-outline",
      type: "line",
      source: "tracts",
      paint: {
        "line-color": "#111827",
        "line-width": 1.5,
      },
      filter: ["==", ["get", "GEOID"], ""],
    });

    map.addLayer({
      id: "redev-outline",
      type: "line",
      source: "redev",
      paint: {
        "line-color": "#0f172a",
        "line-width": 2,
      },
    });

    map.addLayer({
      id: "lodes-dots",
      type: "circle",
      source: "lodesDots",
      layout: {
        visibility: "none",
      },
      paint: {
        "circle-radius": 3,
        "circle-color": [
          "match",
          ["get", "sector_name"],
          ...Object.entries(SECTOR_COLORS).flat(),
          "#999999",
        ],
        "circle-opacity": 0.8,
      },
    });

    map.on("click", "tract-fill-acs", handleTractClick);
    map.on("click", "tract-fill-lodes", handleTractClick);
    map.on("mouseenter", "tract-fill-acs", () => {
      map.getCanvas().style.cursor = "pointer";
    });
    map.on("mouseleave", "tract-fill-acs", () => {
      map.getCanvas().style.cursor = "";
    });
    map.on("mouseenter", "tract-fill-lodes", () => {
      map.getCanvas().style.cursor = "pointer";
    });
    map.on("mouseleave", "tract-fill-lodes", () => {
      map.getCanvas().style.cursor = "";
    });

    updateAcsData();
    updateViewMode();
  });
}

function handleTractClick(event) {
  const feature = event.features?.[0];
  if (!feature) return;
  STATE.selectedGeoid = feature.properties.GEOID;
  const label = `Tract ${feature.properties.GEOID}`;
  elements.selectionLabel.textContent = label;
  map.setFilter("tract-outline", ["==", ["get", "GEOID"], STATE.selectedGeoid]);
  if (popup) {
    popup.remove();
  }
  const props = feature.properties;
  const value = props.current_value;
  popup = new maplibregl.Popup({ closeButton: false })
    .setLngLat(event.lngLat)
    .setHTML(
      `<strong>${label}</strong><br/>Value: ${
        typeof value === "number" ? formatNumber(value) : value
      }`
    )
    .addTo(map);
}

function updateAcsData() {
  tractsData.features.forEach((feature) => {
    const record = acsLookup[STATE.year]?.[feature.properties.GEOID];
    if (!record) {
      feature.properties.current_value = null;
      return;
    }
    const value = +record[STATE.acsField];
    feature.properties.current_value = Number.isFinite(value) ? value : null;
  });
  map.getSource("tracts").setData(tractsData);
  updateAcsStyle();
}

function updateAcsStyle() {
  const values = tractsData.features
    .filter((f) => filterRedev(f))
    .map((f) => f.properties.current_value)
    .filter((v) => v !== null && !Number.isNaN(v));

  if (!values.length) return;
  const scale = d3.scaleQuantile().domain(values).range([
    "#eff3ff",
    "#bdd7e7",
    "#6baed6",
    "#3182bd",
    "#08519c",
  ]);
  const breaks = scale.quantiles();
  const colors = scale.range();

  map.setPaintProperty("tract-fill-acs", "fill-color", [
    "step",
    ["get", "current_value"],
    colors[0],
    breaks[0],
    colors[1],
    breaks[1],
    colors[2],
    breaks[2],
    colors[3],
    breaks[3],
    colors[4],
  ]);

  updateLegend(breaks, colors, ACS_FIELDS.find((f) => f.id === STATE.acsField));
}

function updateLegend(breaks, colors, field) {
  elements.legend.innerHTML = "";
  const title = document.createElement("h3");
  title.textContent = field ? field.label : "Legend";
  elements.legend.appendChild(title);
  const labels = [
    `< ${formatNumber(breaks[0])}`,
    `${formatNumber(breaks[0])} - ${formatNumber(breaks[1])}`,
    `${formatNumber(breaks[1])} - ${formatNumber(breaks[2])}`,
    `${formatNumber(breaks[2])} - ${formatNumber(breaks[3])}`,
    `> ${formatNumber(breaks[3])}`,
  ];
  colors.forEach((color, idx) => {
    const row = document.createElement("div");
    row.className = "legend-row";
    const swatch = document.createElement("span");
    swatch.className = "legend-swatch";
    swatch.style.background = color;
    const label = document.createElement("span");
    label.textContent = labels[idx];
    row.appendChild(swatch);
    row.appendChild(label);
    elements.legend.appendChild(row);
  });
}

function filterRedev(feature) {
  if (STATE.selectedRedev === "All") return true;
  return feature.properties.redev_name === STATE.selectedRedev;
}

function updateFilters() {
  if (STATE.selectedRedev === "All") {
    map.setFilter("tract-fill-acs", null);
    map.setFilter("tract-fill-lodes", null);
    map.setFilter("lodes-dots", null);
  } else {
    map.setFilter("tract-fill-acs", [
      "==",
      ["get", "redev_name"],
      STATE.selectedRedev,
    ]);
    map.setFilter("tract-fill-lodes", [
      "==",
      ["get", "redev_name"],
      STATE.selectedRedev,
    ]);
    map.setFilter("lodes-dots", [
      "==",
      ["get", "redev_name"],
      STATE.selectedRedev,
    ]);
  }
  updateAcsStyle();
}

function updateViewMode() {
  const isAcs = STATE.viewMode === "acs";
  const isLodes = STATE.viewMode === "lodes";
  const isDots = STATE.viewMode === "dots";
  map.setLayoutProperty(
    "tract-fill-acs",
    "visibility",
    isAcs ? "visible" : "none"
  );
  map.setLayoutProperty(
    "tract-fill-lodes",
    "visibility",
    isLodes ? "visible" : "none"
  );
  map.setLayoutProperty("lodes-dots", "visibility", isDots ? "visible" : "none");
  elements.acsField.disabled = !isAcs;
  elements.yearSlider.disabled = !isAcs;

  if (isLodes) {
    updateLodesStyle();
  } else if (isAcs) {
    updateAcsStyle();
  }
}

function updateLodesStyle() {
  const matchExpr = ["match", ["get", "dominant_sector"]];
  Object.entries(SECTOR_COLORS).forEach(([sector, color]) => {
    matchExpr.push(sector, color);
  });
  matchExpr.push("#cbd2d9");
  map.setPaintProperty("tract-fill-lodes", "fill-color", matchExpr);

  elements.legend.innerHTML = "<h3>Dominant sector</h3>";
  Object.entries(SECTOR_COLORS).forEach(([sector, color]) => {
    const row = document.createElement("div");
    row.className = "legend-row";
    const swatch = document.createElement("span");
    swatch.className = "legend-swatch";
    swatch.style.background = color;
    const label = document.createElement("span");
    label.textContent = sector.replace(/_/g, " ");
    row.appendChild(swatch);
    row.appendChild(label);
    elements.legend.appendChild(row);
  });
}

function downloadAcsCsv() {
  const rows = acsData.filter((row) => {
    const matchesYear = +row.year === STATE.year;
    const matchesRedev =
      STATE.selectedRedev === "All" || row.redev_name === STATE.selectedRedev;
    const matchesTract =
      !STATE.selectedGeoid || row.geoid === STATE.selectedGeoid;
    return matchesYear && matchesRedev && matchesTract;
  });
  const csv = d3.csvFormat(rows);
  downloadBlob(csv, `acs_${STATE.year}.csv`);
}

function downloadLodesCsv() {
  const rows = lodesSector.filter((row) => {
    const matchesRedev =
      STATE.selectedRedev === "All" || row.redev_name === STATE.selectedRedev;
    const matchesTract =
      !STATE.selectedGeoid || row.geoid === STATE.selectedGeoid;
    return matchesRedev && matchesTract;
  });
  const csv = d3.csvFormat(rows);
  downloadBlob(csv, "lodes_tract_sector.csv");
}

function downloadBlob(content, filename) {
  const blob = new Blob([content], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  URL.revokeObjectURL(url);
}

function attachLodesDominant() {
  const lookup = {};
  lodesDominant.forEach((row) => {
    lookup[row.geoid] = row.dominant_sector;
  });
  tractsData.features.forEach((feature) => {
    feature.properties.dominant_sector =
      lookup[feature.properties.GEOID] || null;
  });
}

Promise.all([
  d3.json(DATA_PATHS.tracts),
  d3.json(DATA_PATHS.redev),
  d3.csv(DATA_PATHS.acs),
  d3.csv(DATA_PATHS.lodesDominant),
  d3.json(DATA_PATHS.lodesDots),
  d3.csv(DATA_PATHS.lodesSector),
]).then((datasets) => {
  tractsData = datasets[0];
  redevData = datasets[1];
  acsData = datasets[2];
  lodesDominant = datasets[3];
  lodesDots = datasets[4];
  lodesSector = datasets[5];

  buildAcsLookup();
  attachLodesDominant();
  initializeUI();
  buildMap();
});
