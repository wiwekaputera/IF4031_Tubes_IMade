(function () {
  "use strict";

  const CATEGORIES = ["SEISMIC", "TSUNAMI", "VOLCANIC", "WILDFIRE", "COASTAL"];
  const CATEGORY_COLORS = {
    SEISMIC: "#2563eb",
    TSUNAMI: "#0891b2",
    VOLCANIC: "#dc2626",
    WILDFIRE: "#ea580c",
    COASTAL: "#0d9488",
  };
  const SEVERITY_ORDER = ["NORMAL", "RENDAH", "WASPADA", "SIAGA", "AWAS"];
  const SEVERITY_COLORS = {
    NORMAL: "#22c55e",
    RENDAH: "#22c55e",
    WASPADA: "#eab308",
    SIAGA: "#f97316",
    AWAS: "#ef4444",
  };

  let activePreset = "7d";
  let refreshTimer = null;
  let seriesChart = null;
  let severityChart = null;

  const els = {
    presetBtns: Array.from(document.querySelectorAll(".preset-btn")),
    fromInput: document.getElementById("from-input"),
    toInput: document.getElementById("to-input"),
    bucketSelect: document.getElementById("bucket-select"),
    applyBtn: document.getElementById("apply-btn"),
    refreshBtn: document.getElementById("refresh-btn"),
    autoRefresh: document.getElementById("auto-refresh"),
    partialBanner: document.getElementById("partial-banner"),
    partialMessage: document.getElementById("partial-message"),
    errorBanner: document.getElementById("error-banner"),
    errorMessage: document.getElementById("error-message"),
    windowBadge: document.getElementById("window-badge"),
    generatedBadge: document.getElementById("generated-badge"),
    bucketBadge: document.getElementById("bucket-badge"),
    areasTbody: document.getElementById("areas-tbody"),
    valTotal: document.getElementById("val-total"),
    cardTotal: document.getElementById("card-total"),
  };

  function toLocalInputValue(date) {
    const pad = (n) => String(n).padStart(2, "0");
    return (
      date.getFullYear() +
      "-" +
      pad(date.getMonth() + 1) +
      "-" +
      pad(date.getDate()) +
      "T" +
      pad(date.getHours()) +
      ":" +
      pad(date.getMinutes())
    );
  }

  function formatCount(n, isPartial) {
    const formatted = Number(n || 0).toLocaleString("id-ID");
    return isPartial ? "≥ " + formatted : formatted;
  }

  function formatTimestamp(iso) {
    if (!iso) return "—";
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return iso;
    return d.toLocaleString("id-ID", {
      year: "numeric",
      month: "short",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  }

  function formatBucketLabel(iso, bucket) {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return iso;
    if (bucket === "hour") {
      return d.toLocaleString("id-ID", {
        month: "short",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
      });
    }
    return d.toLocaleDateString("id-ID", {
      year: "numeric",
      month: "short",
      day: "2-digit",
    });
  }

  function setActivePreset(preset) {
    activePreset = preset;
    els.presetBtns.forEach((btn) => {
      const isMatch = btn.dataset.range === preset;
      btn.classList.toggle("is-active", isMatch);
      btn.setAttribute("aria-pressed", String(isMatch));
    });
    if (preset === "all") {
      els.fromInput.value = "";
      els.toInput.value = "";
      return;
    }
    if (preset) {
      const now = new Date();
      const hours = preset === "24h" ? 24 : preset === "30d" ? 24 * 30 : 24 * 7;
      const from = new Date(now.getTime() - hours * 3600 * 1000);
      els.fromInput.value = toLocalInputValue(from);
      els.toInput.value = toLocalInputValue(now);
    }
  }

  function buildQueryURL() {
    const params = new URLSearchParams();
    if (activePreset === "all") {
      params.set("range", "all");
    } else {
      if (els.fromInput.value) {
        params.set("from", new Date(els.fromInput.value).toISOString());
      }
      if (els.toInput.value) {
        params.set("to", new Date(els.toInput.value).toISOString());
      }
      if (!els.fromInput.value && activePreset) {
        params.set("range", activePreset);
      }
    }
    if (els.bucketSelect.value && els.bucketSelect.value !== "auto") {
      params.set("bucket", els.bucketSelect.value);
    }
    params.set("top_areas", "20");
    return "/internal/analytics/summary?" + params.toString();
  }

  function renderKPIs(data) {
    const partial = Boolean(data.partial);
    els.valTotal.textContent = formatCount(data.total, partial);
    els.cardTotal.classList.toggle("is-partial", partial);

    CATEGORIES.forEach((cat) => {
      const el = document.getElementById("val-" + cat);
      const card = document.getElementById("card-" + cat);
      const count = (data.by_category && data.by_category[cat]) || 0;
      if (el) el.textContent = formatCount(count, partial);
      if (card) card.classList.toggle("is-partial", partial);
    });
  }

  function renderPartialBanner(data) {
    if (data.partial) {
      const units = Array.isArray(data.unavailable_units) ? data.unavailable_units.join(", ") : "";
      els.partialMessage.textContent = "Data belum lengkap — unit tidak terjangkau: " + (units || "unknown");
      els.partialBanner.hidden = false;
    } else {
      els.partialBanner.hidden = true;
    }
  }

  function renderSeriesChart(data) {
    const canvas = document.getElementById("series-chart");
    if (!canvas || typeof Chart === "undefined") return;

    const tsSet = new Set();
    const seriesMap = data.series || {};
    CATEGORIES.forEach((cat) => {
      (seriesMap[cat] || []).forEach((pt) => tsSet.add(pt.t));
    });
    const sortedTimestamps = Array.from(tsSet).sort();
    const labels = sortedTimestamps.map((t) => formatBucketLabel(t, data.bucket));

    const datasets = CATEGORIES.map((cat) => {
      const lookup = new Map((seriesMap[cat] || []).map((pt) => [pt.t, pt.count]));
      return {
        label: cat,
        data: sortedTimestamps.map((t) => lookup.get(t) || 0),
        borderColor: CATEGORY_COLORS[cat],
        backgroundColor: CATEGORY_COLORS[cat] + "22",
        tension: 0.25,
        fill: false,
        pointRadius: 3,
      };
    });

    if (seriesChart) {
      seriesChart.data.labels = labels;
      seriesChart.data.datasets = datasets;
      seriesChart.update();
      return;
    }

    seriesChart = new Chart(canvas.getContext("2d"), {
      type: "line",
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        scales: {
          y: { beginAtZero: true, ticks: { precision: 0 } },
        },
      },
    });
  }

  function renderSeverityChart(data) {
    const canvas = document.getElementById("severity-chart");
    if (!canvas || typeof Chart === "undefined") return;

    const sevMap = data.by_severity || {};
    const labels = SEVERITY_ORDER.filter((s) => s !== "RENDAH" || (sevMap.RENDAH || 0) > 0);
    Object.keys(sevMap).forEach((k) => {
      if (!labels.includes(k) && sevMap[k] > 0) labels.push(k);
    });

    const values = labels.map((s) => sevMap[s] || 0);
    const colors = labels.map((s) => SEVERITY_COLORS[s] || "#64748b");

    if (severityChart) {
      severityChart.data.labels = labels;
      severityChart.data.datasets[0].data = values;
      severityChart.data.datasets[0].backgroundColor = colors;
      severityChart.update();
      return;
    }

    severityChart = new Chart(canvas.getContext("2d"), {
      type: "bar",
      data: {
        labels,
        datasets: [
          {
            label: "Jumlah Kejadian",
            data: values,
            backgroundColor: colors,
            borderRadius: 6,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          y: { beginAtZero: true, ticks: { precision: 0 } },
        },
      },
    });
  }

  function renderAreasTable(data) {
    const partial = Boolean(data.partial);
    const areas = Array.isArray(data.by_area) ? data.by_area : [];
    els.areasTbody.replaceChildren();

    if (areas.length === 0) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = 3;
      td.textContent = "Tidak ada data pada rentang waktu ini.";
      tr.appendChild(td);
      els.areasTbody.appendChild(tr);
      return;
    }

    areas.forEach((item, idx) => {
      const tr = document.createElement("tr");

      const rankTd = document.createElement("td");
      rankTd.textContent = String(idx + 1);

      const areaTh = document.createElement("th");
      areaTh.scope = "row";
      areaTh.textContent = item.area_name || "(tidak diketahui)";

      const countTd = document.createElement("td");
      countTd.className = "num-col";
      countTd.textContent = formatCount(item.count, partial);

      tr.append(rankTd, areaTh, countTd);
      els.areasTbody.appendChild(tr);
    });
  }

  async function loadSummary() {
    els.errorBanner.hidden = true;
    try {
      const res = await fetch(buildQueryURL(), { cache: "no-store" });
      if (!res.ok) {
        throw new Error("HTTP " + res.status);
      }
      const data = await res.json();

      const fromLabel = data.from ? formatTimestamp(data.from) : "Awal";
      els.windowBadge.textContent = "Rentang: " + fromLabel + " — " + formatTimestamp(data.to);
      els.generatedBadge.textContent = "Diperbarui: " + formatTimestamp(data.generated_at);
      els.bucketBadge.textContent = "Bucket: " + (data.bucket || "—");

      renderPartialBanner(data);
      renderKPIs(data);
      renderSeriesChart(data);
      renderSeverityChart(data);
      renderAreasTable(data);
    } catch (err) {
      els.errorMessage.textContent = "Gagal memuat data analitik (" + err.message + ").";
      els.errorBanner.hidden = false;
    }
  }

  els.presetBtns.forEach((btn) => {
    btn.addEventListener("click", () => {
      setActivePreset(btn.dataset.range);
      loadSummary();
    });
  });

  [els.fromInput, els.toInput].forEach((input) => {
    input.addEventListener("change", () => {
      setActivePreset("");
    });
  });

  els.applyBtn.addEventListener("click", loadSummary);
  els.refreshBtn.addEventListener("click", loadSummary);
  els.bucketSelect.addEventListener("change", loadSummary);

  els.autoRefresh.addEventListener("change", () => {
    if (refreshTimer) {
      clearInterval(refreshTimer);
      refreshTimer = null;
    }
    if (els.autoRefresh.checked) {
      refreshTimer = setInterval(loadSummary, 10000);
    }
  });

  setActivePreset("7d");
  loadSummary();
})();
