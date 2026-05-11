from __future__ import annotations

import re


def build_live_chart_html(
    *,
    server_url: str,
    symbol: str,
    pair_symbol: str | None,
    stoch_pct: int,
    cci_pct: int,
    rsi_pct: int,
    strategy_name: str,
    strategy_label: str,
    start_date: str,
    end_date: str,
    render_nonce: int,
) -> str:
    pair_query = pair_symbol or ""
    root_suffix = re.sub(
        r"[^a-zA-Z0-9_-]+",
        "-",
        f"{symbol}-{pair_query or 'none'}-{strategy_name}-{render_nonce}-{stoch_pct}-{cci_pct}-{rsi_pct}",
    )
    main_root_id = f"main-chart-root-{root_suffix}"
    indicator_root_id = f"indicator-chart-root-{root_suffix}"

    return f"""
<div style=\"display:flex;flex-direction:column;gap:10px;\">
  <div id=\"chart-status-{root_suffix}\" style=\"font-size:12px;color:#9aa4b2;margin:0 0 2px 6px;\"></div>
  <div id=\"chart-marker-filter-{root_suffix}\" style=\"display:flex;flex-wrap:wrap;gap:8px;margin:0 0 4px 6px;\"></div>
  <div id=\"{main_root_id}\" style=\"width:100%;height:400px;background:#131722;border:1px solid #2a2e39;border-radius:12px;\"></div>
  <div id=\"{indicator_root_id}\" style=\"width:100%;height:340px;background:#131722;border:1px solid #2a2e39;border-radius:12px;\"></div>
</div>
<script src=\"https://cdn.plot.ly/plotly-2.35.2.min.js\"></script>
<script>
const mainRoot = document.getElementById("{main_root_id}");
const indicatorRoot = document.getElementById("{indicator_root_id}");
const statusRoot = document.getElementById("chart-status-{root_suffix}");
const filterRoot = document.getElementById("chart-marker-filter-{root_suffix}");

const hostWindow = window.parent && window.parent.location ? window.parent : window;
const hostName = hostWindow.location.hostname || "127.0.0.1";
const proto = hostWindow.location.protocol || "http:";

const endpointBases = [
  `http://${{hostName}}:8766/v1/chart`,
  `http://${{hostName}}:8766/chart`,
  `${{proto}}//${{hostName}}/v1/chart`,
  `${{proto}}//${{hostName}}/chart`,
  "http://127.0.0.1:8766/v1/chart",
  "http://127.0.0.1:8766/chart",
];

const filterKey = "shinobu_marker_filters_{root_suffix}";
const filters = {{
  primary_open: true,
  primary_close: true,
  pair_open: true,
  pair_close: true,
  order_buy: true,
  order_sell: true,
}};

let mainReady = false;
let indicatorReady = false;
let prevPayload = null;
let timer = null;
let syncingRange = false;

function loadFilters() {{
  try {{
    const raw = sessionStorage.getItem(filterKey);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    Object.keys(filters).forEach((k) => {{
      if (typeof parsed[k] === "boolean") filters[k] = parsed[k];
    }});
  }} catch (_) {{}}
}}

function saveFilters() {{
  try {{ sessionStorage.setItem(filterKey, JSON.stringify(filters)); }} catch (_) {{}}
}}

function drawFilterUI() {{
  filterRoot.innerHTML = "";
  const title = document.createElement("span");
  title.textContent = "마커 표시:";
  title.style.color = "#94a3b8";
  title.style.fontSize = "12px";
  filterRoot.appendChild(title);

  const labels = [
    ["primary_open", "레버리지 Open"],
    ["primary_close", "레버리지 Close"],
    ["pair_open", "곱버스 Open"],
    ["pair_close", "곱버스 Close"],
    ["order_buy", "실매수"],
    ["order_sell", "실매도"],
  ];

  labels.forEach(([key, labelText]) => {{
    const label = document.createElement("label");
    label.style.display = "inline-flex";
    label.style.alignItems = "center";
    label.style.gap = "4px";
    label.style.fontSize = "12px";
    label.style.color = "#d1d5db";

    const input = document.createElement("input");
    input.type = "checkbox";
    input.checked = !!filters[key];
    input.addEventListener("change", async () => {{
      filters[key] = input.checked;
      saveFilters();
      await refreshCharts();
    }});

    const text = document.createElement("span");
    text.textContent = labelText;
    label.appendChild(input);
    label.appendChild(text);
    filterRoot.appendChild(label);
  }});
}}

function hoverCandle(item) {{
  const t = (item.t || "").replace("T", " ").slice(0, 16);
  return `시간 ${{t}}<br>시가 ${{Number(item.o).toLocaleString()}}<br>고가 ${{Number(item.h).toLocaleString()}}<br>저가 ${{Number(item.l).toLocaleString()}}<br>종가 ${{Number(item.c).toLocaleString()}}`;
}}

function detailHover(item) {{
  const out = [item.label || ""];
  if (item.time) out.push(`시간: ${{item.time}}`);
  if (item.price !== undefined && item.price !== null) out.push(`가격: ${{Number(item.price).toLocaleString()}}`);
  if (item.reason) out.push(`사유: ${{item.reason}}`);
  if (item.scr !== undefined && item.scr !== null) out.push(`SCR: ${{Number(item.scr).toFixed(2)}}`);
  return out.join("<br>");
}}

function markerTrace(items, color, symbol, showText=true) {{
  return {{
    type: "scatter",
    mode: showText ? "markers+text" : "markers",
    x: items.map((x) => x.x),
    y: items.map((x) => x.y),
    text: showText ? items.map((x) => x.label || "") : undefined,
    textposition: "top center",
    textfont: {{ size: 10, color }},
    marker: {{ color, size: 10, symbol, line: {{ color: "#fff", width: 1 }} }},
    hovertext: items.map((x) => detailHover(x)),
    hovertemplate: "%{{hovertext}}<extra></extra>",
    hoverinfo: "text",
    showlegend: false,
  }};
}}

function pickMarkers(payload) {{
  const sig = payload.signals || {{}};
  const orders = payload.orders || [];
  const out = {{
    primaryOpenMain: filters.primary_open ? (sig.primaryOpenMain || []) : [],
    primaryCloseMain: filters.primary_close ? (sig.primaryCloseMain || []) : [],
    pairOpenMain: filters.pair_open ? (sig.pairOpenMain || []) : [],
    pairCloseMain: filters.pair_close ? (sig.pairCloseMain || []) : [],
    primaryOpenIndicator: filters.primary_open ? (sig.primaryOpenIndicator || []) : [],
    primaryCloseIndicator: filters.primary_close ? (sig.primaryCloseIndicator || []) : [],
    pairOpenIndicator: filters.pair_open ? (sig.pairOpenIndicator || []) : [],
    pairCloseIndicator: filters.pair_close ? (sig.pairCloseIndicator || []) : [],
    orderBuy: filters.order_buy ? orders.filter((x) => x.side === "buy") : [],
    orderSell: filters.order_sell ? orders.filter((x) => x.side === "sell") : [],
  }};
  return out;
}}

function ticks(payload) {{
  const x = payload.candles.map((_, i) => i);
  const step = Math.max(1, Math.ceil(x.length / 8));
  const vals = x.filter((_, i) => i % step === 0 || i === x.length - 1);
  const txt = (payload.tickText || []).filter((_, i) => i % step === 0 || i === payload.tickText.length - 1);
  return {{ vals, txt }};
}}

function renderStatus(payload) {{
  if (!statusRoot) return;
  const c = payload.currentCandle || null;
  if (!c) {{
    statusRoot.innerHTML = "";
    return;
  }}
  const accent = c.isUnconfirmed ? "#f59e0b" : "#22c55e";
  const p = Math.max(0, Math.min(Number(c.progressPct || 0), 100));
  statusRoot.innerHTML =
    `<div style=\"display:flex;align-items:center;gap:10px;flex-wrap:wrap;\">` +
    `<span style=\"color:${{accent}};\">${{c.statusText || ""}}</span>` +
    `<span style=\"color:#64748b;\">기준 봉 ${{c.candleTime || "-"}}</span>` +
    `<div style=\"width:120px;height:6px;background:#1e293b;border-radius:999px;overflow:hidden;\">` +
    `<div style=\"width:${{p}}%;height:100%;background:${{accent}};\"></div>` +
    `</div></div>`;
}}

async function fetchPayload(includeMarkers=true) {{
  let lastErr = null;
  for (const base of endpointBases) {{
    const url =
      `${{base}}?kind=overlay&symbol={symbol}` +
      `&pair_symbol={pair_query}&stoch_pct={stoch_pct}&cci_pct={cci_pct}&rsi_pct={rsi_pct}` +
      `&strategy_name={strategy_name}&start_date={start_date}&end_date={end_date}` +
      `&include_markers=${{includeMarkers ? "1" : "0"}}`;
    try {{
      const res = await fetch(url, {{ cache: "no-store" }});
      if (!res.ok) throw new Error(`HTTP ${{res.status}}`);
      const ct = String(res.headers.get("content-type") || "").toLowerCase();
      if (!ct.includes("application/json")) throw new Error(`Unexpected content-type: ${{ct}}`);
      return await res.json();
    }} catch (e) {{
      lastErr = e;
    }}
  }}
  throw lastErr || new Error("fetch failed");
}}

function buildMain(payload) {{
  const m = pickMarkers(payload);
  const x = payload.candles.map((_, i) => i);
  const tk = ticks(payload);
  const hover = payload.candles.map((c) => hoverCandle(c));
  return {{
    data: [
      {{
        type: "candlestick",
        x,
        open: payload.candles.map((c) => c.o),
        high: payload.candles.map((c) => c.h),
        low: payload.candles.map((c) => c.l),
        close: payload.candles.map((c) => c.c),
        whiskerwidth: 1,
        text: hover,
        hovertext: hover,
        hovertemplate: "%{{hovertext}}<extra></extra>",
        increasing: {{ line: {{ color: "#089981" }}, fillcolor: "#089981" }},
        decreasing: {{ line: {{ color: "#f23645" }}, fillcolor: "#f23645" }},
        showlegend: false,
      }},
      markerTrace(m.primaryOpenMain, "#3b82f6", "circle"),
      markerTrace(m.primaryCloseMain, "#ef4444", "circle"),
      markerTrace(m.pairOpenMain, "#3b82f6", "star"),
      markerTrace(m.pairCloseMain, "#ef4444", "star"),
      markerTrace(m.orderBuy, "#22c55e", "diamond"),
      markerTrace(m.orderSell, "#f59e0b", "diamond"),
    ],
    layout: {{
      paper_bgcolor: "#131722",
      plot_bgcolor: "#131722",
      font: {{ color: "#d1d4dc", family: "Malgun Gothic" }},
      margin: {{ l: 8, r: 56, t: 42, b: 18 }},
      height: 400,
      dragmode: "pan",
      hovermode: "closest",
      showlegend: false,
      uirevision: "shinobu-main-chart",
      xaxis: {{
        tickmode: "array",
        tickvals: tk.vals,
        ticktext: tk.txt,
        showgrid: false,
        range: [-0.01, Math.max(x.length - 0.99, 1)],
        rangeslider: {{ visible: false }},
      }},
      yaxis: {{ side: "right", showgrid: true, gridcolor: "rgba(42,46,57,0.65)" }},
      annotations: [
        {{
          x: 0.01, y: 1.04, xref: "paper", yref: "paper", showarrow: false,
          text: `${{payload.symbolName}} · 5분봉 · 실전 가격`,
          font: {{ size: 14, color: "#e5e7eb", family: "Malgun Gothic" }},
        }},
        {{
          x: 0.99, y: 1.04, xref: "paper", yref: "paper", xanchor: "right", showarrow: false,
          text: "{strategy_label}",
          font: {{ size: 13, color: "#60a5fa", family: "Malgun Gothic" }},
        }},
      ],
    }},
  }};
}}

function buildIndicator(payload) {{
  const m = pickMarkers(payload);
  const x = payload.candles.map((_, i) => i);
  const tk = ticks(payload);
  const times = payload.candles.map((c) => String(c.t || "").replace("T", " ").slice(0, 16));
  return {{
    data: [
      markerTrace(m.primaryOpenMain, "#3b82f6", "circle", false),
      markerTrace(m.primaryCloseMain, "#ef4444", "circle", false),
      markerTrace(m.pairOpenMain, "#3b82f6", "star", false),
      markerTrace(m.pairCloseMain, "#ef4444", "star", false),
      markerTrace(m.orderBuy, "#22c55e", "diamond", false),
      markerTrace(m.orderSell, "#f59e0b", "diamond", false),
      markerTrace(m.primaryOpenIndicator, "#3b82f6", "circle", false),
      markerTrace(m.primaryCloseIndicator, "#ef4444", "circle", false),
      markerTrace(m.pairOpenIndicator, "#3b82f6", "star", false),
      markerTrace(m.pairCloseIndicator, "#ef4444", "star", false),
      {{
        type: "scatter", mode: "lines", x, y: payload.scr || [], customdata: times,
        line: {{ color: "#ffffff", width: 4.2, dash: "solid" }},
        hovertemplate: `시간 %{{customdata}}<br>${{payload.symbolName}} SCR %{{y:.2f}}<extra></extra>`,
        showlegend: false,
      }},
      {{
        type: "scatter", mode: "lines", x, y: payload.pairScr || [], customdata: times,
        line: {{ color: "#f59e0b", width: 3.5, dash: "dot" }},
        hovertemplate: `시간 %{{customdata}}<br>${{payload.pairName || "곱버스"}} SCR %{{y:.2f}}<extra></extra>`,
        showlegend: false,
      }},
    ],
    layout: {{
      paper_bgcolor: "#131722",
      plot_bgcolor: "#131722",
      font: {{ color: "#d1d4dc", family: "Malgun Gothic" }},
      margin: {{ l: 16, r: 64, t: 36, b: 56 }},
      height: 340,
      dragmode: false,
      hovermode: "closest",
      showlegend: false,
      uirevision: "shinobu-indicator-chart",
      xaxis: {{ tickmode: "array", tickvals: tk.vals, ticktext: tk.txt, showgrid: false, fixedrange: true, automargin: true }},
      yaxis: {{ side: "right", range: [-2.4, 2.4], tickmode: "array", tickvals: [-1,0,1], ticktext: ["하단","0","상단"], showgrid: true, gridcolor: "rgba(42,46,57,0.35)", fixedrange: true, automargin: true }},
      annotations: [
        {{
          x: 0.01, y: 1.08, xref: "paper", yref: "paper", showarrow: false,
          text: "보조지표 (흰 실선: 레버리지 / 주황 점선: 곱버스)",
          font: {{ size: 12, color: "#9aa4b2", family: "Malgun Gothic" }},
        }},
      ],
    }},
  }};
}}

async function refreshCharts() {{
  try {{
    const payload = await fetchPayload(true);
    renderStatus(payload);
    const config = {{ responsive: true, displaylogo: false, displayModeBar: false, scrollZoom: true }};

    const main = buildMain(payload);
    const ind = buildIndicator(payload);

    if (!mainReady) {{
      await Plotly.newPlot(mainRoot, main.data, main.layout, config);
      mainReady = true;
      mainRoot.on("plotly_relayout", (eventData) => {{
        if (syncingRange) return;
        const x0 = eventData["xaxis.range[0]"];
        const x1 = eventData["xaxis.range[1]"];
        if (x0 === undefined || x1 === undefined) return;
        if (!indicatorReady) return;
        syncingRange = true;
        Plotly.relayout(indicatorRoot, {{"xaxis.range": [x0, x1]}})
          .finally(() => {{
            syncingRange = false;
          }});
      }});
    }} else {{
      await Plotly.react(mainRoot, main.data, main.layout, config);
    }}

    if (!indicatorReady) {{
      await Plotly.newPlot(indicatorRoot, ind.data, ind.layout, config);
      indicatorReady = true;
    }} else {{
      await Plotly.react(indicatorRoot, ind.data, ind.layout, config);
    }}

    // Keep indicator x-range aligned with current main x-range.
    const mainRange = mainRoot.layout?.xaxis?.range;
    if (indicatorReady && mainRange && mainRange.length === 2) {{
      syncingRange = true;
      await Plotly.relayout(indicatorRoot, {{"xaxis.range": mainRange}})
        .finally(() => {{
          syncingRange = false;
        }});
    }}

    prevPayload = payload;
  }} catch (e) {{
    console.error("refreshCharts failed", e);
  }}
}}

loadFilters();
drawFilterUI();
refreshCharts();
if (timer) clearInterval(timer);
timer = setInterval(refreshCharts, 5000);
</script>
"""
