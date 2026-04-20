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
    root_suffix = re.sub(r"[^a-zA-Z0-9_-]+", "-", f"{symbol}-{pair_query or 'none'}-{strategy_name}-{render_nonce}-{stoch_pct}-{cci_pct}-{rsi_pct}")
    main_root_id = f"main-chart-root-{root_suffix}"
    indicator_root_id = f"indicator-chart-root-{root_suffix}"
    return f"""
<div style="display:flex;flex-direction:column;gap:10px;">
  <div style="display:none" data-strategy-name="{strategy_name}" data-strategy-label="{strategy_label}" data-render-nonce="{render_nonce}"></div>
  <div id="chart-status-{root_suffix}" style="font-size:12px;color:#9aa4b2;margin:0 0 2px 6px;"></div>
  <div id="chart-marker-filter-{root_suffix}" style="display:flex;flex-wrap:wrap;gap:8px;margin:0 0 4px 6px;"></div>
  <div id="{main_root_id}" style="width:100%;height:400px;background:#131722;border:1px solid #2a2e39;border-radius:12px;"></div>
  <div id="{indicator_root_id}" style="width:100%;height:300px;background:#131722;border:1px solid #2a2e39;border-radius:12px;"></div>
</div>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<script>
const mainRoot = document.getElementById("{main_root_id}");
const indicatorRoot = document.getElementById("{indicator_root_id}");
const chartStatusRoot = document.getElementById("chart-status-{root_suffix}");
const markerFilterRoot = document.getElementById("chart-marker-filter-{root_suffix}");
const hostWindow = window.parent && window.parent.location ? window.parent : window;
const hostName = hostWindow.location.hostname || "";
const hostPort = hostWindow.location.port || "";
const isLocalHost = ["localhost", "127.0.0.1"].includes(hostName);
const isIpv4Host = /^(\\d{1,3}\\.){3}\\d{1,3}$/.test(hostName);
const isDirectStreamlit = hostPort === "8501";
const isDirectApi = isLocalHost || isIpv4Host || isDirectStreamlit;
const chartEndpointBases = isDirectApi
  ? [
      `${{hostWindow.location.protocol}}//${{hostName}}:8766/v1/chart`,
      `${{hostWindow.location.protocol}}//${{hostName}}:8766/chart`
    ]
  : [
      "https://shinobu-chart.ukin.dev/v1/chart"
    ];
const refreshTimerKey = "__shinobu_chart_refresh_{root_suffix}";
const markerFilterStorageKey = "shinobu_marker_filters_v1_{root_suffix}";
const markerFilterOptions = [
  {{ key: "primary_open", label: "레버리지 Open" }},
  {{ key: "primary_close", label: "레버리지 Close" }},
  {{ key: "pair_open", label: "곱버스 Open" }},
  {{ key: "pair_close", label: "곱버스 Close" }},
  {{ key: "stop_loss", label: "손절" }},
  {{ key: "order_buy", label: "실매수" }},
  {{ key: "order_sell", label: "실매도" }}
];
const markerFilters = {{
  primary_open: true,
  primary_close: true,
  pair_open: true,
  pair_close: true,
  stop_loss: true,
  order_buy: true,
  order_sell: true
}};

let initializedMain = false;
let initializedIndicator = false;
let syncingRange = false;
let previousPayload = null;
let latestMarkerPayload = null;
let countdownTimer = null;
let liveCountdownState = null;

const MAIN_TRACE = {{
  candle: 0,
  primaryOpen: 1,
  primaryClose: 2,
  pairOpen: 3,
  pairClose: 4,
  orderBuy: 5,
  orderSell: 6
}};

const INDICATOR_TRACE = {{
  primaryOpenMain: 0,
  primaryCloseMain: 1,
  pairOpenMain: 2,
  pairCloseMain: 3,
  orderBuy: 4,
  orderSell: 5,
  primaryOpen: 6,
  primaryClose: 7,
  pairOpen: 8,
  pairClose: 9,
  primaryScr: 10,
  pairScr: 11
}};

function detailHover(items) {{
  return items.map((item) =>
    [
      item.label || "",
      item.time ? `시간: ${{item.time}}` : "",
      item.price ? `가격: ${{Number(item.price).toLocaleString()}}` : "",
      item.reason ? `사유: ${{item.reason}}` : "",
      item.scr !== undefined ? `SCR: ${{Number(item.scr).toFixed(2)}}` : ""
    ].filter(Boolean).join("<br>")
  );
}}

function normalizeMarkerText(value) {{
  return String(value || "").toLowerCase();
}}

function isStopMarker(item) {{
  const label = normalizeMarkerText(item?.label);
  const reason = normalizeMarkerText(item?.reason);
  return (
    label.includes("손절") ||
    reason.includes("손절") ||
    label.includes("stop") ||
    reason.includes("stop") ||
    label.includes("trailing") ||
    reason.includes("trailing")
  );
}}

function filterOpenMarkers(markers, key) {{
  if (!markerFilters[key]) return [];
  return markers;
}}

function filterCloseMarkers(markers, closeKey) {{
  const showClose = Boolean(markerFilters[closeKey]);
  const showStop = Boolean(markerFilters.stop_loss);
  return markers.filter((item) => {{
    if (isStopMarker(item)) return showStop;
    return showClose;
  }});
}}

function loadMarkerFilters() {{
  try {{
    const raw = window.sessionStorage.getItem(markerFilterStorageKey);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return;
    markerFilterOptions.forEach((option) => {{
      if (typeof parsed[option.key] === "boolean") {{
        markerFilters[option.key] = parsed[option.key];
      }}
    }});
  }} catch (error) {{
    // ignore local storage parsing errors
  }}
}}

function saveMarkerFilters() {{
  try {{
    window.sessionStorage.setItem(markerFilterStorageKey, JSON.stringify(markerFilters));
  }} catch (error) {{
    // ignore local storage write errors
  }}
}}

async function applyMarkerFiltersOnly() {{
  const markerPayload = latestMarkerPayload || previousPayload;
  if (!markerPayload) return;
  if (initializedMain) {{
    const markers = markerSeries(markerPayload).main;
    for (let i = 0; i < markers.length; i += 1) {{
      const traceIndex = MAIN_TRACE.primaryOpen + i;
      const current = markers[i];
      await Plotly.restyle(
        mainRoot,
        {{
          x: [current.map((item) => item.x)],
          y: [current.map((item) => item.y)],
          text: [current.map((item) => item.label)],
          hovertext: [detailHover(current)]
        }},
        [traceIndex]
      );
    }}
  }}
  if (initializedIndicator) {{
    const markers = markerSeries(markerPayload).indicator;
    for (let i = 0; i < markers.length; i += 1) {{
      const current = markers[i];
      await Plotly.restyle(
        indicatorRoot,
        {{
          x: [current.map((item) => item.x)],
          y: [current.map((item) => item.y)],
          hovertext: [detailHover(current)]
        }},
        [i]
      );
    }}
  }}
}}

function renderMarkerFilterControls() {{
  if (!markerFilterRoot) return;
  markerFilterRoot.innerHTML = "";
  const title = document.createElement("span");
  title.textContent = "마커 표시:";
  title.style.color = "#94a3b8";
  title.style.fontSize = "12px";
  title.style.marginRight = "4px";
  markerFilterRoot.appendChild(title);

  markerFilterOptions.forEach((option) => {{
    const label = document.createElement("label");
    label.style.display = "inline-flex";
    label.style.alignItems = "center";
    label.style.gap = "4px";
    label.style.fontSize = "12px";
    label.style.color = "#d1d5db";
    label.style.cursor = "pointer";

    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.checked = Boolean(markerFilters[option.key]);
    checkbox.style.margin = "0";
    checkbox.addEventListener("change", async () => {{
      markerFilters[option.key] = checkbox.checked;
      saveMarkerFilters();
      await applyMarkerFiltersOnly();
    }});

    const text = document.createElement("span");
    text.textContent = option.label;

    label.appendChild(checkbox);
    label.appendChild(text);
    markerFilterRoot.appendChild(label);
  }});
}}

function renderCurrentCandleStatus(payload) {{
  if (!chartStatusRoot) return;
  const current = payload.currentCandle || null;
  if (!current) {{
    chartStatusRoot.innerHTML = "";
    return;
  }}
  const accent = current.isUnconfirmed ? "#f59e0b" : "#22c55e";
  const progress = Math.max(0, Math.min(Number(current.progressPct || 0), 100));
  chartStatusRoot.innerHTML =
    `<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">` +
    `<span style="color:${{accent}};">${{current.statusText || ""}}</span>` +
    `<span style="color:#64748b;">기준 봉 ${{
      current.candleTime || "-"
    }}</span>` +
    `<div style="width:120px;height:6px;background:#1e293b;border-radius:999px;overflow:hidden;">` +
    `<div style="width:${{progress}}%;height:100%;background:${{accent}};"></div>` +
    `</div>` +
    `</div>`;
}}

function renderCurrentCandleStatusFromState(current) {{
  if (!chartStatusRoot || !current) return;
  const accent = current.isUnconfirmed ? "#f59e0b" : "#22c55e";
  const progress = Math.max(0, Math.min(Number(current.progressPct || 0), 100));
  chartStatusRoot.innerHTML =
    `<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">` +
    `<span style="color:${{accent}};">${{current.statusText || ""}}</span>` +
    `<span style="color:#64748b;">기준 봉 ${{
      current.candleTime || "-"
    }}</span>` +
    `<div style="width:120px;height:6px;background:#1e293b;border-radius:999px;overflow:hidden;">` +
    `<div style="width:${{progress}}%;height:100%;background:${{accent}};"></div>` +
    `</div>` +
    `</div>`;
}}

function startCurrentCandleCountdown(payload) {{
  if (countdownTimer) {{
    clearInterval(countdownTimer);
    countdownTimer = null;
  }}
  const current = payload.currentCandle || null;
  if (!current) {{
    liveCountdownState = null;
    renderCurrentCandleStatus(payload);
    return;
  }}

  liveCountdownState = {{ ...current }};
  renderCurrentCandleStatusFromState(liveCountdownState);
  if (!liveCountdownState.isUnconfirmed) {{
    return;
  }}

  countdownTimer = setInterval(() => {{
    if (!liveCountdownState || !liveCountdownState.isUnconfirmed) {{
      clearInterval(countdownTimer);
      countdownTimer = null;
      return;
    }}
    const nextRemaining = Math.max(0, Number(liveCountdownState.remainingSeconds || 0) - 1);
    liveCountdownState.remainingSeconds = nextRemaining;
    liveCountdownState.remainingText =
      `${{String(Math.floor(nextRemaining / 60)).padStart(2, "0")}}:${{String(nextRemaining % 60).padStart(2, "0")}}`;
    const nextProgress = Math.max(0, Math.min(100, 100 - (nextRemaining / 300) * 100));
    liveCountdownState.progressPct = nextProgress;
    if (nextRemaining <= 0) {{
      liveCountdownState.isUnconfirmed = false;
      liveCountdownState.statusText = "최근 봉 확정";
      clearInterval(countdownTimer);
      countdownTimer = null;
    }} else {{
      liveCountdownState.statusText = `현재 봉 미확정 · 마감까지 ${{liveCountdownState.remainingText}}`;
    }}
    renderCurrentCandleStatusFromState(liveCountdownState);
  }}, 1000);
}}

function mainMarkerTrace(markers, color, symbol) {{
  return {{
    type: "scatter",
    mode: "markers+text",
    x: markers.map((item) => item.x),
    y: markers.map((item) => item.y),
    text: markers.map((item) => item.label),
    textposition: "top center",
    textfont: {{ size: 10, color }},
    marker: {{ color, size: 10, symbol, line: {{ color: "#ffffff", width: 1 }} }},
    hoverinfo: "text",
    hovertext: detailHover(markers),
    hovertemplate: "%{{hovertext}}<extra></extra>",
    showlegend: false
  }};
}}

function indicatorMarkerTrace(markers, color, symbol) {{
  return {{
    type: "scatter",
    mode: "markers",
    x: markers.map((item) => item.x),
    y: markers.map((item) => item.y),
    marker: {{ color, size: 10, opacity: 0.82, symbol, line: {{ color: "#ffffff", width: 1.4 }} }},
    hoverinfo: "text",
    hovertext: detailHover(markers),
    hovertemplate: "%{{hovertext}}<extra></extra>",
    showlegend: false
  }};
}}

function candleArrays(payload) {{
  return {{
    x: payload.candles.map((_, index) => index),
    open: payload.candles.map((item) => item.o),
    high: payload.candles.map((item) => item.h),
    low: payload.candles.map((item) => item.l),
    close: payload.candles.map((item) => item.c),
    times: payload.candles.map((item) => item.t)
  }};
}}

function markerSeries(payload) {{
  const orders = payload.orders || [];
  const buyOrders = markerFilters.order_buy ? orders.filter((item) => item.side === "buy") : [];
  const sellOrders = markerFilters.order_sell ? orders.filter((item) => item.side === "sell") : [];
  const primaryOpenMain = filterOpenMarkers(payload.signals.primaryOpenMain || [], "primary_open");
  const primaryCloseMain = filterCloseMarkers(payload.signals.primaryCloseMain || [], "primary_close");
  const pairOpenMain = filterOpenMarkers(payload.signals.pairOpenMain || [], "pair_open");
  const pairCloseMain = filterCloseMarkers(payload.signals.pairCloseMain || [], "pair_close");
  const primaryOpenIndicator = filterOpenMarkers(payload.signals.primaryOpenIndicator || [], "primary_open");
  const primaryCloseIndicator = filterCloseMarkers(payload.signals.primaryCloseIndicator || [], "primary_close");
  const pairOpenIndicator = filterOpenMarkers(payload.signals.pairOpenIndicator || [], "pair_open");
  const pairCloseIndicator = filterCloseMarkers(payload.signals.pairCloseIndicator || [], "pair_close");
  return {{
    main: [
      primaryOpenMain,
      primaryCloseMain,
      pairOpenMain,
      pairCloseMain,
      buyOrders,
      sellOrders
    ],
    indicator: [
      primaryOpenMain,
      primaryCloseMain,
      pairOpenMain,
      pairCloseMain,
      buyOrders,
      sellOrders,
      primaryOpenIndicator,
      primaryCloseIndicator,
      pairOpenIndicator,
      pairCloseIndicator
    ]
  }};
}}

function withMarkers(basePayload, markerPayload) {{
  if (!basePayload) return markerPayload;
  if (!markerPayload) return basePayload;
  return {{
    ...basePayload,
    orders: markerPayload.orders || [],
    signals: markerPayload.signals || {{}}
  }};
}}

function tickData(payload) {{
  const x = payload.candles.map((_, index) => index);
  const step = Math.max(1, Math.ceil(x.length / 8));
  return {{
    tickvals: x.filter((_, index) => index % step === 0 || index === x.length - 1),
    ticktext: payload.tickText.filter((_, index) => index % step === 0 || index === x.length - 1)
  }};
}}

function buildMainFigure(payload) {{
  const candle = candleArrays(payload);
  const ticks = tickData(payload);
  const markers = markerSeries(payload).main;
  const candleHoverText = payload.candles.map((item) => {{
    const timeText = (item.t || "").replace("T", " ").slice(0, 16);
    return `시간 ${{timeText}}<br>시가 ${{Number(item.o).toLocaleString()}}<br>고가 ${{Number(item.h).toLocaleString()}}<br>저가 ${{Number(item.l).toLocaleString()}}<br>종가 ${{Number(item.c).toLocaleString()}}`;
  }});
  return {{
    data: [
      {{
        type: "candlestick",
        x: candle.x,
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close,
        text: candleHoverText,
        increasing: {{ line: {{ color: "#089981" }}, fillcolor: "#089981" }},
        decreasing: {{ line: {{ color: "#f23645" }}, fillcolor: "#f23645" }},
        hoverinfo: "text",
        hovertext: candleHoverText,
        hovertemplate: "%{{hovertext}}<extra></extra>",
        showlegend: false
      }},
      mainMarkerTrace(markers[0], "#3b82f6", "circle"),
      mainMarkerTrace(markers[1], "#ef4444", "circle"),
      mainMarkerTrace(markers[2], "#3b82f6", "star"),
      mainMarkerTrace(markers[3], "#ef4444", "star"),
      mainMarkerTrace(markers[4], "#22c55e", "heart"),
      mainMarkerTrace(markers[5], "#f59e0b", "heart")
    ],
    layout: {{
      paper_bgcolor: "#131722",
      plot_bgcolor: "#131722",
      font: {{ color: "#d1d4dc", family: "Malgun Gothic" }},
      margin: {{ l: 8, r: 56, t: 42, b: 18 }},
      height: 400,
      dragmode: "pan",
      hovermode: "closest",
      hoverlabel: {{ bgcolor: "#1e222d", font: {{ color: "#d1d4dc" }} }},
      showlegend: false,
      uirevision: "shinobu-main-chart",
      xaxis: {{
        tickmode: "array",
        tickvals: ticks.tickvals,
        ticktext: ticks.ticktext,
        showgrid: false,
        range: [-0.45, Math.max(candle.x.length - 0.55, 1)],
        fixedrange: false,
        rangeslider: {{ visible: false }}
      }},
      yaxis: {{
        side: "right",
        showgrid: true,
        gridcolor: "rgba(42,46,57,0.65)",
        fixedrange: false
      }},
      annotations: [
        {{
          x: 0.01,
          y: 1.04,
          xref: "paper",
          yref: "paper",
          showarrow: false,
          text: `${{payload.symbolName}} · 5분봉 · 실전 가격`,
          font: {{ size: 14, color: "#e5e7eb", family: "Malgun Gothic" }}
        }},
        {{
          x: 0.99,
          y: 1.04,
          xref: "paper",
          yref: "paper",
          xanchor: "right",
          showarrow: false,
          text: "{strategy_label}",
          font: {{ size: 13, color: "#60a5fa", family: "Malgun Gothic" }}
        }}
      ]
    }}
  }};
}}

function buildIndicatorFigure(payload) {{
  const candle = candleArrays(payload);
  const ticks = tickData(payload);
  const indicatorTimes = candle.times.map((item) => (item || "").replace("T", " ").slice(0, 16));
  const markers = markerSeries(payload).indicator;
  return {{
    data: [
      indicatorMarkerTrace(markers[0], "#3b82f6", "circle"),
      indicatorMarkerTrace(markers[1], "#ef4444", "circle"),
      indicatorMarkerTrace(markers[2], "#3b82f6", "star"),
      indicatorMarkerTrace(markers[3], "#ef4444", "star"),
      indicatorMarkerTrace(markers[4], "#22c55e", "heart"),
      indicatorMarkerTrace(markers[5], "#f59e0b", "heart"),
      indicatorMarkerTrace(markers[6], "#3b82f6", "circle"),
      indicatorMarkerTrace(markers[7], "#ef4444", "circle"),
      indicatorMarkerTrace(markers[8], "#3b82f6", "star"),
      indicatorMarkerTrace(markers[9], "#ef4444", "star"),
      {{
        type: "scatter",
        mode: "lines",
        x: candle.x,
        y: payload.scr || [],
        customdata: indicatorTimes,
        line: {{ color: "#ffffff", width: 4.2, dash: "solid" }},
        hovertemplate: `시간 %{{customdata}}<br>${{payload.symbolName}} SCR %{{y:.2f}}<extra></extra>`,
        showlegend: false
      }},
      {{
        type: "scatter",
        mode: "lines",
        x: candle.x,
        y: payload.pairScr || [],
        customdata: indicatorTimes,
        line: {{ color: "#f59e0b", width: 3.5, dash: "dot" }},
        hovertemplate: `시간 %{{customdata}}<br>${{payload.pairName || "곱버스"}} SCR %{{y:.2f}}<extra></extra>`,
        showlegend: false
      }}
    ],
    layout: {{
      paper_bgcolor: "#131722",
      plot_bgcolor: "#131722",
      font: {{ color: "#d1d4dc", family: "Malgun Gothic" }},
      margin: {{ l: 8, r: 56, t: 34, b: 22 }},
      height: 300,
      dragmode: false,
      hovermode: "closest",
      hoverdistance: 20,
      spikedistance: 20,
      hoverlabel: {{ bgcolor: "#1e222d", font: {{ color: "#d1d4dc" }} }},
      showlegend: false,
      uirevision: "shinobu-indicator-chart",
      xaxis: {{
        tickmode: "array",
        tickvals: ticks.tickvals,
        ticktext: ticks.ticktext,
        showgrid: false,
        range: [-0.45, Math.max(candle.x.length - 0.55, 1)],
        fixedrange: true,
        showspikes: true,
        spikemode: "across",
        spikecolor: "#4b5563",
        spikethickness: 1
      }},
      yaxis: {{
        side: "right",
        range: [-1.9, 1.9],
        tickmode: "array",
        tickvals: [-1, 0, 1],
        ticktext: ["하단", "0", "상단"],
        showgrid: true,
        gridcolor: "rgba(42,46,57,0.35)",
        fixedrange: true
      }},
      annotations: [
        {{
          x: 0.01,
          y: 1.08,
          xref: "paper",
          yref: "paper",
          showarrow: false,
          text: "보조지표 (흰 실선: 레버리지 / 주황 점선: 곱버스)",
          font: {{ size: 12, color: "#9aa4b2", family: "Malgun Gothic" }}
        }}
      ]
    }}
  }};
}}

function canAppend(prevPayload, nextPayload) {{
  if (!prevPayload || !nextPayload) return false;
  const prevCandles = prevPayload.candles || [];
  const nextCandles = nextPayload.candles || [];
  if (nextCandles.length !== prevCandles.length + 1) return false;
  for (let i = 0; i < prevCandles.length; i += 1) {{
    if (prevCandles[i].t !== nextCandles[i].t) return false;
  }}
  return true;
}}

async function syncIndicatorRangeFromMain() {{
  const currentMainRange = mainRoot.layout?.xaxis?.range;
  if (!currentMainRange || currentMainRange.length !== 2) return;
  syncingRange = true;
  await Plotly.relayout(indicatorRoot, {{ "xaxis.range": currentMainRange }});
  syncingRange = false;
}}

async function applyMainIncremental(prevPayload, nextPayload) {{
  const nextCandles = candleArrays(nextPayload);

  if (canAppend(prevPayload, nextPayload)) {{
    const newIndex = nextCandles.x[nextCandles.x.length - 1];
    await Plotly.extendTraces(
      mainRoot,
      {{
        x: [[newIndex]],
        open: [[nextCandles.open[nextCandles.open.length - 1]]],
        high: [[nextCandles.high[nextCandles.high.length - 1]]],
        low: [[nextCandles.low[nextCandles.low.length - 1]]],
        close: [[nextCandles.close[nextCandles.close.length - 1]]]
      }},
      [MAIN_TRACE.candle],
      nextCandles.x.length
    );
  }} else {{
    const candleHoverText = nextPayload.candles.map((item) => {{
      const timeText = (item.t || "").replace("T", " ").slice(0, 16);
      return `시간 ${{timeText}}<br>시가 ${{Number(item.o).toLocaleString()}}<br>고가 ${{Number(item.h).toLocaleString()}}<br>저가 ${{Number(item.l).toLocaleString()}}<br>종가 ${{Number(item.c).toLocaleString()}}`;
    }});
    await Plotly.restyle(
      mainRoot,
      {{
        x: [nextCandles.x],
        open: [nextCandles.open],
        high: [nextCandles.high],
        low: [nextCandles.low],
        close: [nextCandles.close],
        hovertext: [candleHoverText],
        text: [candleHoverText]
      }},
      [MAIN_TRACE.candle]
    );
  }}

  const ticks = tickData(nextPayload);
  await Plotly.relayout(mainRoot, {{
    "xaxis.tickvals": ticks.tickvals,
    "xaxis.ticktext": ticks.ticktext
  }});
}}

async function applyIndicatorIncremental(prevPayload, nextPayload) {{
  const nextCandles = candleArrays(nextPayload);
  const indicatorTimes = nextCandles.times.map((item) => (item || "").replace("T", " ").slice(0, 16));

  if (canAppend(prevPayload, nextPayload)) {{
    const newIndex = nextCandles.x[nextCandles.x.length - 1];
    await Plotly.extendTraces(
      indicatorRoot,
      {{
        x: [[newIndex]],
        y: [[(nextPayload.scr || [])[nextPayload.scr.length - 1]]],
        customdata: [[indicatorTimes[indicatorTimes.length - 1]]]
      }},
      [INDICATOR_TRACE.primaryScr],
      nextCandles.x.length
    );
    await Plotly.extendTraces(
      indicatorRoot,
      {{
        x: [[newIndex]],
        y: [[(nextPayload.pairScr || [])[nextPayload.pairScr.length - 1]]],
        customdata: [[indicatorTimes[indicatorTimes.length - 1]]]
      }},
      [INDICATOR_TRACE.pairScr],
      nextCandles.x.length
    );
  }} else {{
    await Plotly.restyle(
      indicatorRoot,
      {{
        x: [nextCandles.x],
        y: [nextPayload.scr || []],
        customdata: [indicatorTimes]
      }},
      [INDICATOR_TRACE.primaryScr]
    );
    await Plotly.restyle(
      indicatorRoot,
      {{
        x: [nextCandles.x],
        y: [nextPayload.pairScr || []],
        customdata: [indicatorTimes]
      }},
      [INDICATOR_TRACE.pairScr]
    );
  }}

  const ticks = tickData(nextPayload);
  await Plotly.relayout(indicatorRoot, {{
    "xaxis.tickvals": ticks.tickvals,
    "xaxis.ticktext": ticks.ticktext
  }});
}}

async function applyMarkerPayload(basePayload, markerPayload) {{
  if (!basePayload || !markerPayload) return;
  latestMarkerPayload = withMarkers(basePayload, markerPayload);
  const markers = markerSeries(latestMarkerPayload);
  if (initializedMain) {{
    for (let i = 0; i < markers.main.length; i += 1) {{
      const traceIndex = MAIN_TRACE.primaryOpen + i;
      const current = markers.main[i];
      await Plotly.restyle(
        mainRoot,
        {{
          x: [current.map((item) => item.x)],
          y: [current.map((item) => item.y)],
          text: [current.map((item) => item.label)],
          hovertext: [detailHover(current)]
        }},
        [traceIndex]
      );
    }}
  }}
  if (initializedIndicator) {{
    for (let i = 0; i < markers.indicator.length; i += 1) {{
      const current = markers.indicator[i];
      await Plotly.restyle(
        indicatorRoot,
        {{
          x: [current.map((item) => item.x)],
          y: [current.map((item) => item.y)],
          hovertext: [detailHover(current)]
        }},
        [i]
      );
    }}
  }}
}}

async function fetchPayload(includeMarkers) {{
  let lastError = null;
  for (const base of chartEndpointBases) {{
    const endpoint =
      `${{base}}?kind=overlay&symbol={symbol}` +
      `&pair_symbol={pair_query}&stoch_pct={stoch_pct}&cci_pct={cci_pct}&rsi_pct={rsi_pct}` +
      `&strategy_name={strategy_name}&start_date={start_date}&end_date={end_date}` +
      `&include_markers=${{includeMarkers ? "1" : "0"}}`;
    try {{
      const response = await fetch(endpoint, {{ cache: "no-store" }});
      if (!response.ok) {{
        throw new Error(`HTTP ${{response.status}}`);
      }}
      const contentType = String(response.headers.get("content-type") || "").toLowerCase();
      if (!contentType.includes("application/json")) {{
        throw new Error(`Unexpected content-type: ${{contentType || "unknown"}}`);
      }}
      return response.json();
    }} catch (error) {{
      lastError = error;
    }}
  }}
  throw lastError || new Error("Failed to fetch chart payload");
}}

async function refreshCharts() {{
  const nextPayload = await fetchPayload(true);
  startCurrentCandleCountdown(nextPayload);
  const config = {{
    responsive: true,
    displaylogo: false,
    displayModeBar: false,
    scrollZoom: true,
    modeBarButtonsToRemove: ["zoom2d", "pan2d", "lasso2d", "select2d", "zoomIn2d", "zoomOut2d", "autoScale2d", "resetScale2d"]
  }};

  if (!initializedMain) {{
    const mainFigure = buildMainFigure(nextPayload);
    await Plotly.newPlot(mainRoot, mainFigure.data, mainFigure.layout, config);
    initializedMain = true;
    mainRoot.on("plotly_relayout", (eventData) => {{
      if (syncingRange) return;
      const x0 = eventData["xaxis.range[0]"];
      const x1 = eventData["xaxis.range[1]"];
      if (x0 === undefined || x1 === undefined) return;
      syncingRange = true;
      Plotly.relayout(indicatorRoot, {{ "xaxis.range": [x0, x1] }}).finally(() => {{
        syncingRange = false;
      }});
    }});
  }} else {{
    await applyMainIncremental(previousPayload, nextPayload);
  }}

  if (!initializedIndicator) {{
    const indicatorFigure = buildIndicatorFigure(nextPayload);
    await Plotly.newPlot(indicatorRoot, indicatorFigure.data, indicatorFigure.layout, config);
    initializedIndicator = true;
  }} else {{
    await applyIndicatorIncremental(previousPayload, nextPayload);
  }}

  await syncIndicatorRangeFromMain();
  await applyMarkerPayload(nextPayload, nextPayload);
  previousPayload = nextPayload;
}}

loadMarkerFilters();
renderMarkerFilterControls();
refreshCharts();
if (window[refreshTimerKey]) {{
  clearInterval(window[refreshTimerKey]);
}}
window[refreshTimerKey] = setInterval(refreshCharts, 5000);
</script>
"""
