from __future__ import annotations

from urllib.parse import urlparse


def build_live_chart_html(
    *,
    server_url: str,
    symbol: str,
    pair_symbol: str | None,
    stoch_pct: int,
    cci_pct: int,
    rsi_pct: int,
    profile_name: str,
) -> str:
    pair_query = pair_symbol or ""
    parsed_server = urlparse(server_url)
    chart_port = parsed_server.port or 8765
    return f"""
<div style="display:flex;flex-direction:column;gap:10px;">
  <div id="main-chart-root" style="width:100%;height:400px;background:#131722;border:1px solid #2a2e39;border-radius:12px;"></div>
  <div id="indicator-chart-root" style="width:100%;height:220px;background:#131722;border:1px solid #2a2e39;border-radius:12px;"></div>
</div>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<script>
const mainRoot = document.getElementById("main-chart-root");
const indicatorRoot = document.getElementById("indicator-chart-root");
const hostWindow = window.parent && window.parent.location ? window.parent : window;
const chartBaseUrl = `${{hostWindow.location.protocol}}//${{hostWindow.location.hostname}}:{chart_port}`;
const endpoint =
  `${{chartBaseUrl}}/chart?kind=overlay&symbol={symbol}` +
  `&pair_symbol={pair_query}&stoch_pct={stoch_pct}&cci_pct={cci_pct}&rsi_pct={rsi_pct}&profile_name={profile_name}`;

let initializedMain = false;
let initializedIndicator = false;
let syncingRange = false;
let previousPayload = null;

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
  const buyOrders = orders.filter((item) => item.side === "buy");
  const sellOrders = orders.filter((item) => item.side === "sell");
  return {{
    main: [
      payload.signals.primaryOpenMain || [],
      payload.signals.primaryCloseMain || [],
      payload.signals.pairOpenMain || [],
      payload.signals.pairCloseMain || [],
      buyOrders,
      sellOrders
    ],
    indicator: [
      payload.signals.primaryOpenMain || [],
      payload.signals.primaryCloseMain || [],
      payload.signals.pairOpenMain || [],
      payload.signals.pairCloseMain || [],
      buyOrders,
      sellOrders,
      payload.signals.primaryOpenIndicator || [],
      payload.signals.primaryCloseIndicator || [],
      payload.signals.pairOpenIndicator || [],
      payload.signals.pairCloseIndicator || []
    ]
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
      margin: {{ l: 8, r: 56, t: 36, b: 18 }},
      height: 220,
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
        range: [-1.6, 1.6],
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
  const markers = markerSeries(nextPayload).main;

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

  const ticks = tickData(nextPayload);
  await Plotly.relayout(mainRoot, {{
    "xaxis.tickvals": ticks.tickvals,
    "xaxis.ticktext": ticks.ticktext
  }});
}}

async function applyIndicatorIncremental(prevPayload, nextPayload) {{
  const nextCandles = candleArrays(nextPayload);
  const markers = markerSeries(nextPayload).indicator;
  const indicatorTimes = nextCandles.times.map((item) => (item || "").replace("T", " ").slice(0, 16));

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

async function refreshCharts() {{
  const response = await fetch(endpoint, {{ cache: "no-store" }});
  if (!response.ok) throw new Error(`HTTP ${{response.status}}`);
  const nextPayload = await response.json();
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
  previousPayload = nextPayload;
}}

refreshCharts();
setInterval(refreshCharts, 5000);
</script>
"""
