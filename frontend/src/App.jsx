import { useEffect, useMemo, useRef, useState } from "react";

const API_BASE = (import.meta.env.VITE_API_BASE_URL || window.location.origin).replace(/\/$/, "");
const WS_BASE = `${API_BASE.replace(/^http/, "ws")}/ws/dashboard`;

const severityLabels = {
  critical: "紧急",
  high: "高优先级",
  medium: "中优先级",
  low: "低优先级"
};

const categoryLabels = {
  funding: "费率异动",
  orderbook: "大额挂单",
  intel: "情报催化"
};

const sourceTypeLabels = {
  official: "官方",
  media: "媒体",
  x: "X 白名单"
};

const workerLabels = {
  bootstrapped: "已完成启动",
  live: "在线",
  reconnecting: "重连中",
  degraded: "降级",
  unknown: "未知"
};

function fmtNumber(value, digits = 2) {
  const numeric = Number(value || 0);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  return numeric.toLocaleString("zh-CN", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits
  });
}

function fmtCompact(value) {
  const numeric = Number(value || 0);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  return new Intl.NumberFormat("zh-CN", {
    notation: "compact",
    maximumFractionDigits: 2
  }).format(numeric);
}

function fmtFundingRate(value, digits = 4) {
  const numeric = Number(value || 0);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  return `${(numeric * 100).toLocaleString("zh-CN", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits
  })}%`;
}

function fmtDateTime(value) {
  if (!value) {
    return "--";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "--";
  }
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  }).format(date);
}

function fmtAge(seconds) {
  const numeric = Number(seconds || 0);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return "刚刚";
  }
  if (numeric < 60) {
    return `${Math.round(numeric)} 秒前`;
  }
  if (numeric < 3600) {
    return `${Math.round(numeric / 60)} 分钟前`;
  }
  return `${Math.round(numeric / 3600)} 小时前`;
}

function severityClass(value) {
  return `severity-${String(value || "low").toLowerCase()}`;
}

function playAlertTone() {
  const AudioContextClass = window.AudioContext || window.webkitAudioContext;
  if (!AudioContextClass) {
    return;
  }
  const context = new AudioContextClass();
  const oscillator = context.createOscillator();
  const gain = context.createGain();
  oscillator.type = "triangle";
  oscillator.frequency.value = 880;
  gain.gain.setValueAtTime(0.001, context.currentTime);
  gain.gain.exponentialRampToValueAtTime(0.12, context.currentTime + 0.03);
  gain.gain.exponentialRampToValueAtTime(0.001, context.currentTime + 0.45);
  oscillator.connect(gain);
  gain.connect(context.destination);
  oscillator.start();
  oscillator.stop(context.currentTime + 0.5);
}

function FundingChart({ symbol, points }) {
  const width = 820;
  const height = 250;
  const paddingX = 18;
  const paddingY = 20;

  const chart = useMemo(() => {
    if (!points.length) {
      return null;
    }
    const values = points.map((item) => Number(item.funding_rate || 0));
    let min = Math.min(...values);
    let max = Math.max(...values);
    if (min === max) {
      const padding = Math.max(Math.abs(min) * 0.25, 0.00005);
      min -= padding;
      max += padding;
    }
    const range = Math.max(max - min, 0.00001);
    const plotWidth = width - paddingX * 2;
    const plotHeight = height - paddingY * 2;
    const yForValue = (value) => paddingY + (max - value) / range * plotHeight;
    const xForIndex = (index) => (
      points.length === 1
        ? width / 2
        : paddingX + index / (points.length - 1) * plotWidth
    );
    const coords = points.map((item, index) => ({
      x: xForIndex(index),
      y: yForValue(Number(item.funding_rate || 0)),
      value: Number(item.funding_rate || 0),
      observedAt: item.observed_at
    }));
    const line = coords
      .map((point, index) => `${index === 0 ? "M" : "L"} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`)
      .join(" ");
    const last = coords.at(-1);
    const first = coords[0];
    const area = `${line} L ${last.x.toFixed(2)} ${(height - paddingY).toFixed(2)} L ${first.x.toFixed(2)} ${(height - paddingY).toFixed(2)} Z`;
    return {
      line,
      area,
      last,
      zeroY: Math.min(height - paddingY, Math.max(paddingY, yForValue(0))),
      min,
      max
    };
  }, [points]);

  if (!symbol) {
    return (
      <div className="empty-state">
        暂无可展示的费率币种，等合约列表加载完成后这里会自动出现。
      </div>
    );
  }

  if (!chart) {
    return (
      <div className="empty-state">
        {symbol} 暂时还没有费率历史点位，worker 采集几轮后会自动生成走势图。
      </div>
    );
  }

  const latest = points.at(-1);

  return (
    <div className="chart-shell">
      <div className="chart-metrics">
        <div>
          <span>当前费率</span>
          <strong className={Number(latest?.funding_rate || 0) >= 0 ? "positive" : "negative"}>
            {fmtFundingRate(latest?.funding_rate, 4)}
          </strong>
        </div>
        <div>
          <span>最高点</span>
          <strong>{fmtFundingRate(chart.max, 4)}</strong>
        </div>
        <div>
          <span>最低点</span>
          <strong>{fmtFundingRate(chart.min, 4)}</strong>
        </div>
        <div>
          <span>最近更新时间</span>
          <strong>{fmtDateTime(latest?.observed_at)}</strong>
        </div>
      </div>
      <div className="chart-frame">
        <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={`${symbol} 费率走势图`}>
          <defs>
            <linearGradient id="fundingArea" x1="0" x2="0" y1="0" y2="1">
              <stop offset="0%" stopColor="rgba(73, 211, 255, 0.42)" />
              <stop offset="100%" stopColor="rgba(73, 211, 255, 0.02)" />
            </linearGradient>
          </defs>
          <line x1={paddingX} x2={width - paddingX} y1={chart.zeroY} y2={chart.zeroY} className="chart-zero-line" />
          <path d={chart.area} className="chart-area" />
          <path d={chart.line} className="chart-line" />
          <circle cx={chart.last.x} cy={chart.last.y} r="5.5" className="chart-point" />
        </svg>
        <div className="chart-axis chart-axis-top">{fmtFundingRate(chart.max, 4)}</div>
        <div className="chart-axis chart-axis-bottom">{fmtFundingRate(chart.min, 4)}</div>
      </div>
      <div className="chart-footer">
        <span>{symbol} 最近 {points.length} 个采样点</span>
        <span>默认按 2 分钟采样入库，适合 VPS 长时间稳定运行</span>
      </div>
    </div>
  );
}

function App() {
  const [overview, setOverview] = useState({
    monitored_symbol_count: 0,
    top_funding: [],
    top_walls: [],
    latest_alerts: [],
    latest_intel: [],
    health: {}
  });
  const [chartState, setChartState] = useState({ symbol: "", points: [] });
  const [symbols, setSymbols] = useState([]);
  const [rules, setRules] = useState({});
  const [selectedSymbol, setSelectedSymbol] = useState("ALL");
  const [wsStatus, setWsStatus] = useState("connecting");
  const [saving, setSaving] = useState(false);
  const latestAlertId = useRef(0);

  const filteredOverview = useMemo(() => {
    if (selectedSymbol === "ALL") {
      return overview;
    }
    return {
      ...overview,
      top_funding: overview.top_funding.filter((item) => item.symbol === selectedSymbol),
      top_walls: overview.top_walls.filter((item) => item.symbol === selectedSymbol),
      latest_alerts: overview.latest_alerts.filter((item) => item.symbol === selectedSymbol),
      latest_intel: overview.latest_intel.filter((item) => (item.symbols || []).includes(selectedSymbol))
    };
  }, [overview, selectedSymbol]);

  const chartSymbol = useMemo(() => {
    if (selectedSymbol !== "ALL") {
      return selectedSymbol;
    }
    return overview.top_funding?.[0]?.symbol || symbols[0]?.symbol || "";
  }, [overview.top_funding, selectedSymbol, symbols]);

  async function fetchJson(path, options) {
    const response = await fetch(`${API_BASE}${path}`, options);
    if (!response.ok) {
      throw new Error(`请求失败: ${response.status}`);
    }
    return response.json();
  }

  async function loadBootData() {
    const [overviewPayload, rulesPayload, symbolsPayload] = await Promise.all([
      fetchJson("/api/v1/dashboard/overview"),
      fetchJson("/api/v1/rules"),
      fetchJson("/api/v1/symbols")
    ]);
    setOverview(overviewPayload);
    setRules(rulesPayload);
    setSymbols(symbolsPayload);
    latestAlertId.current = overviewPayload.latest_alerts?.[0]?.id || 0;
  }

  useEffect(() => {
    loadBootData().catch((error) => {
      console.error(error);
    });
    const interval = window.setInterval(() => {
      fetchJson("/api/v1/dashboard/overview")
        .then((payload) => {
          setOverview(payload);
        })
        .catch((error) => {
          console.error(error);
        });
    }, 15000);
    return () => window.clearInterval(interval);
  }, []);

  useEffect(() => {
    if (!chartSymbol) {
      setChartState({ symbol: "", points: [] });
      return undefined;
    }

    let active = true;
    const loadChart = () => {
      fetchJson(`/api/v1/funding/history?symbol=${encodeURIComponent(chartSymbol)}&limit=120`)
        .then((payload) => {
          if (active) {
            setChartState(payload);
          }
        })
        .catch((error) => {
          console.error(error);
        });
    };

    loadChart();
    const interval = window.setInterval(loadChart, 30000);
    return () => {
      active = false;
      window.clearInterval(interval);
    };
  }, [chartSymbol]);

  useEffect(() => {
    let active = true;
    let socket;
    let retryTimer;

    const connect = () => {
      socket = new WebSocket(WS_BASE);
      socket.onopen = () => {
        if (!active) {
          return;
        }
        setWsStatus("live");
      };
      socket.onmessage = (event) => {
        if (!active) {
          return;
        }
        const payload = JSON.parse(event.data);
        const incomingAlertId = payload.latest_alerts?.[0]?.id || 0;
        if (latestAlertId.current && incomingAlertId > latestAlertId.current) {
          playAlertTone();
        }
        latestAlertId.current = Math.max(latestAlertId.current, incomingAlertId);
        setOverview(payload);
      };
      socket.onerror = () => {
        if (active) {
          setWsStatus("degraded");
        }
      };
      socket.onclose = () => {
        if (!active) {
          return;
        }
        setWsStatus("reconnecting");
        retryTimer = window.setTimeout(connect, 3000);
      };
    };

    connect();

    return () => {
      active = false;
      if (retryTimer) {
        window.clearTimeout(retryTimer);
      }
      if (socket) {
        socket.close();
      }
    };
  }, []);

  function updateRule(name, value) {
    setRules((previous) => ({
      ...previous,
      [name]: Number(value)
    }));
  }

  async function saveRules() {
    setSaving(true);
    try {
      const payload = await fetchJson("/api/v1/rules", {
        method: "PUT",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify(rules)
      });
      setRules(payload);
    } catch (error) {
      console.error(error);
      window.alert("保存规则失败，请检查后端接口是否可用。");
    } finally {
      setSaving(false);
    }
  }

  const highAlertCount = filteredOverview.latest_alerts.filter((item) =>
    ["high", "critical"].includes(String(item.severity).toLowerCase())
  ).length;

  return (
    <div className="app-shell">
      <div className="background-grid" />

      <header className="hero">
        <div className="hero-copy-wrap">
          <p className="eyebrow">BINANCE USDT 永续异动哨兵</p>
          <h1>实时盯住费率异动、大额挂单和最新催化情报</h1>
          <p className="hero-copy">
            这不是普通后台，而是一块偏交易台风格的监控屏。高优先级信号会第一时间打到网页和
            Telegram，同时把最近情报、费率热榜和盘口异常一起摊开给你看。
          </p>
        </div>
        <div className="hero-side">
          <div className={`status-chip ${wsStatus}`}>实时推送：{workerLabels[wsStatus] || wsStatus}</div>
          <div className="status-chip neutral">
            Worker：{workerLabels[overview.health?.worker] || overview.health?.worker || "未知"}
          </div>
          <div className="status-chip neutral">
            监控合约：{overview.health?.tracked_symbols || overview.monitored_symbol_count || 0}
          </div>
        </div>
      </header>

      <section className="summary-grid">
        <article className="summary-card">
          <span>监控币种</span>
          <strong>{overview.monitored_symbol_count || 0}</strong>
          <small>仅统计 Binance USDT 永续</small>
        </article>
        <article className="summary-card">
          <span>高优先级告警</span>
          <strong>{highAlertCount}</strong>
          <small>当前筛选范围内的高危信号</small>
        </article>
        <article className="summary-card">
          <span>大额挂单雷达</span>
          <strong>{filteredOverview.top_walls.length}</strong>
          <small>已确认或候选中的异常墙单</small>
        </article>
        <article className="summary-card">
          <span>最新情报</span>
          <strong>{filteredOverview.latest_intel.length}</strong>
          <small>官方、媒体和 X 白名单来源</small>
        </article>
      </section>

      <section className="toolbar">
        <label>
          聚焦币种
          <select value={selectedSymbol} onChange={(event) => setSelectedSymbol(event.target.value)}>
            <option value="ALL">全部</option>
            {symbols.map((item) => (
              <option key={item.symbol} value={item.symbol}>
                {item.symbol}
              </option>
            ))}
          </select>
        </label>
        <div className="health-line">
          <span>数据库：{overview.health?.database || "未知"}</span>
          <span>Worker 心跳：{fmtAge(overview.health?.worker_age_seconds)}</span>
          <span>待确认墙单：{overview.health?.pending_confirmations || 0}</span>
        </div>
      </section>

      <main className="dashboard-grid">
        <section className="panel funding-chart-panel">
          <div className="panel-head">
            <div>
              <h2>费率走势图</h2>
              <span>当前聚焦币种最近一段时间的资金费率变化轨迹</span>
            </div>
            <div className="panel-chip">{chartState.symbol || "等待数据"}</div>
          </div>
          <FundingChart symbol={chartState.symbol} points={chartState.points || []} />
        </section>

        <section className="panel funding-board-panel">
          <div className="panel-head">
            <div>
              <h2>费率热榜</h2>
              <span>按异常评分、绝对费率和横截面分位综合排序</span>
            </div>
            <div className="panel-chip">Top 12</div>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>币种</th>
                  <th>费率</th>
                  <th>Z 分数</th>
                  <th>分位</th>
                  <th>持仓价值</th>
                  <th>评分</th>
                </tr>
              </thead>
              <tbody>
                {filteredOverview.top_funding.map((item) => (
                  <tr key={item.symbol}>
                    <td>{item.symbol}</td>
                    <td className={item.funding_rate >= 0 ? "positive" : "negative"}>
                      {fmtFundingRate(item.funding_rate, 4)}
                    </td>
                    <td>{fmtNumber(item.funding_zscore, 2)}</td>
                    <td>{fmtNumber(item.funding_percentile * 100, 1)}%</td>
                    <td>{fmtCompact(item.open_interest_value)}</td>
                    <td>{fmtNumber(item.funding_score, 1)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

        <section className="panel orderbook-panel">
          <div className="panel-head">
            <div>
              <h2>大额挂单雷达</h2>
              <span>先轻量扫描，再对疑似大墙做深度确认</span>
            </div>
            <div className="panel-chip">盘口异常</div>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>币种</th>
                  <th>方向</th>
                  <th>名义价值</th>
                  <th>距离</th>
                  <th>持续度</th>
                  <th>评分</th>
                </tr>
              </thead>
              <tbody>
                {filteredOverview.top_walls.map((item) => (
                  <tr key={`${item.symbol}-${item.wall_side}`}>
                    <td>{item.symbol}</td>
                    <td className={item.wall_side === "bid" ? "positive" : "negative"}>
                      {item.wall_side === "bid" ? "买墙" : "卖墙"}
                    </td>
                    <td>{fmtCompact(item.wall_notional)}</td>
                    <td>{fmtNumber(item.wall_distance_bps, 1)} bps</td>
                    <td>{fmtNumber(item.wall_persistence * 100, 0)}%</td>
                    <td>{fmtNumber(item.wall_score, 1)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

        <section className="panel alerts-panel">
          <div className="panel-head">
            <div>
              <h2>最新告警流</h2>
              <span>网页弹窗、声音提醒与 Telegram 推送共用这条告警主链路</span>
            </div>
            <div className="panel-chip">滚动刷新</div>
          </div>
          <div className="alert-list">
            {filteredOverview.latest_alerts.map((item) => (
              <article key={item.id} className={`alert-card ${severityClass(item.severity)}`}>
                <div className="alert-top">
                  <strong>{item.headline}</strong>
                  <span>{item.symbol}</span>
                </div>
                <p>{item.message}</p>
                <div className="alert-meta">
                  <span>{categoryLabels[item.category] || item.category}</span>
                  <span>{severityLabels[item.severity] || item.severity}</span>
                  <span>评分 {fmtNumber(item.score, 1)}</span>
                  <span>{fmtDateTime(item.triggered_at)}</span>
                </div>
              </article>
            ))}
          </div>
        </section>

        <section className="panel intel-panel">
          <div className="panel-head">
            <div>
              <h2>最新情报流</h2>
              <span>官方公告、主流媒体和 X 白名单情报汇总</span>
            </div>
            <div className="panel-chip">近 12 条</div>
          </div>
          <div className="intel-list">
            {filteredOverview.latest_intel.map((item) => (
              <article key={item.id} className="intel-card">
                <div className="intel-top">
                  <span>{item.source_name}</span>
                  <span>{sourceTypeLabels[item.source_type] || item.source_type}</span>
                </div>
                <h3>{item.title}</h3>
                <p>{item.summary || "当前来源没有提供摘要，系统保留了原始标题供你快速判断。"}</p>
                <div className="intel-bottom">
                  <span>{(item.symbols || []).join("、") || "暂未识别到明确币种"}</span>
                  {item.url ? (
                    <a href={item.url} target="_blank" rel="noreferrer">
                      查看来源
                    </a>
                  ) : (
                    <span>无外部链接</span>
                  )}
                </div>
              </article>
            ))}
          </div>
        </section>

        <section className="panel rules-panel">
          <div className="panel-head">
            <div>
              <h2>规则面板</h2>
              <span>这里改阈值，后端告警逻辑会直接跟着更新</span>
            </div>
            <div className="panel-chip">实时生效</div>
          </div>
          <div className="rules-grid">
            <label>
              费率绝对值下限
              <input
                type="number"
                step="0.00001"
                value={rules.funding_abs_rate_floor ?? 0}
                onChange={(event) => updateRule("funding_abs_rate_floor", event.target.value)}
              />
            </label>
            <label>
              费率预警评分
              <input
                type="number"
                step="1"
                value={rules.funding_prealert_score ?? 0}
                onChange={(event) => updateRule("funding_prealert_score", event.target.value)}
              />
            </label>
            <label>
              费率正式告警评分
              <input
                type="number"
                step="1"
                value={rules.funding_alert_score ?? 0}
                onChange={(event) => updateRule("funding_alert_score", event.target.value)}
              />
            </label>
            <label>
              墙单最小名义价值
              <input
                type="number"
                step="10000"
                value={rules.wall_min_notional_usd ?? 0}
                onChange={(event) => updateRule("wall_min_notional_usd", event.target.value)}
              />
            </label>
            <label>
              墙单最大距离 bps
              <input
                type="number"
                step="1"
                value={rules.wall_distance_limit_bps ?? 0}
                onChange={(event) => updateRule("wall_distance_limit_bps", event.target.value)}
              />
            </label>
            <label>
              墙单正式告警评分
              <input
                type="number"
                step="1"
                value={rules.wall_alert_score ?? 0}
                onChange={(event) => updateRule("wall_alert_score", event.target.value)}
              />
            </label>
          </div>
          <button className="save-button" onClick={saveRules} disabled={saving}>
            {saving ? "保存中..." : "保存规则"}
          </button>
        </section>
      </main>
    </div>
  );
}

export default App;
