/* * ISA Standard Compliant
 * Distributed under the Apache License, Version 2.0.
 * SPDX-License-Identifier: Apache-2.0
 */
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";
import { askDataset, fetchRecentSearches, uploadDatasets } from "./api.js";

const acceptedFiles = ".csv,.json";
const emptyAnalytics = {
  chart_type: "table",
  data_points: [],
  outliers_noted: []
};
const emptyDashboard = {
  bar_chart: emptyAnalytics,
  headline_metrics: [],
  insight: "Upload multiple datasets to build a connected analytics workspace.",
  pie_chart: { chart_type: "pie", data_points: [], outliers_noted: [] },
  transparency: null
};
const chartPalette = ["#5a287d", "#7b52a1", "#a17ac6", "#c9b0e3", "#eadcf8"];

const formatValue = (value) => {
  const numericValue = Number(value);
  if (Number.isNaN(numericValue)) {
    return value;
  }

  return new Intl.NumberFormat("en-GB", {
    maximumFractionDigits: numericValue >= 1000 ? 0 : 2
  }).format(numericValue);
};

const buildPiePoints = (points = []) => points
  .map((point) => ({
    label: point.label,
    value: Math.abs(Number(point.value) || 0)
  }))
  .filter((point) => point.value > 0)
  .slice(0, 5);

function Header() {
  return (
    <header className="topbar fade-in">
      <div className="brand-lockup">
        <div className="brand-mark" aria-hidden="true">N</div>
        <div>
          <p className="eyebrow">Connected Analytics Workspace</p>
          <h1>Nexus AI</h1>
        </div>
      </div>
      <nav className="topnav" aria-label="Primary">
        <a href="#dashboard">Dashboard</a>
        <a href="#workspace">Workspace</a>
        <a href="#relations">Relations</a>
      </nav>
    </header>
  );
}

function UploadPanel({ files, onFilesAdded, onProcess, onRemoveFile, processing, status, workspace }) {
  const [dragging, setDragging] = useState(false);
  const inputRef = useRef(null);

  const addFiles = useCallback((fileList) => {
    const incoming = Array.from(fileList || []);
    if (incoming.length > 0) {
      onFilesAdded(incoming);
    }
  }, [onFilesAdded]);

  return (
    <section className="hero-copy fade-in-up">
      <div className="hero-badge">Multi-dataset intelligence</div>
      <h2>Upload connected business datasets and let Nexus AI link them automatically.</h2>
      <p className="hero-description">
        Nexus AI detects shared keys, builds a merged workspace, surfaces relationship-aware analytics, and answers
        questions across all uploaded files like a modern talk-to-data platform.
      </p>
      <div
        className={`upload-card ${dragging ? "is-dragging" : ""}`}
        onDragLeave={() => setDragging(false)}
        onDragOver={(event) => {
          event.preventDefault();
          setDragging(true);
        }}
        onDrop={(event) => {
          event.preventDefault();
          setDragging(false);
          addFiles(event.dataTransfer.files);
        }}
      >
        <div>
          <p className="upload-title">Dataset upload</p>
          <p className="upload-subtitle">Add multiple CSV or JSON files. Remove and re-stage files before processing.</p>
        </div>
        <div className="upload-actions">
          <button className="primary-button" disabled={processing} onClick={() => inputRef.current?.click()} type="button">
            Add files
          </button>
          <button className="secondary-button" disabled={processing || files.length === 0} onClick={onProcess} type="button">
            {processing ? "Processing workspace..." : "Build workspace"}
          </button>
        </div>
        <p className="status-text">{status}</p>
        {processing ? <div className="progress-rail"><span className="progress-fill" /></div> : null}
        <div className="file-stack">
          {files.length > 0 ? files.map((file) => (
            <div className="file-chip" key={`${file.name}-${file.size}`}>
              <div>
                <strong>{file.name}</strong>
                <small>{formatValue(file.size / 1024)} KB</small>
              </div>
              <button aria-label={`Remove ${file.name}`} onClick={() => onRemoveFile(file)} type="button">Remove</button>
            </div>
          )) : (
            <p className="muted">Stage two or more related files such as `users.csv` and `bookings.csv`.</p>
          )}
        </div>
        {workspace?.source_files?.length ? (
          <div className="dataset-pill">
            <span>{workspace.source_files.length} connected file(s)</span>
            <strong>{formatValue(workspace.row_count || 0)} merged rows</strong>
          </div>
        ) : null}
        <input
          accept={acceptedFiles}
          className="visually-hidden"
          multiple
          onChange={(event) => {
            addFiles(event.target.files);
            event.target.value = "";
          }}
          ref={inputRef}
          type="file"
        />
      </div>
    </section>
  );
}

function DashboardHero({ dashboard, workspace }) {
  const barPoints = dashboard?.bar_chart?.data_points || [];
  const piePoints = dashboard?.pie_chart?.data_points || [];
  const metrics = dashboard?.headline_metrics || [];

  return (
    <section className="hero-dashboard fade-in-up" id="dashboard">
      <div className="dashboard-header">
        <div>
          <p className="eyebrow">Merged analytics</p>
          <h3>{workspace ? "Connected workspace ready" : "Awaiting linked datasets"}</h3>
        </div>
        <span className="dashboard-chip">{workspace ? `${workspace.source_files?.length || 1} FILES` : "DEMO"}</span>
      </div>
      <div className="metric-grid">
        {metrics.length > 0 ? metrics.map((metric) => (
          <article className="metric-card" key={metric.label}>
            <span>{metric.label}</span>
            <strong>{formatValue(metric.value)}</strong>
          </article>
        )) : (
          <>
            <article className="metric-card"><span>Rows</span><strong>0</strong></article>
            <article className="metric-card"><span>Columns</span><strong>0</strong></article>
            <article className="metric-card"><span>Segments</span><strong>0</strong></article>
          </>
        )}
      </div>
      <div className="hero-chart-grid">
        <ChartCard title="Bar Graph" subtitle="Merged grouped values" type="bar" points={barPoints} />
        <ChartCard title="Pie Chart" subtitle="Share of merged segments" type="pie" points={piePoints} />
      </div>
      <div className="insight-banner">
        <p className="eyebrow">Auto insight</p>
        <p>{dashboard?.insight || "Build a workspace to generate relationship-aware insights."}</p>
      </div>
    </section>
  );
}

function ChartCard({ points, subtitle, title, type }) {
  const hasPoints = points.length > 0;

  return (
    <article className="chart-card">
      <div className="chart-card-header">
        <div>
          <h4>{title}</h4>
          <p>{subtitle}</p>
        </div>
      </div>
      <div className="chart-shell">
        {!hasPoints ? (
          <div className="chart-empty">Process a connected workspace to populate this chart.</div>
        ) : (
          <ResponsiveContainer height="100%" width="100%">
            {type === "pie" ? (
              <PieChart>
                <Pie
                  animationDuration={700}
                  cx="50%"
                  cy="50%"
                  data={points}
                  dataKey="value"
                  innerRadius={54}
                  outerRadius={90}
                  paddingAngle={2}
                >
                  {points.map((entry, index) => (
                    <Cell fill={chartPalette[index % chartPalette.length]} key={entry.label} />
                  ))}
                </Pie>
                <Tooltip formatter={(value) => formatValue(value)} />
              </PieChart>
            ) : (
              <BarChart data={points} margin={{ bottom: 12, left: 0, right: 12, top: 8 }}>
                <CartesianGrid stroke="#efe7f6" vertical={false} />
                <XAxis axisLine={false} dataKey="label" tick={{ fill: "#71538d", fontSize: 12 }} tickLine={false} />
                <YAxis axisLine={false} tick={{ fill: "#71538d", fontSize: 12 }} tickFormatter={(value) => formatValue(value)} tickLine={false} />
                <Tooltip formatter={(value) => formatValue(value)} />
                <Bar animationDuration={700} barSize={30} dataKey="value" fill="#5a287d" radius={[10, 10, 0, 0]} />
              </BarChart>
            )}
          </ResponsiveContainer>
        )}
      </div>
      {type === "pie" && hasPoints ? (
        <div className="legend-list">
          {points.map((point, index) => (
            <div className="legend-item" key={point.label}>
              <span className="legend-swatch" style={{ backgroundColor: chartPalette[index % chartPalette.length] }} />
              <span>{point.label}</span>
            </div>
          ))}
        </div>
      ) : null}
    </article>
  );
}

function WorkspacePane({ history, onPick, relationships, sourceFiles }) {
  return (
    <aside className="panel fade-in-up" id="relations">
      <div className="section-heading">
        <p className="eyebrow">Workspace</p>
        <h3>Source files and joins</h3>
      </div>
      <div className="source-file-list">
        {sourceFiles.length > 0 ? sourceFiles.map((file) => (
          <article className="source-file-card" key={file.filename}>
            <div>
              <strong>{file.filename}</strong>
              <p>{file.name}</p>
            </div>
            <small>{formatValue(file.row_count || 0)} rows</small>
          </article>
        )) : <p className="muted">Processed source files will appear here.</p>}
      </div>
      <div className="relationship-map">
        <h4>Detected relationships</h4>
        {relationships.length > 0 ? relationships.map((relationship, index) => (
          <div className="relationship-link" key={`${relationship.left_dataset}-${relationship.right_dataset}-${index}`}>
            <div>
              <strong>{relationship.left_dataset}</strong>
              <span>{relationship.left_column}</span>
            </div>
            <div className="relationship-arrow">
              <span>links to</span>
              <small>{Math.round((relationship.confidence || 0) * 100)}% match</small>
            </div>
            <div>
              <strong>{relationship.right_dataset}</strong>
              <span>{relationship.right_column}</span>
            </div>
          </div>
        )) : <p className="muted">Nexus AI will show detected joins here after processing.</p>}
      </div>
      <div className="history-block">
        <h4>Recent prompts</h4>
        {history.length === 0 ? (
          <p className="muted">Your latest workspace questions will appear here once you ask them.</p>
        ) : (
          <ol className="history-list">
            {history.map((item, index) => (
              <li key={`${item.created_at}-${index}`}>
                <button onClick={() => onPick(item)} type="button">
                  <strong>{item.prompt}</strong>
                  <small>{new Date(item.created_at).toLocaleString()}</small>
                </button>
              </li>
            ))}
          </ol>
        )}
      </div>
    </aside>
  );
}

function TypingIndicator() {
  return (
    <div className="message assistant pending">
      <div className="typing-dots" aria-hidden="true">
        <span />
        <span />
        <span />
      </div>
      <span>Nexus AI is thinking across your connected data...</span>
    </div>
  );
}

function ChatConsole({ messages, onSubmit, prompt, sending, setPrompt, workspace }) {
  const messageListRef = useRef(null);

  useEffect(() => {
    const container = messageListRef.current;
    if (container) {
      container.scrollTo({ behavior: "smooth", top: container.scrollHeight });
    }
  }, [messages, sending]);

  return (
    <main className="panel chat-panel fade-in-up" id="workspace">
      <div className="section-heading">
        <p className="eyebrow">Assistant</p>
        <h3>{workspace ? "Merged workspace chat" : "Upload files to begin"}</h3>
      </div>
      <div className="message-list" ref={messageListRef} role="log">
        {messages.length === 0 ? (
          <div className="message assistant">
            Upload multiple related datasets and ask questions like "Which user has highest bookings?" or
            "Total revenue per user?".
          </div>
        ) : (
          messages.map((message, index) => (
            <div className={`message ${message.role}`} key={`${message.role}-${index}`}>
              {message.content}
            </div>
          ))
        )}
        {sending ? <TypingIndicator /> : null}
      </div>
      <form className="prompt-form" onSubmit={onSubmit}>
        <label htmlFor="prompt">Ask Nexus AI</label>
        <textarea
          disabled={!workspace || sending}
          id="prompt"
          onChange={(event) => setPrompt(event.target.value)}
          placeholder="Ask a cross-dataset question about users, bookings, revenue, segments, or trends."
          rows={4}
          value={prompt}
        />
        <div className="prompt-actions">
          <span className="helper-text">Natural-language questions run across the merged workspace.</span>
          <button className="primary-button" disabled={!workspace || sending || !prompt.trim()} type="submit">
            {sending ? "Analyzing..." : "Ask Nexus AI"}
          </button>
        </div>
      </form>
    </main>
  );
}

function AnalyticsPanel({ analytics, dashboard, transparency, workspace }) {
  const points = analytics?.data_points || [];
  const sources = transparency?.data_sources || dashboard?.transparency?.data_sources || [];
  const metricDefinition = transparency?.metric_definition_used || dashboard?.transparency?.metric_definition_used;
  const outliers = analytics?.outliers_noted || dashboard?.bar_chart?.outliers_noted || [];

  return (
    <aside className="panel analytics-panel fade-in-up">
      <div className="section-heading">
        <p className="eyebrow">Insights</p>
        <h3>Trends, anomalies, and lineage</h3>
      </div>
      <div className="mini-chart-table">
        <div className="table-head">
          <span>Label</span>
          <span>Value</span>
        </div>
        {(points.length > 0 ? points : dashboard?.bar_chart?.data_points || []).slice(0, 6).map((point, index) => (
          <div className="table-row" key={`${point.label}-${index}`}>
            <span>{point.label}</span>
            <strong>{formatValue(point.value)}</strong>
          </div>
        ))}
      </div>
      <div className="insight-stack">
        <div className="insight-card">
          <h4>Workspace insight</h4>
          <p>{workspace?.upload_insight || dashboard?.insight || "Workspace insights will appear here after processing."}</p>
        </div>
        <div className="insight-card">
          <h4>Data sources</h4>
          {sources.length > 0 ? sources.map((source) => <p key={source}>{source}</p>) : <p>No source notes yet.</p>}
        </div>
        <div className="insight-card">
          <h4>Method</h4>
          <p>{metricDefinition || "Relationship-aware aggregate analysis"}</p>
        </div>
        <div className="insight-card">
          <h4>Anomalies</h4>
          {outliers.length > 0 ? outliers.map((outlier) => <p key={outlier}>{outlier}</p>) : <p>No strong anomalies surfaced yet.</p>}
        </div>
      </div>
    </aside>
  );
}

export default function App() {
  const [analytics, setAnalytics] = useState(emptyAnalytics);
  const [dashboard, setDashboard] = useState(emptyDashboard);
  const [history, setHistory] = useState([]);
  const [messages, setMessages] = useState([]);
  const [prompt, setPrompt] = useState("");
  const [selectedFiles, setSelectedFiles] = useState([]);
  const [sending, setSending] = useState(false);
  const [status, setStatus] = useState("Stage CSV or JSON files to build a connected workspace.");
  const [transparency, setTransparency] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [workspace, setWorkspace] = useState(null);

  const refreshHistory = useCallback(async (datasetId) => {
    const result = await fetchRecentSearches(datasetId);
    setHistory(result.searches || []);
  }, []);

  const activeDashboard = useMemo(() => dashboard || emptyDashboard, [dashboard]);

  const handleFilesAdded = useCallback((files) => {
    setSelectedFiles((current) => {
      const existing = new Set(current.map((file) => `${file.name}-${file.size}`));
      const next = [...current];
      files.forEach((file) => {
        const key = `${file.name}-${file.size}`;
        if (!existing.has(key)) {
          next.push(file);
        }
      });
      return next;
    });
    setStatus("Files staged. Build the workspace when you're ready.");
  }, []);

  const handleRemoveFile = useCallback((fileToRemove) => {
    setSelectedFiles((current) => current.filter((file) => !(file.name === fileToRemove.name && file.size === fileToRemove.size)));
  }, []);

  const handleProcessFiles = useCallback(async () => {
    if (selectedFiles.length === 0) {
      setStatus("Add at least one CSV or JSON file first.");
      return;
    }

    setUploading(true);
    setStatus(`Processing ${selectedFiles.length} file(s) and detecting relationships...`);
    try {
      const result = await uploadDatasets(selectedFiles);
      const preview = result.dashboard_preview || emptyDashboard;
      setWorkspace(result);
      setDashboard(preview);
      setAnalytics(preview.bar_chart || emptyAnalytics);
      setTransparency(preview.transparency || null);
      setMessages([
        {
          content: result.upload_insight || "Your connected workspace is ready. Ask a cross-dataset question to continue.",
          role: "assistant"
        }
      ]);
      setStatus(`Workspace ready with ${result.source_files?.length || selectedFiles.length} linked file(s).`);
      setSelectedFiles([]);
      await refreshHistory(result.dataset_id);
    } catch (error) {
      setStatus(error.message);
      setMessages([{ content: error.message, role: "assistant" }]);
    } finally {
      setUploading(false);
    }
  }, [refreshHistory, selectedFiles]);

  const handleSubmit = useCallback(async (event) => {
    event.preventDefault();
    if (!workspace || !prompt.trim()) {
      return;
    }

    const question = prompt.trim();
    setPrompt("");
    setSending(true);
    setMessages((current) => [...current, { content: question, role: "user" }]);

    try {
      const result = await askDataset(workspace.dataset_id, question);
      const nextAnalytics = result.analytics_sidebar || emptyAnalytics;
      setAnalytics(nextAnalytics);
      setTransparency(result.transparency || null);
      setDashboard((current) => ({
        ...(current || emptyDashboard),
        bar_chart: nextAnalytics,
        insight: result.insight_narrative,
        pie_chart: {
          chart_type: "pie",
          data_points: buildPiePoints(nextAnalytics.data_points || []),
          outliers_noted: nextAnalytics.outliers_noted || []
        },
        transparency: result.transparency || current?.transparency || null
      }));
      setMessages((current) => [...current, { content: result.insight_narrative, role: "assistant" }]);
      await refreshHistory(workspace.dataset_id);
    } catch (error) {
      setMessages((current) => [...current, { content: error.message, role: "assistant" }]);
    } finally {
      setSending(false);
    }
  }, [prompt, refreshHistory, workspace]);

  const handleHistoryPick = useCallback((item) => {
    const pickedAnalytics = item.analytics_sidebar || emptyAnalytics;
    setAnalytics(pickedAnalytics);
    setTransparency(item.transparency || null);
    setDashboard((current) => ({
      ...(current || emptyDashboard),
      bar_chart: pickedAnalytics,
      insight: item.insight_narrative,
      pie_chart: {
        chart_type: "pie",
        data_points: buildPiePoints(pickedAnalytics.data_points || []),
        outliers_noted: pickedAnalytics.outliers_noted || []
      },
      transparency: item.transparency || current?.transparency || null
    }));
    setMessages((current) => [
      ...current,
      { content: item.prompt, role: "user" },
      { content: item.insight_narrative, role: "assistant" }
    ]);
  }, []);

  useEffect(() => {
    if (workspace?.dataset_id) {
      refreshHistory(workspace.dataset_id).catch(() => setHistory([]));
    }
  }, [workspace?.dataset_id, refreshHistory]);

  return (
    <div className="app-shell">
      <Header />
      <section className="hero-grid">
        <UploadPanel
          files={selectedFiles}
          onFilesAdded={handleFilesAdded}
          onProcess={handleProcessFiles}
          onRemoveFile={handleRemoveFile}
          processing={uploading}
          status={status}
          workspace={workspace}
        />
        <DashboardHero dashboard={activeDashboard} workspace={workspace} />
      </section>
      <section className="workspace-grid">
        <WorkspacePane
          history={history}
          onPick={handleHistoryPick}
          relationships={workspace?.relationships || []}
          sourceFiles={workspace?.source_files || []}
        />
        <ChatConsole
          messages={messages}
          onSubmit={handleSubmit}
          prompt={prompt}
          sending={sending}
          setPrompt={setPrompt}
          workspace={workspace}
        />
        <AnalyticsPanel analytics={analytics} dashboard={activeDashboard} transparency={transparency} workspace={workspace} />
      </section>
    </div>
  );
}
