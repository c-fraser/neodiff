// Copyright 2025 c-fraser
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(target_os = "linux")]
use arboard::SetExtLinux;
use async_trait::async_trait;
use clap::{Parser, ValueEnum};
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use neodiff::{
    Diff, DiffConfig, DiffSummary, DiffWriter, GraphConfig, NodeRef, PropertyDiff, diff_graphs,
    new_jsonl_writer,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use serde_json::Value;
use similar::{ChangeTag, TextDiff};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::error::Error;
use std::fs::File;
use std::io::{self, IsTerminal};
use std::process;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::mpsc;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::Registry;
use tui_tree_widget::{Tree, TreeItem, TreeState};
use url::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = Args::parse();

    let source = parse_neo4j_uri(&args.source)?;
    let target = parse_neo4j_uri(&args.target)?;
    let include_diff_tags = args
        .diffs
        .map(|tags| tags.into_iter().collect::<HashSet<_>>());
    let diff_config = DiffConfig::new(
        args.nodes.unwrap_or_default(),
        args.exclude_nodes.unwrap_or_default(),
        args.relationships.unwrap_or_default(),
        args.exclude_relationships.unwrap_or_default(),
        args.exclude_property_keys.unwrap_or_default(),
        args.max_diffs,
        include_diff_tags,
        Some(args.similarity_threshold),
    )?;

    // use TUI if explicitly requested or if stdout is a TTY
    let use_tui = args.output.is_none()
        && args
            .format
            .as_ref()
            .map(|f| matches!(f, OutputFormat::Tui))
            .unwrap_or_else(|| io::stdout().is_terminal());

    if use_tui {
        let (sender, receiver) = mpsc::unbounded_channel();
        let layer = TuiTracingLayer {
            sender: sender.clone(),
        };
        let subscriber = Registry::default().with(layer);
        let _ = tracing::subscriber::set_global_default(subscriber);
        let mut writer = TuiWriter::with_channel(sender, receiver);
        let result = diff_graphs(&source, &target, &diff_config, &mut writer).await;
        if let Err(e) = &result {
            writer.send_error(&e.to_string());
        }
        writer.summarize().await?;
        if result.is_err() {
            process::exit(1);
        }
        Ok(())
    } else {
        let mut writer: Box<dyn DiffWriter> = match &args.output {
            Some(path) => new_jsonl_writer(File::create(path)?),
            None => new_jsonl_writer(io::stdout()),
        };
        diff_graphs(&source, &target, &diff_config, writer.as_mut()).await
    }
}

#[derive(Parser)]
#[command(name = "neodiff", about = "Neo4j graph comparison tool", version)]
struct Args {
    /// *Source* graph URI: bolt://[user]:[password]@[host]:[port]/[database]
    #[arg(long)]
    source: String,

    /// *Target* graph URI: bolt://[user]:[password]@[host]:[port]/[database]
    #[arg(long)]
    target: String,

    /// Output file (implies --format=jsonl)
    #[arg(short, long)]
    output: Option<String>,

    /// Output format (default: tui if stdout is a TTY, otherwise jsonl)
    #[arg(long, value_enum)]
    format: Option<OutputFormat>,

    /// Only compare nodes with labels matching these regex patterns (comma-separated)
    #[arg(long, value_delimiter = ',')]
    nodes: Option<Vec<String>>,

    /// Exclude nodes with labels matching these regex patterns (comma-separated)
    #[arg(long, value_delimiter = ',')]
    exclude_nodes: Option<Vec<String>>,

    /// Only compare relationships with types matching these regex patterns (comma-separated)
    #[arg(long, value_delimiter = ',')]
    relationships: Option<Vec<String>>,

    /// Exclude relationships with types matching these regex patterns (comma-separated)
    #[arg(long, value_delimiter = ',')]
    exclude_relationships: Option<Vec<String>>,

    /// Exclude properties with keys matching these regex patterns (comma-separated)
    #[arg(long, value_delimiter = ',')]
    exclude_property_keys: Option<Vec<String>>,

    /// Maximum number of differences to report per node label or relationship type
    #[arg(long, value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..))]
    max_diffs: Option<usize>,

    /// Only write differences of these types (comma-separated). Options: SourceNodeLabel,
    /// TargetNodeLabel, SourceRelationshipType, TargetRelationshipType, SourceNode,
    /// TargetNode, ModifiedNode, SourceRelationship, TargetRelationship, ModifiedRelationship
    #[arg(long, value_delimiter = ',')]
    diffs: Option<Vec<String>>,

    /// Similarity threshold (0-100) for fuzzy matching nodes/relationships without unique
    /// constraints. Entities with property similarity at or above this threshold are reported
    /// as modified rather than separate removed/added diffs. Set to 0 to disable.
    #[arg(long, value_parser = clap::builder::RangedU64ValueParser::<u8>::new().range(0..=100), default_value_t = neodiff::DiffConfig::DEFAULT_SIMILARITY_THRESHOLD)]
    similarity_threshold: u8,
}

#[derive(Debug, Clone, ValueEnum)]
enum OutputFormat {
    /// Interactive TUI
    Tui,
    /// JSON Lines format
    Jsonl,
}

fn parse_neo4j_uri(uri_str: &str) -> Result<GraphConfig, Box<dyn Error + Send + Sync>> {
    let parsed = Url::parse(uri_str)?;

    let scheme = parsed.scheme();
    if scheme != "bolt" && scheme != "neo4j" && scheme != "bolt+s" && scheme != "neo4j+s" {
        return Err(format!(
            "Unsupported scheme '{}', expected bolt, bolt+s, neo4j, or neo4j+s",
            scheme
        )
        .into());
    }

    let decode = |s: &str| -> Result<String, Box<dyn Error + Send + Sync>> {
        Ok(percent_encoding::percent_decode_str(s)
            .decode_utf8()?
            .to_string())
    };

    let user = if parsed.username().is_empty() {
        "neo4j".to_string()
    } else {
        decode(parsed.username())?
    };
    let password = parsed
        .password()
        .map(decode)
        .transpose()?
        .ok_or("Password is required; e.g., bolt://neo4j:password@host:7687")?;
    let host = parsed.host_str().ok_or("Host is required")?;
    let port = parsed.port().unwrap_or(7687);

    // Database name from path (e.g., bolt://host:7687/mydb)
    let database = {
        let path = parsed.path();
        if path.is_empty() || path == "/" {
            None
        } else {
            Some(path.trim_start_matches('/').to_string())
        }
    };

    let uri = format!("{}://{}:{}", scheme, host, port);
    Ok(GraphConfig::new(uri, user, password, database.as_deref()))
}

struct TuiWriter {
    state: Arc<StdMutex<TuiState>>,
    sender: mpsc::UnboundedSender<TuiMessage>,
    handle: Option<std::thread::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>>,
}

struct TuiState {
    diffs: Vec<Diff>,
    summary: DiffSummary,
    is_complete: bool,
    error: Option<String>,
    // incremented when diffs change; used to avoid rebuilding tree items every frame
    generation: u64,
    // brief status message (e.g., "Copied!") with expiration time
    status_message: Option<(String, std::time::Instant)>,
    // ring buffer of recent log lines from tracing events
    log_lines: VecDeque<(String, tracing::Level)>,
    // pipeline progress: (current, total) entity count
    progress: Option<(usize, usize)>,
}

const LOG_CAPACITY: usize = 50;

impl TuiState {
    fn new() -> Self {
        Self {
            diffs: Vec::new(),
            summary: DiffSummary::default(),
            is_complete: false,
            error: None,
            generation: 0,
            status_message: None,
            log_lines: VecDeque::with_capacity(LOG_CAPACITY),
            progress: None,
        }
    }
}

enum TuiMessage {
    Diff(Diff),
    Complete,
    Error(String),
    Log(String, tracing::Level),
    Progress(usize, usize),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DiffCategory {
    Removed,
    Added,
    Modified,
}

impl DiffCategory {
    fn from_diff(diff: &Diff) -> Option<Self> {
        match diff {
            Diff::SourceNodeLabel { .. }
            | Diff::SourceRelationshipType { .. }
            | Diff::SourceNode { .. }
            | Diff::SourceRelationship { .. } => Some(Self::Removed),
            Diff::TargetNodeLabel { .. }
            | Diff::TargetRelationshipType { .. }
            | Diff::TargetNode { .. }
            | Diff::TargetRelationship { .. } => Some(Self::Added),
            Diff::ModifiedNode { .. } | Diff::ModifiedRelationship { .. } => Some(Self::Modified),
        }
    }

    fn color(self) -> Color {
        match self {
            Self::Removed => Color::Red,
            Self::Added => Color::Green,
            Self::Modified => Color::Yellow,
        }
    }

    fn style(self) -> Style {
        Style::default().fg(self.color())
    }

    fn prefix(self) -> &'static str {
        match self {
            Self::Removed => "-",
            Self::Added => "+",
            Self::Modified => "~",
        }
    }
}

#[derive(Default, Clone)]
struct DiffCounts {
    added: usize,
    removed: usize,
    modified: usize,
}

impl DiffCounts {
    fn total(&self) -> usize {
        self.added + self.removed + self.modified
    }

    fn count(&mut self, category: DiffCategory) {
        match category {
            DiffCategory::Removed => self.removed += 1,
            DiffCategory::Added => self.added += 1,
            DiffCategory::Modified => self.modified += 1,
        }
    }

    fn count_diff(&mut self, diff: &Diff) {
        if let Some(cat) = DiffCategory::from_diff(diff) {
            self.count(cat);
        }
    }

    fn merge(&mut self, other: &DiffCounts) {
        self.added += other.added;
        self.removed += other.removed;
        self.modified += other.modified;
    }

    // formats counts as colored spans: "-N +N ~N" (omitting zero counts)
    fn to_spans(&self) -> Vec<Span<'static>> {
        let mut spans = Vec::new();

        if self.removed > 0 {
            spans.push(Span::styled(
                format!("-{}", self.removed),
                DiffCategory::Removed.style(),
            ));
        }

        if self.added > 0 {
            if !spans.is_empty() {
                spans.push(Span::raw(" "));
            }
            spans.push(Span::styled(
                format!("+{}", self.added),
                DiffCategory::Added.style(),
            ));
        }

        if self.modified > 0 {
            if !spans.is_empty() {
                spans.push(Span::raw(" "));
            }
            spans.push(Span::styled(
                format!("~{}", self.modified),
                DiffCategory::Modified.style(),
            ));
        }

        if spans.is_empty() {
            spans.push(Span::raw("0"));
        }

        spans
    }
}

// formats a tree item header: "prefix (total | counts)" or with relationships
fn summary_header(
    prefix: &str,
    node_counts: &DiffCounts,
    rel_counts: &DiffCounts,
    bold: bool,
) -> Line<'static> {
    let total = node_counts.total() + rel_counts.total();
    let rel_style = if bold {
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::Cyan)
    };
    let mut spans = vec![Span::raw(format!("{} ({} | ", prefix, total))];
    if rel_counts.total() > 0 {
        spans.extend(node_counts.to_spans());
        spans.push(Span::raw(", "));
        spans.push(Span::styled("Relationships: ", rel_style));
        spans.extend(rel_counts.to_spans());
    } else {
        spans.extend(node_counts.to_spans());
    }
    spans.push(Span::raw(")"));
    Line::from(spans)
}

fn rel_suffix_spans(counts: &DiffCounts) -> Vec<Span<'static>> {
    let style = Style::default()
        .fg(Color::Cyan)
        .add_modifier(Modifier::BOLD);
    let mut spans = vec![Span::styled("Relationships (", style)];
    spans.extend(counts.to_spans());
    spans.push(Span::styled(")", style));
    spans
}

fn build_summary_line(label: &str, removed: u64, added: u64, modified: u64) -> Line<'static> {
    Line::from(vec![
        Span::raw(format!("{}: ", label)),
        Span::styled(format!("-{}", removed), DiffCategory::Removed.style()),
        Span::raw(" "),
        Span::styled(format!("+{}", added), DiffCategory::Added.style()),
        Span::raw(" "),
        Span::styled(format!("~{}", modified), DiffCategory::Modified.style()),
    ])
}

struct TuiTracingLayer {
    sender: mpsc::UnboundedSender<TuiMessage>,
}

struct MessageVisitor {
    message: Option<String>,
    progress: Option<usize>,
    total: Option<usize>,
}

impl MessageVisitor {
    fn new() -> Self {
        Self {
            message: None,
            progress: None,
            total: None,
        }
    }
}

impl tracing::field::Visit for MessageVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        }
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        match field.name() {
            "neodiff.progress" => self.progress = Some(value as usize),
            "neodiff.total" => self.total = Some(value as usize),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = Some(format!("{:?}", value));
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for TuiTracingLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = MessageVisitor::new();
        event.record(&mut visitor);
        if let (Some(current), Some(total)) = (visitor.progress, visitor.total) {
            let _ = self.sender.send(TuiMessage::Progress(current, total));
        }
        if let Some(message) = visitor.message {
            let level = *event.metadata().level();
            let _ = self.sender.send(TuiMessage::Log(message, level));
        }
    }
}

impl TuiWriter {
    #[allow(dead_code)]
    fn new() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        Self::with_channel(sender, receiver)
    }

    fn with_channel(
        sender: mpsc::UnboundedSender<TuiMessage>,
        receiver: mpsc::UnboundedReceiver<TuiMessage>,
    ) -> Self {
        let state = Arc::new(StdMutex::new(TuiState::new()));
        let tui_state = Arc::clone(&state);
        let tui_handle = std::thread::spawn(move || Self::run_tui_loop(tui_state, receiver));
        Self {
            state,
            sender,
            handle: Some(tui_handle),
        }
    }

    fn send_error(&self, err: &str) {
        let _ = self.sender.send(TuiMessage::Error(err.to_string()));
    }

    async fn wait_for_exit(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let _ = self.sender.send(TuiMessage::Complete);
        if let Some(handle) = self.handle.take() {
            tokio::task::spawn_blocking(move || handle.join())
                .await?
                .map_err(|_| "TUI thread panicked")??;
        }
        Ok(())
    }

    fn run_tui_loop(
        state: Arc<StdMutex<TuiState>>,
        mut receiver: mpsc::UnboundedReceiver<TuiMessage>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        crossterm::execute!(stdout, EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;
        let mut tree_state = TreeState::default();
        let mut spinner_frame = 0usize;
        let spinner_chars = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

        // cached tree items; rebuilt only when generation changes
        let mut cached_items: Vec<TreeItem<'static, String>> = Vec::new();
        let mut cached_generation: u64 = u64::MAX;

        loop {
            // drain all pending messages without blocking
            loop {
                match receiver.try_recv() {
                    Ok(TuiMessage::Diff(diff)) => {
                        let mut tui_state = state.lock().map_err(|e| e.to_string())?;
                        tui_state.summary.update(&diff);
                        tui_state.diffs.push(diff);
                        tui_state.generation += 1;
                    }
                    Ok(TuiMessage::Complete) => {
                        let mut tui_state = state.lock().map_err(|e| e.to_string())?;
                        tui_state.is_complete = true;
                    }
                    Ok(TuiMessage::Progress(current, total)) => {
                        let mut tui_state = state.lock().map_err(|e| e.to_string())?;
                        tui_state.progress = Some((current, total));
                    }
                    Ok(TuiMessage::Log(line, level)) => {
                        let mut tui_state = state.lock().map_err(|e| e.to_string())?;
                        if tui_state.log_lines.len() >= LOG_CAPACITY {
                            tui_state.log_lines.pop_front();
                        }
                        tui_state.log_lines.push_back((line, level));
                    }
                    Ok(TuiMessage::Error(err)) => {
                        let mut tui_state = state.lock().map_err(|e| e.to_string())?;
                        tui_state.error = Some(err);
                        tui_state.is_complete = true;
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        let mut tui_state = state.lock().map_err(|e| e.to_string())?;
                        tui_state.is_complete = true;
                        break;
                    }
                }
            }

            // extract state under lock; rebuild tree items only when generation changes
            let (summary, is_complete, error, status_msg, log_lines, progress) = {
                let tui_state = state.lock().map_err(|e| e.to_string())?;
                if tui_state.generation != cached_generation {
                    cached_items = Self::build_tree_items(&tui_state);
                    cached_generation = tui_state.generation;
                }
                // clear expired status messages
                let status_msg = tui_state
                    .status_message
                    .as_ref()
                    .and_then(|(msg, expires)| {
                        if std::time::Instant::now() < *expires {
                            Some(msg.clone())
                        } else {
                            None
                        }
                    });
                let log_lines: Vec<(String, tracing::Level)> =
                    tui_state.log_lines.iter().cloned().collect();
                let progress = tui_state.progress;
                (
                    tui_state.summary.clone(),
                    tui_state.is_complete,
                    tui_state.error.clone(),
                    status_msg,
                    log_lines,
                    progress,
                )
            };
            let items = &cached_items;

            spinner_frame = (spinner_frame + 1) % spinner_chars.len();

            terminal.draw(|f| {
                // layout: header row (summary + legend), log panel, then diff tree
                let chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Length(5),
                        Constraint::Length(6),
                        Constraint::Min(0),
                    ])
                    .split(f.area());
                let header_chunks = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints([Constraint::Min(30), Constraint::Length(30)])
                    .split(chunks[0]);

                let status_line = if let Some(ref err) = error {
                    Line::from(Span::styled(
                        format!("Error: {}", Self::extract_error_message(err)),
                        Style::default().fg(Color::Red).bold(),
                    ))
                } else if is_complete {
                    Line::from(Span::styled(
                        "Comparison Complete",
                        Style::default().fg(Color::Green).bold(),
                    ))
                } else {
                    let progress_text = match progress {
                        Some((current, total)) if total > 0 => {
                            let pct = (current * 100) / total;
                            format!("Comparing... {}% ({}/{})", pct, current, total)
                        }
                        _ => "Comparing...".to_string(),
                    };
                    Line::from(vec![
                        Span::styled(
                            format!("{} ", spinner_chars[spinner_frame]),
                            Style::default().fg(Color::Cyan),
                        ),
                        Span::styled(progress_text, Style::default().fg(Color::Cyan)),
                    ])
                };
                let summary_text = vec![
                    build_summary_line(
                        "Nodes",
                        summary.nodes_removed,
                        summary.nodes_added,
                        summary.nodes_modified,
                    ),
                    build_summary_line(
                        "Relationships",
                        summary.relationships_removed,
                        summary.relationships_added,
                        summary.relationships_modified,
                    ),
                    status_line,
                ];

                // summary panel (left)
                let summary_widget = Paragraph::new(summary_text).block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(format!(" Summary ({} differences) ", summary.total()))
                        .title_style(Style::default().bold()),
                );
                f.render_widget(summary_widget, header_chunks[0]);

                // legend panel (right)
                let legend_text = vec![
                    Line::from(vec![
                        Span::styled("- ", DiffCategory::Removed.style()),
                        Span::raw("only in source"),
                    ]),
                    Line::from(vec![
                        Span::styled("+ ", DiffCategory::Added.style()),
                        Span::raw("only in target"),
                    ]),
                    Line::from(vec![
                        Span::styled("~ ", DiffCategory::Modified.style()),
                        Span::raw("modified"),
                    ]),
                ];
                let legend_widget =
                    Paragraph::new(legend_text).block(Block::default().borders(Borders::ALL));
                f.render_widget(legend_widget, header_chunks[1]);

                // log panel
                let log_panel_height = chunks[1].height.saturating_sub(2) as usize; // subtract border
                let visible_logs: Vec<Line> = log_lines
                    .iter()
                    .rev()
                    .take(log_panel_height)
                    .rev()
                    .map(|(msg, level)| {
                        let color = match *level {
                            tracing::Level::ERROR => Color::Red,
                            tracing::Level::WARN => Color::Yellow,
                            tracing::Level::INFO => Color::Cyan,
                            tracing::Level::DEBUG => Color::DarkGray,
                            tracing::Level::TRACE => Color::DarkGray,
                        };
                        let prefix = match *level {
                            tracing::Level::ERROR => "ERROR",
                            tracing::Level::WARN => " WARN",
                            tracing::Level::INFO => " INFO",
                            tracing::Level::DEBUG => "DEBUG",
                            tracing::Level::TRACE => "TRACE",
                        };
                        Line::from(Span::styled(
                            format!("{} {}", prefix, msg),
                            Style::default().fg(color),
                        ))
                    })
                    .collect();
                let log_title = if is_complete {
                    " Log (complete) "
                } else {
                    " Log "
                };
                let log_widget = Paragraph::new(visible_logs).block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(log_title)
                        .title_style(Style::default().bold()),
                );
                f.render_widget(log_widget, chunks[1]);

                // diff tree (main content area)
                let tree_title = if let Some(msg) = &status_msg {
                    format!(" Differences - {} (↑↓ ←→ navigate, c: copy, q: quit) ", msg)
                } else {
                    " Differences (↑↓ ←→ navigate, c: copy, q: quit) ".to_string()
                };
                if items.is_empty() {
                    let message = if is_complete {
                        "No differences found."
                    } else {
                        "Waiting for differences..."
                    };
                    let empty_message = Paragraph::new(message)
                        .block(
                            Block::default()
                                .borders(Borders::ALL)
                                .title(tree_title)
                                .title_style(Style::default().bold()),
                        )
                        .style(Style::default().fg(Color::DarkGray));
                    f.render_widget(empty_message, chunks[2]);
                } else if let Ok(tree) = Tree::new(items) {
                    let tree = tree
                        .block(
                            Block::default()
                                .borders(Borders::ALL)
                                .title(tree_title)
                                .title_style(Style::default().bold()),
                        )
                        .highlight_style(
                            Style::default()
                                .add_modifier(Modifier::BOLD)
                                .bg(Color::DarkGray),
                        );
                    f.render_stateful_widget(tree, chunks[2], &mut tree_state);
                }
            })?;

            // poll with short timeout during comparison (spinner), longer when idle
            let poll_timeout = if is_complete {
                std::time::Duration::from_millis(100)
            } else {
                std::time::Duration::from_millis(50)
            };
            if event::poll(poll_timeout)?
                && let Event::Key(key) = event::read()?
                && key.kind == KeyEventKind::Press
            {
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => break,
                    KeyCode::Down | KeyCode::Char('j') => {
                        tree_state.key_down();
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        tree_state.key_up();
                    }
                    KeyCode::Left | KeyCode::Char('h') => {
                        tree_state.key_left();
                    }
                    KeyCode::Right | KeyCode::Char('l') | KeyCode::Enter => {
                        tree_state.key_right();
                    }
                    KeyCode::Home => {
                        tree_state.select_first();
                    }
                    KeyCode::End => {
                        tree_state.select_last();
                    }
                    KeyCode::Char('c') => {
                        // copy MATCH query for selected diff to clipboard
                        let selected_path = tree_state.selected();
                        if !selected_path.is_empty() {
                            let mut tui_state = state.lock().map_err(|e| e.to_string())?;
                            if let Some(diff) =
                                Self::find_diff_by_tree_path(&tui_state.diffs, selected_path)
                            {
                                if let Some(query) = Self::generate_match_query(diff) {
                                    match arboard::Clipboard::new() {
                                        Ok(mut clipboard) => {
                                            // on Linux, use wait() to fork a background process
                                            // that serves the clipboard data until another app
                                            // claims it
                                            #[cfg(target_os = "linux")]
                                            let result = clipboard.set().wait().text(query);
                                            #[cfg(not(target_os = "linux"))]
                                            let result = clipboard.set_text(&query);

                                            if result.is_ok() {
                                                tui_state.status_message = Some((
                                                    "Copied!".to_string(),
                                                    std::time::Instant::now()
                                                        + std::time::Duration::from_secs(2),
                                                ));
                                            } else {
                                                tui_state.status_message = Some((
                                                    "Copy failed".to_string(),
                                                    std::time::Instant::now()
                                                        + std::time::Duration::from_secs(2),
                                                ));
                                            }
                                        }
                                        Err(_) => {
                                            tui_state.status_message = Some((
                                                "Clipboard unavailable".to_string(),
                                                std::time::Instant::now()
                                                    + std::time::Duration::from_secs(2),
                                            ));
                                        }
                                    }
                                } else {
                                    tui_state.status_message = Some((
                                        "No element ID".to_string(),
                                        std::time::Instant::now()
                                            + std::time::Duration::from_secs(2),
                                    ));
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        disable_raw_mode()?;
        crossterm::execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
        Ok(())
    }

    // builds the hierarchical tree of diffs for the TUI widget
    fn build_tree_items(state: &TuiState) -> Vec<TreeItem<'static, String>> {
        let mut items = Vec::new();

        // partition diffs by type
        let mut schema_diffs: Vec<&Diff> = Vec::new();
        let mut node_diffs: Vec<&Diff> = Vec::new();
        let mut diffed_node_keys: HashSet<String> = HashSet::new();
        let mut rel_diffs: Vec<&Diff> = Vec::new();
        for diff in &state.diffs {
            match diff {
                Diff::SourceNodeLabel { .. }
                | Diff::TargetNodeLabel { .. }
                | Diff::SourceRelationshipType { .. }
                | Diff::TargetRelationshipType { .. } => {
                    schema_diffs.push(diff);
                }
                Diff::SourceNode { .. } | Diff::TargetNode { .. } | Diff::ModifiedNode { .. } => {
                    if let Some((_, node_ref)) = Self::node_diff_to_ref(diff) {
                        diffed_node_keys.insert(Self::node_ref_key(&node_ref));
                    }
                    node_diffs.push(diff);
                }
                Diff::SourceRelationship { .. }
                | Diff::TargetRelationship { .. }
                | Diff::ModifiedRelationship { .. } => {
                    rel_diffs.push(diff);
                }
            }
        }

        // group relationships under their connected node; if neither endpoint
        // is already in node_diffs, create a synthetic "modified" node so the
        // relationship has a parent in the tree
        let mut rels_by_node: HashMap<String, Vec<&Diff>> = HashMap::new();
        let mut synthetic_modified_nodes: HashMap<String, NodeRef> = HashMap::new();
        for rel_diff in &rel_diffs {
            let Some((start, end)) = Self::rel_endpoints(rel_diff) else {
                continue;
            };
            let start_key = Self::node_ref_key(start);
            let end_key = Self::node_ref_key(end);
            if diffed_node_keys.contains(&start_key) {
                rels_by_node.entry(start_key).or_default().push(rel_diff);
            } else if diffed_node_keys.contains(&end_key) {
                rels_by_node.entry(end_key).or_default().push(rel_diff);
            } else {
                // orphan relationship: attach to synthetic start node
                synthetic_modified_nodes
                    .entry(start_key.clone())
                    .or_insert_with(|| start.clone());
                rels_by_node.entry(start_key).or_default().push(rel_diff);
            }
        }

        // schema changes section
        if !schema_diffs.is_empty() {
            let schema_children: Vec<_> = schema_diffs
                .iter()
                .enumerate()
                .map(|(i, diff)| {
                    let item_id = format!("schema_{}", i);
                    let (text, _) = Self::format_diff(diff, &item_id);
                    let style =
                        DiffCategory::from_diff(diff).map_or(Style::default(), |c| c.style());
                    TreeItem::new_leaf(item_id, Self::styled_line(&text, style))
                })
                .collect();
            if let Ok(item) = TreeItem::new(
                "schema".to_string(),
                format!("Schema ({})", schema_diffs.len()),
                schema_children,
            ) {
                items.push(item);
            }
        }

        // build node items grouped by label; each node includes its relationships
        #[allow(clippy::type_complexity)]
        let mut nodes_by_label: BTreeMap<
            String,
            Vec<(String, TreeItem<'static, String>, DiffCounts, DiffCounts)>,
        > = BTreeMap::new();
        for (node_idx, node_diff) in node_diffs.iter().enumerate() {
            let (label, node_ref) = match Self::node_diff_to_ref(node_diff) {
                Some((l, r)) => (l, r),
                None => {
                    if let Diff::ModifiedNode { label, .. } = node_diff {
                        (label.clone(), NodeRef::default())
                    } else {
                        continue;
                    }
                }
            };
            let node_key = if node_ref.labels.is_empty() {
                if let Diff::ModifiedNode { label, id, .. } = node_diff {
                    format!("{}|{}", label, id)
                } else {
                    continue;
                }
            } else {
                Self::node_ref_key(&node_ref)
            };
            let sort_key = Self::node_sort_key(node_diff);
            let category_prefix = DiffCategory::from_diff(node_diff)
                .map(|c| c.prefix())
                .unwrap_or("?");
            // include node_idx to guarantee uniqueness even if category + sort_key collide
            let item_id = format!("node_{}_{}_{}", node_idx, category_prefix, sort_key);
            let (header, details) = Self::format_diff(node_diff, &item_id);
            let style = DiffCategory::from_diff(node_diff).map_or(Style::default(), |c| c.style());
            let mut children: Vec<TreeItem<'static, String>> = details;
            let mut node_counts = DiffCounts::default();
            let mut rel_counts = DiffCounts::default();
            node_counts.count_diff(node_diff);
            if let Some(rels) = rels_by_node.get(&node_key) {
                for rel in rels.iter() {
                    rel_counts.count_diff(rel);
                }
                children.extend(Self::build_relationship_items(rels, &node_key, &item_id));
            }
            // build header line with optional element_id(s)
            let header_line = {
                let mut spans = vec![Span::styled(header.clone(), style)];
                // append element_id(s) as dimmed text
                if let Some(eid_display) = Self::format_element_ids(node_diff) {
                    spans.push(Span::styled(
                        format!(" {}", eid_display),
                        Style::default().fg(Color::DarkGray),
                    ));
                }
                if rel_counts.total() > 0 {
                    spans.push(Span::raw(" "));
                    spans.extend(rel_suffix_spans(&rel_counts));
                }
                Line::from(spans)
            };
            let node_item = if children.is_empty() {
                TreeItem::new_leaf(item_id.clone(), header_line)
            } else {
                TreeItem::new(item_id.clone(), header_line.clone(), children)
                    .unwrap_or_else(|_| TreeItem::new_leaf(item_id, header_line))
            };
            nodes_by_label.entry(label).or_default().push((
                sort_key,
                node_item,
                node_counts,
                rel_counts,
            ));
        }

        // add synthetic nodes for orphan relationships
        for (node_key, node_ref) in synthetic_modified_nodes.iter() {
            let label = node_ref.labels.first().cloned().unwrap_or_default();
            let sort_key = node_key.clone();
            let item_id = format!("synthetic_{}", node_key);
            let node_counts = DiffCounts::default();
            let mut rel_counts = DiffCounts::default();
            let children: Vec<TreeItem<'static, String>> =
                if let Some(rels) = rels_by_node.get(node_key) {
                    for rel in rels.iter() {
                        rel_counts.count_diff(rel);
                    }
                    Self::build_relationship_items(rels, node_key, &item_id)
                } else {
                    Vec::new()
                };
            let mut header_spans = vec![Span::styled(
                format!("~ {} ", Self::format_node_ref(node_ref)),
                DiffCategory::Modified.style(),
            )];
            header_spans.extend(rel_suffix_spans(&rel_counts));
            let header_line = Line::from(header_spans);
            let node_item = if children.is_empty() {
                TreeItem::new_leaf(item_id.clone(), header_line)
            } else {
                TreeItem::new(item_id.clone(), header_line.clone(), children)
                    .unwrap_or_else(|_| TreeItem::new_leaf(item_id, header_line))
            };
            nodes_by_label.entry(label).or_default().push((
                sort_key,
                node_item,
                node_counts,
                rel_counts,
            ));
        }

        // assemble tree: Nodes -> :Label -> individual nodes
        if !nodes_by_label.is_empty() {
            let mut total_node_counts = DiffCounts::default();
            let mut total_rel_counts = DiffCounts::default();
            let label_items: Vec<_> = nodes_by_label
                .into_iter()
                .filter_map(|(label, mut node_items)| {
                    node_items.sort_by(|(a, _, _, _), (b, _, _, _)| a.cmp(b));

                    let mut label_node_counts = DiffCounts::default();
                    let mut label_rel_counts = DiffCounts::default();
                    for (_, _, nc, rc) in &node_items {
                        label_node_counts.merge(nc);
                        label_rel_counts.merge(rc);
                    }
                    total_node_counts.merge(&label_node_counts);
                    total_rel_counts.merge(&label_rel_counts);

                    let sorted_items: Vec<_> =
                        node_items.into_iter().map(|(_, item, _, _)| item).collect();
                    let header = summary_header(
                        &format!(":{}", label),
                        &label_node_counts,
                        &label_rel_counts,
                        false,
                    );
                    TreeItem::new(format!("label_{}", label), header, sorted_items).ok()
                })
                .collect();

            let total_header = summary_header("Nodes", &total_node_counts, &total_rel_counts, true);
            if let Ok(item) = TreeItem::new("nodes".to_string(), total_header, label_items) {
                items.push(item);
            }
        }
        items
    }

    fn build_relationship_items(
        rels: &[&Diff],
        node_key: &str,
        prefix: &str,
    ) -> Vec<TreeItem<'static, String>> {
        if rels.is_empty() {
            return Vec::new();
        }
        let mut sorted_rels: Vec<_> = rels.iter().collect();
        sorted_rels.sort_by_key(|r| Self::rel_sort_key(r));
        sorted_rels
            .iter()
            .enumerate()
            .filter_map(|(k, rel)| {
                Self::format_rel_for_node(rel, node_key)
                    .map(|line| TreeItem::new_leaf(format!("{}_rel_{}", prefix, k), line))
            })
            .collect()
    }

    // extracts the "message" field from a JSON-formatted error string
    fn extract_error_message(err: &str) -> String {
        // try to extract "message": "..." from JSON error response
        if let Some(start) = err.find("\"message\"")
            && let Some(colon) = err[start..].find(':')
        {
            let after_colon = &err[start + colon + 1..];
            if let Some(quote_start) = after_colon.find('"') {
                let after_quote = &after_colon[quote_start + 1..];
                if let Some(quote_end) = after_quote.find('"') {
                    return after_quote[..quote_end].to_string();
                }
            }
        }

        // otherwise, use truncated raw message
        if err.len() > 100 {
            format!("{}...", &err[..100])
        } else {
            err.to_string()
        }
    }

    fn format_diff(diff: &Diff, item_id: &str) -> (String, Vec<TreeItem<'static, String>>) {
        let get_details = |properties: &BTreeMap<String, Value>,
                           style: Style|
         -> Vec<TreeItem<'static, String>> {
            properties
                .iter()
                .enumerate()
                .map(|(i, (k, v))| {
                    Self::format_property_item(&format!("{}_prop_{}", item_id, i), "", k, v, style)
                })
                .collect()
        };
        match diff {
            Diff::SourceNodeLabel { label } => (format!("- Node :{}", label), vec![]),
            Diff::TargetNodeLabel { label } => (format!("+ Node :{}", label), vec![]),
            Diff::SourceRelationshipType { relationship_type } => {
                (format!("- Relationship [{}]", relationship_type), vec![])
            }
            Diff::TargetRelationshipType { relationship_type } => {
                (format!("+ Relationship [{}]", relationship_type), vec![])
            }
            Diff::SourceNode {
                label,
                id,
                properties,
                ..
            }
            | Diff::TargetNode {
                label,
                id,
                properties,
                ..
            } => {
                let prefix = DiffCategory::from_diff(diff).map_or("-", |c| c.prefix());
                let style = DiffCategory::from_diff(diff).map_or(Style::default(), |c| c.style());
                let header = format!("{} (:{} {{{}}})", prefix, label, id);
                let details: Vec<_> = get_details(properties, style);
                (header, details)
            }
            Diff::ModifiedNode {
                label, id, changes, ..
            } => {
                let header = format!("~ (:{} {{{}}})", label, id);
                let details = Self::format_changes(item_id, changes);
                (header, details)
            }
            Diff::SourceRelationship {
                relationship_type,
                start_node,
                end_node,
                properties,
                ..
            }
            | Diff::TargetRelationship {
                relationship_type,
                start_node,
                end_node,
                properties,
                ..
            } => {
                let prefix = DiffCategory::from_diff(diff).map_or("-", |c| c.prefix());
                let style = DiffCategory::from_diff(diff).map_or(Style::default(), |c| c.style());
                let header = format!(
                    "{} {}-[:{}]->{}",
                    prefix,
                    Self::format_node_ref(start_node),
                    relationship_type,
                    Self::format_node_ref(end_node)
                );
                let details: Vec<_> = get_details(properties, style);
                (header, details)
            }
            Diff::ModifiedRelationship {
                relationship_type,
                start_node,
                end_node,
                changes,
                ..
            } => {
                let header = format!(
                    "~ {}-[:{}]->{}",
                    Self::format_node_ref(start_node),
                    relationship_type,
                    Self::format_node_ref(end_node)
                );
                let details = Self::format_changes(item_id, changes);
                (header, details)
            }
        }
    }

    // formats a relationship relative to a node, using outgoing (->) or incoming (<-)
    // arrow direction based on which endpoint matches the associated node
    fn format_rel_for_node(diff: &Diff, associated_node_key: &str) -> Option<Line<'static>> {
        let (start, end) = Self::rel_endpoints(diff)?;
        let rel_type = match diff {
            Diff::SourceRelationship {
                relationship_type, ..
            }
            | Diff::TargetRelationship {
                relationship_type, ..
            }
            | Diff::ModifiedRelationship {
                relationship_type, ..
            } => relationship_type,
            _ => return None,
        };
        let category = DiffCategory::from_diff(diff)?;
        let prefix = category.prefix();
        let start_key = Self::node_ref_key(start);
        let end_key = Self::node_ref_key(end);
        let base_style = category.style();
        let type_style = Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD);

        let spans = if start_key == associated_node_key {
            // outgoing: node -[:TYPE]-> other
            vec![
                Span::styled(format!("{} -[:", prefix), base_style),
                Span::styled(rel_type.clone(), type_style),
                Span::styled(format!("]->{}", Self::format_node_ref(end)), base_style),
            ]
        } else if end_key == associated_node_key {
            // incoming: node <-[:TYPE]- other
            vec![
                Span::styled(format!("{} <-[:", prefix), base_style),
                Span::styled(rel_type.clone(), type_style),
                Span::styled(format!("]-{}", Self::format_node_ref(start)), base_style),
            ]
        } else {
            // neither endpoint matches (shouldn't happen normally)
            vec![
                Span::styled(
                    format!("{} {}-[:", prefix, Self::format_node_ref(start)),
                    base_style,
                ),
                Span::styled(rel_type.clone(), type_style),
                Span::styled(format!("]->{}", Self::format_node_ref(end)), base_style),
            ]
        };

        Some(Line::from(spans))
    }

    // max width before wrapping property values
    const MAX_VALUE_WIDTH: usize = 100;

    fn format_changes(item_id: &str, changes: &[PropertyDiff]) -> Vec<TreeItem<'static, String>> {
        changes
            .iter()
            .enumerate()
            .map(|(i, c)| match c {
                PropertyDiff::Added { key, value } => Self::format_property_item(
                    &format!("{}_change_{}", item_id, i),
                    "+ ",
                    key,
                    value,
                    DiffCategory::Added.style(),
                ),
                PropertyDiff::Removed { key, value } => Self::format_property_item(
                    &format!("{}_change_{}", item_id, i),
                    "- ",
                    key,
                    value,
                    DiffCategory::Removed.style(),
                ),
                PropertyDiff::Changed { key, old, new } => Self::format_inline_diff_item(
                    &format!("{}_change_{}", item_id, i),
                    key,
                    old,
                    new,
                ),
            })
            .collect()
    }

    fn format_property_item(
        id: &str,
        prefix: &str,
        key: &str,
        value: &Value,
        style: Style,
    ) -> TreeItem<'static, String> {
        let value_str = Self::format_value(value);
        let header = format!("  {} {}: ", prefix, key);
        let header_len = header.len();

        if header_len + value_str.len() <= Self::MAX_VALUE_WIDTH {
            // short value - single line leaf
            TreeItem::new_leaf(
                id.to_string(),
                Line::from(Span::styled(format!("{}{}", header, value_str), style)),
            )
        } else {
            // long value - wrap into multiple lines
            let wrapped = Self::wrap_text(&value_str, Self::MAX_VALUE_WIDTH - 6);
            let children: Vec<_> = wrapped
                .into_iter()
                .enumerate()
                .map(|(j, line)| {
                    TreeItem::new_leaf(
                        format!("{}_{}", id, j),
                        Line::from(Span::styled(format!("      {}", line), style)),
                    )
                })
                .collect();
            let header_line = Line::from(Span::styled(format!("  {} {}:", prefix, key), style));
            TreeItem::new(id.to_string(), header_line.clone(), children)
                .unwrap_or_else(|_| TreeItem::new_leaf(id.to_string(), header_line))
        }
    }

    fn format_inline_diff_item(
        id: &str,
        key: &str,
        old: &Value,
        new: &Value,
    ) -> TreeItem<'static, String> {
        let old_str = Self::format_value(old);
        let new_str = Self::format_value(new);
        let header = format!("  ~ {}: ", key);
        let header_len = header.len();

        // check if the combined diff would be too long
        let total_len = header_len + old_str.len().max(new_str.len());
        if total_len <= Self::MAX_VALUE_WIDTH {
            // short diff - inline on single line
            let diff = TextDiff::from_chars(&old_str, &new_str);
            let mut spans = vec![Span::styled(header, DiffCategory::Modified.style())];

            for change in diff.iter_all_changes() {
                let text = change.value().to_string();
                let span = match change.tag() {
                    ChangeTag::Equal => Span::raw(text),
                    ChangeTag::Delete => Span::styled(
                        text,
                        Style::default()
                            .fg(Color::Red)
                            .add_modifier(Modifier::CROSSED_OUT),
                    ),
                    ChangeTag::Insert => Span::styled(
                        text,
                        Style::default()
                            .fg(Color::Green)
                            .add_modifier(Modifier::BOLD),
                    ),
                };
                spans.push(span);
            }

            TreeItem::new_leaf(id.to_string(), Line::from(spans))
        } else {
            // long diff - show old and new on separate wrapped lines
            let header_line = Line::from(Span::styled(
                format!("  ~ {}:", key),
                DiffCategory::Modified.style(),
            ));
            let mut children = Vec::new();

            // old value (wrapped, in red)
            let old_wrapped = Self::wrap_text(&old_str, Self::MAX_VALUE_WIDTH - 8);
            for (j, line) in old_wrapped.into_iter().enumerate() {
                children.push(TreeItem::new_leaf(
                    format!("{}_old_{}", id, j),
                    Line::from(Span::styled(
                        format!("      - {}", line),
                        Style::default().fg(Color::Red),
                    )),
                ));
            }

            // new value (wrapped, in green)
            let new_wrapped = Self::wrap_text(&new_str, Self::MAX_VALUE_WIDTH - 8);
            for (j, line) in new_wrapped.into_iter().enumerate() {
                children.push(TreeItem::new_leaf(
                    format!("{}_new_{}", id, j),
                    Line::from(Span::styled(
                        format!("      + {}", line),
                        Style::default().fg(Color::Green),
                    )),
                ));
            }

            TreeItem::new(id.to_string(), header_line.clone(), children)
                .unwrap_or_else(|_| TreeItem::new_leaf(id.to_string(), header_line))
        }
    }

    fn wrap_text(text: &str, max_width: usize) -> Vec<String> {
        if text.len() <= max_width {
            return vec![text.to_string()];
        }

        let mut lines = Vec::new();
        let mut remaining = text;
        while !remaining.is_empty() {
            if remaining.len() <= max_width {
                lines.push(remaining.to_string());
                break;
            }
            // try to break at a reasonable point (space, comma, slash)
            let break_at = remaining[..max_width]
                .rfind(|c: char| [' ', ',', '/', '&', '?'].contains(&c))
                .map(|i| i + 1)
                .unwrap_or(max_width);
            lines.push(remaining[..break_at].to_string());
            remaining = &remaining[break_at..];
        }
        lines
    }

    /// Formats element ID(s) for display. For modified diffs, shows both source and target IDs.
    fn format_element_ids(diff: &Diff) -> Option<String> {
        match diff {
            Diff::SourceNode { element_id, .. }
            | Diff::TargetNode { element_id, .. }
            | Diff::SourceRelationship { element_id, .. }
            | Diff::TargetRelationship { element_id, .. } => {
                element_id.as_ref().map(|eid| format!("[{}]", eid))
            }
            Diff::ModifiedNode {
                source_element_id,
                target_element_id,
                ..
            }
            | Diff::ModifiedRelationship {
                source_element_id,
                target_element_id,
                ..
            } => match (source_element_id.as_ref(), target_element_id.as_ref()) {
                (Some(src), Some(tgt)) => Some(format!("[src: {} | tgt: {}]", src, tgt)),
                (Some(src), None) => Some(format!("[src: {}]", src)),
                (None, Some(tgt)) => Some(format!("[tgt: {}]", tgt)),
                (None, None) => None,
            },
            _ => None,
        }
    }

    // generates a MATCH query for the selected diff's entity
    fn generate_match_query(diff: &Diff) -> Option<String> {
        match diff {
            Diff::SourceNode {
                element_id: Some(eid),
                ..
            } => Some(format!("MATCH (n) WHERE elementId(n) = '{}' RETURN n", eid)),
            Diff::TargetNode {
                element_id: Some(eid),
                ..
            } => Some(format!("MATCH (n) WHERE elementId(n) = '{}' RETURN n", eid)),
            Diff::ModifiedNode {
                source_element_id,
                target_element_id,
                ..
            } => {
                let src = source_element_id.as_deref()?;
                let tgt = target_element_id.as_deref()?;
                Some(format!(
                    "// source\nMATCH (n) WHERE elementId(n) = '{}' RETURN n\n// target\nMATCH (n) WHERE elementId(n) = '{}' RETURN n",
                    src, tgt
                ))
            }
            Diff::SourceRelationship {
                element_id: Some(eid),
                ..
            } => Some(format!(
                "MATCH ()-[r]->() WHERE elementId(r) = '{}' RETURN r",
                eid
            )),
            Diff::TargetRelationship {
                element_id: Some(eid),
                ..
            } => Some(format!(
                "MATCH ()-[r]->() WHERE elementId(r) = '{}' RETURN r",
                eid
            )),
            Diff::ModifiedRelationship {
                source_element_id,
                target_element_id,
                ..
            } => {
                let src = source_element_id.as_deref()?;
                let tgt = target_element_id.as_deref()?;
                Some(format!(
                    "// source\nMATCH ()-[r]->() WHERE elementId(r) = '{}' RETURN r\n// target\nMATCH ()-[r]->() WHERE elementId(r) = '{}' RETURN r",
                    src, tgt
                ))
            }
            _ => None,
        }
    }

    // finds the diff corresponding to a tree selection path
    fn find_diff_by_tree_path<'a>(diffs: &'a [Diff], path: &[String]) -> Option<&'a Diff> {
        // tree path examples:
        // ["nodes", "label_Person", "node_Alice"] -> node diff with id "Alice"
        // ["schema", "schema_0"] -> first schema diff
        if path.is_empty() {
            return None;
        }

        // check for node path: ["nodes", "label_X", "node_{idx}_{category}_{id}"]
        if path.len() >= 3 && path[0] == "nodes" {
            let last = path.last()?;
            // parse format: "node_{idx}_{prefix}_{id}" where prefix is -, +, or ~
            if let Some(rest) = last.strip_prefix("node_") {
                // find the index (first segment before _)
                let first_underscore = rest.find('_')?;
                let idx_str = &rest[..first_underscore];
                let idx: usize = idx_str.parse().ok()?;
                let after_idx = &rest[first_underscore + 1..];

                // extract category prefix and id
                let (category_char, id) = if let Some(id) = after_idx.strip_prefix("-_") {
                    (Some('-'), id)
                } else if let Some(id) = after_idx.strip_prefix("+_") {
                    (Some('+'), id)
                } else if let Some(id) = after_idx.strip_prefix("~_") {
                    (Some('~'), id)
                } else {
                    // fallback for unknown prefix
                    (None, after_idx)
                };

                // use the index to directly get the diff from node_diffs
                let node_diffs: Vec<_> = diffs
                    .iter()
                    .filter(|d| {
                        matches!(
                            d,
                            Diff::SourceNode { .. }
                                | Diff::TargetNode { .. }
                                | Diff::ModifiedNode { .. }
                        )
                    })
                    .collect();

                if let Some(&diff) = node_diffs.get(idx) {
                    // verify it matches the expected id and category
                    let id_matches = match diff {
                        Diff::SourceNode { id: node_id, .. }
                        | Diff::TargetNode { id: node_id, .. }
                        | Diff::ModifiedNode { id: node_id, .. } => node_id == id,
                        _ => false,
                    };
                    let category_matches = match category_char {
                        Some('-') => matches!(diff, Diff::SourceNode { .. }),
                        Some('+') => matches!(diff, Diff::TargetNode { .. }),
                        Some('~') => matches!(diff, Diff::ModifiedNode { .. }),
                        None | Some(_) => true,
                    };
                    if id_matches && category_matches {
                        return Some(diff);
                    }
                }
                return None;
            }
            if let Some(key) = last.strip_prefix("synthetic_") {
                // synthetic nodes don't have direct diffs, but their relationships do
                // try to find a relationship attached to this synthetic node
                return diffs.iter().find(|d| match d {
                    Diff::SourceRelationship { start_node, .. }
                    | Diff::TargetRelationship { start_node, .. }
                    | Diff::ModifiedRelationship { start_node, .. } => {
                        Self::node_ref_key(start_node) == key
                    }
                    _ => false,
                });
            }
        }

        // check for relationship path under a node: [..., "node_X_rel_N"]
        for item in path.iter() {
            if item.contains("_rel_") {
                // this is a relationship item; we need to find which rel it corresponds to
                // the format is "{node_item_id}_rel_{index}"
                // we can try to match by index within the relationship diffs
                // For now, return None as relationship selection is more complex
                return None;
            }
        }

        // check for schema path: ["schema", "schema_N"]
        if path.len() >= 2
            && path[0] == "schema"
            && let Some(idx_str) = path[1].strip_prefix("schema_")
            && let Ok(idx) = idx_str.parse::<usize>()
        {
            let schema_diffs: Vec<_> = diffs
                .iter()
                .filter(|d| {
                    matches!(
                        d,
                        Diff::SourceNodeLabel { .. }
                            | Diff::TargetNodeLabel { .. }
                            | Diff::SourceRelationshipType { .. }
                            | Diff::TargetRelationshipType { .. }
                    )
                })
                .collect();
            return schema_diffs.get(idx).copied();
        }

        None
    }

    fn format_node_ref(node: &NodeRef) -> String {
        let labels = if node.labels.is_empty() {
            String::new()
        } else {
            format!(":{}", node.labels.join(":"))
        };
        if node.properties.is_empty() {
            format!("({})", labels)
        } else {
            let props: Vec<_> = node
                .properties
                .iter()
                .map(|(k, v)| format!("{}: {}", k, Self::format_value(v)))
                .collect();
            format!("({} {{{}}})", labels, props.join(", "))
        }
    }

    fn format_value(v: &Value) -> String {
        match v {
            Value::String(s) => format!("\"{}\"", s),
            Value::Array(arr) => serde_json::to_string(arr).unwrap_or_default(),
            _ => v.to_string(),
        }
    }

    fn styled_line(text: &str, style: Style) -> Line<'static> {
        Line::from(Span::styled(text.to_string(), style))
    }

    // creates a unique key for a node from its labels and identifying properties
    fn node_ref_key(node_ref: &NodeRef) -> String {
        let labels = node_ref.labels.join(":");
        let props = serde_json::to_string(&node_ref.properties).unwrap_or_default();
        format!("{}|{}", labels, props)
    }

    fn node_diff_to_ref(diff: &Diff) -> Option<(String, NodeRef)> {
        match diff {
            Diff::SourceNode {
                label, properties, ..
            }
            | Diff::TargetNode {
                label, properties, ..
            } => Some((
                label.clone(),
                NodeRef {
                    labels: vec![label.clone()],
                    properties: properties.clone(),
                },
            )),
            _ => None,
        }
    }

    fn node_sort_key(diff: &Diff) -> String {
        match diff {
            Diff::SourceNode { id, .. }
            | Diff::TargetNode { id, .. }
            | Diff::ModifiedNode { id, .. } => id.clone(),
            _ => String::new(),
        }
    }

    fn rel_endpoints(diff: &Diff) -> Option<(&NodeRef, &NodeRef)> {
        match diff {
            Diff::SourceRelationship {
                start_node,
                end_node,
                ..
            }
            | Diff::TargetRelationship {
                start_node,
                end_node,
                ..
            }
            | Diff::ModifiedRelationship {
                start_node,
                end_node,
                ..
            } => Some((start_node, end_node)),
            _ => None,
        }
    }

    fn rel_sort_key(diff: &Diff) -> String {
        let (start, end) = match Self::rel_endpoints(diff) {
            Some(endpoints) => endpoints,
            None => return String::new(),
        };
        let rel_type = match diff {
            Diff::SourceRelationship {
                relationship_type, ..
            }
            | Diff::TargetRelationship {
                relationship_type, ..
            }
            | Diff::ModifiedRelationship {
                relationship_type, ..
            } => relationship_type,
            _ => return String::new(),
        };
        format!(
            "{}->{}:{}",
            Self::node_ref_key(start),
            Self::node_ref_key(end),
            rel_type
        )
    }
}

#[async_trait]
impl DiffWriter for TuiWriter {
    async fn write(&mut self, diff: &Diff) -> Result<(), Box<dyn Error + Send + Sync>> {
        let _ = self.sender.send(TuiMessage::Diff(diff.clone()));
        Ok(())
    }

    async fn summarize(&mut self) -> Result<DiffSummary, Box<dyn Error + Send + Sync>> {
        self.wait_for_exit().await?;
        let state = self.state.lock().map_err(|e| e.to_string())?;
        Ok(state.summary.clone())
    }
}
