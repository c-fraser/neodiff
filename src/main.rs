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
use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error;
use std::fs::File;
use std::io::{self, IsTerminal};
use std::process;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::mpsc;
use tui_tree_widget::{Tree, TreeItem, TreeState};
use url::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = Args::parse();

    let source = parse_neo4j_uri(&args.source)?;
    let target = parse_neo4j_uri(&args.target)?;
    let diff_config = DiffConfig::new(
        args.nodes.unwrap_or_default(),
        args.exclude_nodes.unwrap_or_default(),
        args.relationships.unwrap_or_default(),
        args.exclude_relationships.unwrap_or_default(),
        args.exclude_property_keys.unwrap_or_default(),
        args.max_diffs,
    )?;

    // use TUI if explicitly requested or if stdout is a TTY
    let use_tui = args.output.is_none()
        && args
            .format
            .as_ref()
            .map(|f| matches!(f, OutputFormat::Tui))
            .unwrap_or_else(|| io::stdout().is_terminal());

    if use_tui {
        let mut writer = TuiWriter::new();
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
}

impl TuiState {
    fn new() -> Self {
        Self {
            diffs: Vec::new(),
            summary: DiffSummary::default(),
            is_complete: false,
            error: None,
            generation: 0,
        }
    }
}

enum TuiMessage {
    Diff(Diff),
    Complete,
    Error(String),
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

impl TuiWriter {
    fn new() -> Self {
        let state = Arc::new(StdMutex::new(TuiState::new()));
        let (sender, receiver) = mpsc::unbounded_channel();
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
            let (summary, is_complete, error) = {
                let tui_state = state.lock().map_err(|e| e.to_string())?;
                if tui_state.generation != cached_generation {
                    cached_items = Self::build_tree_items(&tui_state);
                    cached_generation = tui_state.generation;
                }
                (
                    tui_state.summary.clone(),
                    tui_state.is_complete,
                    tui_state.error.clone(),
                )
            };
            let items = &cached_items;

            spinner_frame = (spinner_frame + 1) % spinner_chars.len();

            terminal.draw(|f| {
                // layout: header row (summary + legend), then diff tree
                let chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([Constraint::Length(5), Constraint::Min(0)])
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
                    Line::from(vec![
                        Span::styled(
                            format!("{} ", spinner_chars[spinner_frame]),
                            Style::default().fg(Color::Cyan),
                        ),
                        Span::styled("Comparing...", Style::default().fg(Color::Cyan)),
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

                // diff tree (main content area)
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
                                .title(" Differences (q quit) ")
                                .title_style(Style::default().bold()),
                        )
                        .style(Style::default().fg(Color::DarkGray));
                    f.render_widget(empty_message, chunks[1]);
                } else if let Ok(tree) = Tree::new(items) {
                    let tree = tree
                        .block(
                            Block::default()
                                .borders(Borders::ALL)
                                .title(" Differences (↑↓ navigate, ←→ collapse/expand, q quit) ")
                                .title_style(Style::default().bold()),
                        )
                        .highlight_style(
                            Style::default()
                                .add_modifier(Modifier::BOLD)
                                .bg(Color::DarkGray),
                        );
                    f.render_stateful_widget(tree, chunks[1], &mut tree_state);
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
                    let (text, _) = Self::format_diff(diff);
                    let style =
                        DiffCategory::from_diff(diff).map_or(Style::default(), |c| c.style());
                    TreeItem::new_leaf(format!("schema_{}", i), Self::styled_line(&text, style))
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
        for node_diff in node_diffs.iter() {
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
            let item_id = format!("node_{}", sort_key);
            let (header, details) = Self::format_diff(node_diff);
            let style = DiffCategory::from_diff(node_diff).map_or(Style::default(), |c| c.style());
            let mut children: Vec<TreeItem<'static, String>> = Vec::new();
            for (j, detail) in details.iter().enumerate() {
                children.push(TreeItem::new_leaf(
                    format!("{}_prop_{}", item_id, j),
                    Self::styled_line(detail, style),
                ));
            }
            let mut node_counts = DiffCounts::default();
            let mut rel_counts = DiffCounts::default();
            node_counts.count_diff(node_diff);
            if let Some(rels) = rels_by_node.get(&node_key) {
                for rel in rels.iter() {
                    rel_counts.count_diff(rel);
                }
                children.extend(Self::build_relationship_items(rels, &node_key, &item_id));
            }
            let header_line = if rel_counts.total() > 0 {
                let mut spans = vec![Span::styled(format!("{} ", header), style)];
                spans.extend(rel_suffix_spans(&rel_counts));
                Line::from(spans)
            } else {
                Self::styled_line(&header, style)
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

    fn format_diff(diff: &Diff) -> (String, Vec<String>) {
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
            }
            | Diff::TargetNode {
                label,
                id,
                properties,
            } => {
                let prefix = DiffCategory::from_diff(diff).map_or("-", |c| c.prefix());
                let header = format!("{} (:{} {{{}}})", prefix, label, id);
                let details: Vec<_> = properties
                    .iter()
                    .map(|(k, v)| format!("  {}: {}", k, Self::format_value(v)))
                    .collect();
                (header, details)
            }
            Diff::ModifiedNode { label, id, changes } => {
                let header = format!("~ (:{} {{{}}})", label, id);
                let details = Self::format_changes(changes);
                (header, details)
            }
            Diff::SourceRelationship {
                relationship_type,
                start_node,
                end_node,
                properties,
            }
            | Diff::TargetRelationship {
                relationship_type,
                start_node,
                end_node,
                properties,
            } => {
                let prefix = DiffCategory::from_diff(diff).map_or("-", |c| c.prefix());
                let header = format!(
                    "{} {}-[:{}]->{}",
                    prefix,
                    Self::format_node_ref(start_node),
                    relationship_type,
                    Self::format_node_ref(end_node)
                );
                let details: Vec<_> = properties
                    .iter()
                    .map(|(k, v)| format!("  {}: {}", k, Self::format_value(v)))
                    .collect();
                (header, details)
            }
            Diff::ModifiedRelationship {
                relationship_type,
                start_node,
                end_node,
                changes,
            } => {
                let header = format!(
                    "~ {}-[:{}]->{}",
                    Self::format_node_ref(start_node),
                    relationship_type,
                    Self::format_node_ref(end_node)
                );
                let details = Self::format_changes(changes);
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

    fn format_changes(changes: &[PropertyDiff]) -> Vec<String> {
        changes
            .iter()
            .map(|c| match c {
                PropertyDiff::Added { key, value } => {
                    format!("  + {}: {}", key, Self::format_value(value))
                }
                PropertyDiff::Removed { key, value } => {
                    format!("  - {}: {}", key, Self::format_value(value))
                }
                PropertyDiff::Changed { key, old, new } => format!(
                    "  ~ {}: {} → {}",
                    key,
                    Self::format_value(old),
                    Self::format_value(new)
                ),
            })
            .collect()
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
