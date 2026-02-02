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

//! # neodiff
//!
//! A [Neo4j](https://neo4j.com/) graph comparison tool that identifies and reports differences
//! between *source* and *target* databases.
//!
//! ## Usage
//!
//! ```rust,no_run
//! use neodiff::{diff_graphs, new_jsonl_writer, DiffConfig, GraphConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     let source = GraphConfig::new("bolt://source:7687", "neo4j", "pass", None);
//!     let target = GraphConfig::new("bolt://target:7687", "neo4j", "pass", None);
//!     let config = DiffConfig::new(vec![], vec![], vec![], vec![], vec![".*_at$".into()], None)?;
//!     let mut writer = new_jsonl_writer(std::io::stdout());
//!     diff_graphs(&source, &target, &config, writer.as_mut()).await
//! }
//! ```

use async_stream::try_stream;
use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};
use neo4rs::{ConfigBuilder, Graph, Query};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error;
use std::fmt::Debug;
use std::io::Write;
use std::pin::Pin;

/// Compares the *source* and *target* *Neo4j* graphs using the `config`, then outputs the
/// differences using the `writer`.
pub async fn diff_graphs(
    source: &GraphConfig,
    target: &GraphConfig,
    config: &DiffConfig,
    writer: &mut dyn DiffWriter,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let source_graph = source.connect().await?;
    let target_graph = target.connect().await?;
    let schema = discover_schema(&source_graph, &target_graph, config).await?;

    // report schema-level differences (labels/types that exist in only one graph)
    for label in &schema.source_only_nodes {
        writer
            .write(&Diff::SourceNodeLabel {
                label: label.clone(),
            })
            .await?;
    }
    for label in &schema.target_only_nodes {
        writer
            .write(&Diff::TargetNodeLabel {
                label: label.clone(),
            })
            .await?;
    }
    for t in &schema.source_only_rels {
        writer
            .write(&Diff::SourceRelationshipType {
                relationship_type: t.clone(),
            })
            .await?;
    }
    for t in &schema.target_only_rels {
        writer
            .write(&Diff::TargetRelationshipType {
                relationship_type: t.clone(),
            })
            .await?;
    }

    // compare nodes by label using sorted merge
    for label in &schema.nodes {
        let id_props = schema.identifiers.get(label);
        diff_stream(
            label,
            stream_nodes(
                &source_graph,
                label,
                id_props,
                &config.exclude_property_keys,
            ),
            stream_nodes(
                &target_graph,
                label,
                id_props,
                &config.exclude_property_keys,
            ),
            writer,
            config.max_diffs_per_entity,
        )
        .await?;
    }

    // compare relationships by type using sorted merge
    for rel_type in &schema.relationships {
        diff_stream(
            rel_type,
            stream_relationships(
                &source_graph,
                rel_type,
                &config.exclude_property_keys,
                config,
                &schema.identifiers,
            ),
            stream_relationships(
                &target_graph,
                rel_type,
                &config.exclude_property_keys,
                config,
                &schema.identifiers,
            ),
            writer,
            config.max_diffs_per_entity,
        )
        .await?;
    }

    writer.summarize().await?;
    Ok(())
}

/// Connection configuration for a *Neo4j* [`Graph`].
#[derive(Clone)]
pub struct GraphConfig {
    uri: String,
    user: String,
    password: String,
    database: String,
}

impl GraphConfig {
    /// Creates a new [`GraphConfig`] with the given connection details.
    ///
    /// If `database` is `None`, defaults to `"neo4j"`.
    pub fn new(
        uri: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
        database: Option<&str>,
    ) -> Self {
        Self {
            uri: uri.into(),
            user: user.into(),
            password: password.into(),
            database: database.unwrap_or("neo4j").to_string(),
        }
    }

    /// Returns the connection URI.
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Returns the database name.
    pub fn database(&self) -> &str {
        &self.database
    }

    async fn connect(&self) -> Result<Graph, Box<dyn Error + Send + Sync>> {
        let cfg = ConfigBuilder::default()
            .uri(&self.uri)
            .user(&self.user)
            .password(&self.password)
            .db(self.database.as_str())
            .build()?;
        Ok(Graph::connect(cfg).await?)
    }
}

impl Debug for GraphConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphConfig")
            .field("uri", &self.uri)
            .field("user", &self.user)
            .field("password", &"[REDACTED]")
            .field("database", &self.database)
            .finish()
    }
}

/// Configuration to include and/or exclude differences between the graphs.
///
/// Exclusions take precedence over inclusions.
#[derive(Debug, Clone, Default)]
pub struct DiffConfig {
    /// Only compare nodes with labels matching these regex [`Patterns`].
    /// If empty, all labels are included.
    pub include_node_labels: Patterns,
    /// Exclude nodes with labels matching these regex [`Patterns`].
    /// Takes precedence over `include_node_labels`.
    pub exclude_node_labels: Patterns,
    /// Only compare relationships with types matching these regex [`Patterns`].
    /// If empty, all types are included.
    pub include_relationship_types: Patterns,
    /// Exclude relationships with types matching these regex [`Patterns`].
    /// Takes precedence over `include_relationship_types`.
    pub exclude_relationship_types: Patterns,
    /// Exclude node or relationship properties with keys matching these regex [`Patterns`].
    pub exclude_property_keys: Patterns,
    /// Maximum number of differences to report per node label or relationship type.
    /// If `None`, all differences are reported.
    pub max_diffs_per_entity: Option<usize>,
}

impl DiffConfig {
    /// Creates a new [`DiffConfig`] with the given filtering configuration.
    pub fn new(
        include_node_labels: Vec<String>,
        exclude_node_labels: Vec<String>,
        include_relationship_types: Vec<String>,
        exclude_relationship_types: Vec<String>,
        exclude_property_keys: Vec<String>,
        max_diffs_per_entity: Option<usize>,
    ) -> Result<Self, regex::Error> {
        Ok(Self {
            include_node_labels: Patterns::new(include_node_labels)?,
            exclude_node_labels: Patterns::new(exclude_node_labels)?,
            include_relationship_types: Patterns::new(include_relationship_types)?,
            exclude_relationship_types: Patterns::new(exclude_relationship_types)?,
            exclude_property_keys: Patterns::new(exclude_property_keys)?,
            max_diffs_per_entity,
        })
    }
}

/// A [`Vec`] of compiled [`Regex`] patterns.
#[derive(Clone, Default)]
pub struct Patterns(Vec<Regex>);

impl Patterns {
    pub fn new(patterns: Vec<String>) -> Result<Self, regex::Error> {
        patterns
            .iter()
            .map(|s| Regex::new(s))
            .collect::<Result<Vec<_>, _>>()
            .map(Self)
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Debug for Patterns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|r| r.as_str()))
            .finish()
    }
}

/// A difference between the *source* and *target* graphs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "diff")]
pub enum Diff {
    /// A node label exists only in the *source* graph.
    SourceNodeLabel { label: String },
    /// A node label exists only in the *target* graph.
    TargetNodeLabel { label: String },
    /// A relationship type exists only in the *source* graph.
    SourceRelationshipType { relationship_type: String },
    /// A relationship type exists only in the *target* graph.
    TargetRelationshipType { relationship_type: String },
    /// A node exists only in the *source* graph.
    SourceNode {
        label: String,
        id: String,
        properties: BTreeMap<String, Value>,
    },
    /// A node exists only in the *target* graph.
    TargetNode {
        label: String,
        id: String,
        properties: BTreeMap<String, Value>,
    },
    /// A node exists in both graphs but has different properties.
    ModifiedNode {
        label: String,
        id: String,
        changes: Vec<PropertyDiff>,
    },
    /// A relationship exists only in the *source* graph.
    SourceRelationship {
        relationship_type: String,
        start_node: NodeRef,
        end_node: NodeRef,
        properties: BTreeMap<String, Value>,
    },
    /// A relationship exists only in the *target* graph.
    TargetRelationship {
        relationship_type: String,
        start_node: NodeRef,
        end_node: NodeRef,
        properties: BTreeMap<String, Value>,
    },
    /// A relationship exists in both graphs but has different properties.
    ModifiedRelationship {
        relationship_type: String,
        start_node: NodeRef,
        end_node: NodeRef,
        changes: Vec<PropertyDiff>,
    },
}

/// A reference to a node in a relationship.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRef {
    /// The node's labels.
    pub labels: Vec<String>,
    /// The node's properties used to identify it (from unique constraints).
    pub properties: BTreeMap<String, Value>,
}

/// A node or relationship property difference.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PropertyDiff {
    /// A property that exists only in the *target*.
    Added { key: String, value: Value },
    /// A property that exists only in the *source*.
    Removed { key: String, value: Value },
    /// A property that exists in both but has different values.
    Changed { key: String, old: Value, new: Value },
}

/// A summary of the differences between the *source* and *target* graphs.
#[derive(Debug, Clone, Default)]
pub struct DiffSummary {
    /// The number of nodes that exist only in the *target* graph.
    pub nodes_added: u64,
    /// The number of nodes that exist only in the *source* graph.
    pub nodes_removed: u64,
    /// The number of nodes that exist in both graphs but have different properties.
    pub nodes_modified: u64,
    /// The number of relationships that exist only in the *target* graph.
    pub relationships_added: u64,
    /// The number of relationships that exist only in the *source* graph.
    pub relationships_removed: u64,
    /// The number of relationships that exist in both graphs but have different properties.
    pub relationships_modified: u64,
    /// The difference counts grouped by node label.
    pub nodes_by_label: BTreeMap<String, Counts>,
    /// The difference counts grouped by relationship type.
    pub relationships_by_type: BTreeMap<String, Counts>,
}

impl DiffSummary {
    /// Returns the total number of differences.
    pub fn total(&self) -> u64 {
        self.nodes_added
            + self.nodes_removed
            + self.nodes_modified
            + self.relationships_added
            + self.relationships_removed
            + self.relationships_modified
    }

    /// Updates the summary per the [`Diff`].
    pub fn update(&mut self, diff: &Diff) {
        match diff {
            Diff::SourceNode { label, .. } => {
                self.nodes_removed += 1;
                self.nodes_by_label
                    .entry(label.clone())
                    .or_default()
                    .removed += 1;
            }
            Diff::TargetNode { label, .. } => {
                self.nodes_added += 1;
                self.nodes_by_label.entry(label.clone()).or_default().added += 1;
            }
            Diff::ModifiedNode { label, .. } => {
                self.nodes_modified += 1;
                self.nodes_by_label
                    .entry(label.clone())
                    .or_default()
                    .modified += 1;
            }
            Diff::SourceRelationship {
                relationship_type, ..
            } => {
                self.relationships_removed += 1;
                self.relationships_by_type
                    .entry(relationship_type.clone())
                    .or_default()
                    .removed += 1;
            }
            Diff::TargetRelationship {
                relationship_type, ..
            } => {
                self.relationships_added += 1;
                self.relationships_by_type
                    .entry(relationship_type.clone())
                    .or_default()
                    .added += 1;
            }
            Diff::ModifiedRelationship {
                relationship_type, ..
            } => {
                self.relationships_modified += 1;
                self.relationships_by_type
                    .entry(relationship_type.clone())
                    .or_default()
                    .modified += 1;
            }
            _ => {}
        }
    }
}

/// Difference counts for a node label or relationship type.
#[derive(Debug, Clone, Default)]
pub struct Counts {
    /// Count of entities that exist only in the *target* graph.
    pub added: u64,
    /// Count of entities that exist only in the *source* graph.
    pub removed: u64,
    /// Count of entities that exist in both graphs but have different properties.
    pub modified: u64,
}

/// Writes graph differences to an output destination.
#[async_trait]
pub trait DiffWriter: Send + Sync {
    /// Writes a [`Diff`] to the output.
    async fn write(&mut self, diff: &Diff) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Finalizes the output then returns the [`DiffSummary`].
    async fn summarize(&mut self) -> Result<DiffSummary, Box<dyn Error + Send + Sync>>;
}

/// Initializes a [JSON Lines](https://jsonlines.org/) [`DiffWriter`] for the `output`.
pub fn new_jsonl_writer<W: Write + Send + Sync + 'static>(output: W) -> Box<dyn DiffWriter> {
    Box::new(JsonLinesWriter {
        output,
        summary: DiffSummary::default(),
    })
}

struct Schema {
    /// Node label -> identifying property keys (from unique/node_key constraints).
    identifiers: HashMap<String, Vec<String>>,
    /// Labels present in either graph (sorted).
    nodes: Vec<String>,
    /// Relationship types present in either graph (sorted).
    relationships: Vec<String>,
    /// Labels unique to source graph.
    source_only_nodes: Vec<String>,
    /// Labels unique to target graph.
    target_only_nodes: Vec<String>,
    /// Relationship types unique to source graph.
    source_only_rels: Vec<String>,
    /// Relationship types unique to target graph.
    target_only_rels: Vec<String>,
}

async fn discover_schema(
    source: &Graph,
    target: &Graph,
    config: &DiffConfig,
) -> Result<Schema, Box<dyn Error + Send + Sync>> {
    let (src_nodes, tgt_nodes, src_rels, tgt_rels, src_constraints, tgt_constraints) = tokio::try_join!(
        query_labels(
            source,
            &config.include_node_labels,
            &config.exclude_node_labels
        ),
        query_labels(
            target,
            &config.include_node_labels,
            &config.exclude_node_labels
        ),
        query_relationship_types(
            source,
            &config.include_relationship_types,
            &config.exclude_relationship_types
        ),
        query_relationship_types(
            target,
            &config.include_relationship_types,
            &config.exclude_relationship_types
        ),
        query_constraints(source),
        query_constraints(target),
    )?;

    // merge constraints: use source constraint if target matches or is absent,
    // skip labels where constraints differ (no reliable identifier)
    let mut constraints = HashMap::new();
    for (label, src_props) in &src_constraints {
        match tgt_constraints.get(label) {
            Some(tgt_props) if tgt_props == src_props => {
                constraints.insert(label.clone(), src_props.clone());
            }
            None => {
                constraints.insert(label.clone(), src_props.clone());
            }
            _ => {}
        }
    }
    for (label, tgt_props) in tgt_constraints {
        if !src_constraints.contains_key(&label) {
            constraints.insert(label, tgt_props);
        }
    }

    // union and sort labels/types for deterministic ordering
    let mut nodes: Vec<_> = src_nodes.union(&tgt_nodes).cloned().collect();
    nodes.sort();
    let mut relationships: Vec<_> = src_rels.union(&tgt_rels).cloned().collect();
    relationships.sort();

    Ok(Schema {
        identifiers: constraints,
        nodes,
        relationships,
        source_only_nodes: src_nodes.difference(&tgt_nodes).cloned().collect(),
        target_only_nodes: tgt_nodes.difference(&src_nodes).cloned().collect(),
        source_only_rels: src_rels.difference(&tgt_rels).cloned().collect(),
        target_only_rels: tgt_rels.difference(&src_rels).cloned().collect(),
    })
}

async fn query_labels(
    graph: &Graph,
    include: &Patterns,
    exclude: &Patterns,
) -> Result<HashSet<String>, Box<dyn Error + Send + Sync>> {
    let where_clause = build_pattern_filter("label", include, exclude);
    let query = format!("CALL db.labels() YIELD label{where_clause} RETURN label");
    let mut result = graph.execute(Query::new(query)).await?;
    let mut labels = HashSet::new();
    while let Some(row) = result.next().await? {
        if let Ok(label) = row.get::<String>("label") {
            labels.insert(label);
        }
    }
    Ok(labels)
}

async fn query_relationship_types(
    graph: &Graph,
    include: &Patterns,
    exclude: &Patterns,
) -> Result<HashSet<String>, Box<dyn Error + Send + Sync>> {
    let where_clause = build_pattern_filter("relationshipType", include, exclude);
    let query = format!(
        "CALL db.relationshipTypes() YIELD relationshipType{where_clause} RETURN relationshipType"
    );
    let mut result = graph.execute(Query::new(query)).await?;
    let mut types = HashSet::new();
    while let Some(row) = result.next().await? {
        if let Ok(t) = row.get::<String>("relationshipType") {
            types.insert(t);
        }
    }
    Ok(types)
}

async fn query_constraints(
    graph: &Graph,
) -> Result<HashMap<String, Vec<String>>, Box<dyn Error + Send + Sync>> {
    let query = Query::new(
        "SHOW CONSTRAINTS YIELD type, entityType, labelsOrTypes, properties \
         WHERE type IN ['UNIQUENESS', 'NODE_KEY'] AND entityType = 'NODE' \
         RETURN type, labelsOrTypes, properties"
            .into(),
    );
    let mut result = graph.execute(query).await?;
    let mut constraints: HashMap<String, Vec<String>> = HashMap::new();
    while let Some(row) = result.next().await? {
        let constraint_type: String = row.get("type").unwrap_or_default();
        let labels: Vec<String> = row.get("labelsOrTypes").unwrap_or_default();
        let props: Vec<String> = row.get("properties").unwrap_or_default();
        for label in labels {
            let existing = constraints.entry(label).or_default();
            // NODE_KEY takes precedence; otherwise use first constraint found
            if constraint_type == "NODE_KEY" || existing.is_empty() {
                *existing = props.clone();
            }
        }
    }
    Ok(constraints)
}

/// Builds a Cypher WHERE clause for include/exclude pattern matching on `var`.
fn build_pattern_filter(var: &str, include: &Patterns, exclude: &Patterns) -> String {
    let mut conditions = Vec::new();

    if !exclude.is_empty() {
        let patterns = exclude
            .0
            .iter()
            .map(|r| format!("'{}'", r.as_str()))
            .collect::<Vec<_>>()
            .join(",");
        conditions.push(format!("NOT any(p IN [{patterns}] WHERE {var} =~ p)"));
    }

    if !include.is_empty() {
        let patterns = include
            .0
            .iter()
            .map(|r| format!("'{}'", r.as_str()))
            .collect::<Vec<_>>()
            .join(",");
        conditions.push(format!("any(p IN [{patterns}] WHERE {var} =~ p)"));
    }

    if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    }
}

/// Async stream of database rows, boxed for use in sorted merge.
type BoxStream<'a, T> =
    Pin<Box<dyn Stream<Item = Result<T, Box<dyn Error + Send + Sync>>> + Send + 'a>>;

/// Common interface for nodes and relationships in sorted merge comparison.
trait Diffable {
    fn id(&self) -> &str;
    fn props(&self) -> &BTreeMap<String, Value>;
    fn source_diff(&self, key: &str) -> Diff;
    fn target_diff(&self, key: &str) -> Diff;
    fn modified_diff(&self, key: &str, changes: Vec<PropertyDiff>) -> Diff;
}

#[derive(Clone, PartialEq, Eq)]
struct Node {
    id: String,
    props: BTreeMap<String, Value>,
}

impl Diffable for Node {
    fn id(&self) -> &str {
        &self.id
    }

    fn props(&self) -> &BTreeMap<String, Value> {
        &self.props
    }

    fn source_diff(&self, label: &str) -> Diff {
        Diff::SourceNode {
            label: label.into(),
            id: self.id.clone(),
            properties: self.props.clone(),
        }
    }

    fn target_diff(&self, label: &str) -> Diff {
        Diff::TargetNode {
            label: label.into(),
            id: self.id.clone(),
            properties: self.props.clone(),
        }
    }

    fn modified_diff(&self, label: &str, changes: Vec<PropertyDiff>) -> Diff {
        Diff::ModifiedNode {
            label: label.into(),
            id: self.id.clone(),
            changes,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
struct Rel {
    id: String,
    start: NodeRef,
    end: NodeRef,
    props: BTreeMap<String, Value>,
}

impl Diffable for Rel {
    fn id(&self) -> &str {
        &self.id
    }

    fn props(&self) -> &BTreeMap<String, Value> {
        &self.props
    }

    fn source_diff(&self, rel_type: &str) -> Diff {
        Diff::SourceRelationship {
            relationship_type: rel_type.into(),
            start_node: self.start.clone(),
            end_node: self.end.clone(),
            properties: self.props.clone(),
        }
    }

    fn target_diff(&self, rel_type: &str) -> Diff {
        Diff::TargetRelationship {
            relationship_type: rel_type.into(),
            start_node: self.start.clone(),
            end_node: self.end.clone(),
            properties: self.props.clone(),
        }
    }

    fn modified_diff(&self, rel_type: &str, changes: Vec<PropertyDiff>) -> Diff {
        Diff::ModifiedRelationship {
            relationship_type: rel_type.into(),
            start_node: self.start.clone(),
            end_node: self.end.clone(),
            changes,
        }
    }
}

fn stream_nodes<'a>(
    graph: &'a Graph,
    label: &'a str,
    id_props: Option<&'a Vec<String>>,
    exclude_patterns: &'a Patterns,
) -> BoxStream<'a, Node> {
    Box::pin(try_stream! {
        let props_expr = build_props_expr("properties(n)", exclude_patterns);

        // use constraint properties as identity if available; otherwise hash all properties
        let query = match id_props {
            Some(props) if !props.is_empty() => {
                let id_expr = props.iter()
                    .map(|p| format!("coalesce(toString(n.{}),'')", p))
                    .collect::<Vec<_>>()
                    .join("+'::'+");
                Query::new(format!(
                    "MATCH (n:{label}) WHERE n.{} IS NOT NULL \
                     WITH n, ({id_expr}) AS __id \
                     RETURN __id, {props_expr} AS props \
                     ORDER BY __id",
                    props[0]
                ))
            }
            _ => Query::new(format!(
                "MATCH (n:{label}) \
                 WITH n, {props_expr} AS props \
                 WITH n, props, apoc.hashing.fingerprint(props) AS __id \
                 RETURN __id, props \
                 ORDER BY __id"
            )),
        };

        let mut result = graph.execute(query).await?;
        while let Some(row) = result.next().await? {
            let id: String = row.get("__id").unwrap_or_default();
            let props = value_to_props(&row.get::<Value>("props").unwrap_or_default());
            yield Node { id, props };
        }
    })
}

fn stream_relationships<'a>(
    graph: &'a Graph,
    rel_type: &'a str,
    exclude_patterns: &'a Patterns,
    config: &'a DiffConfig,
    identifiers: &'a HashMap<String, Vec<String>>,
) -> BoxStream<'a, Rel> {
    Box::pin(try_stream! {
        let start_props_expr = build_props_expr("properties(s)", exclude_patterns);
        let end_props_expr = build_props_expr("properties(e)", exclude_patterns);
        let rel_props_expr = build_props_expr("properties(r)", exclude_patterns);

        let label_filter = build_label_filter(config);
        let start_id_expr = build_node_identity_expr("s", identifiers, exclude_patterns);
        let end_id_expr = build_node_identity_expr("e", identifiers, exclude_patterns);

        let query = Query::new(format!(
            "MATCH (s)-[r:{rel_type}]->(e){label_filter} \
             WITH r, s, e, \
                  {start_props_expr} AS start_props, \
                  {end_props_expr} AS end_props, \
                  {rel_props_expr} AS props \
             WITH r, s, e, start_props, end_props, props, \
                  ({start_id_expr}) + '->' + ({end_id_expr}) + ':' + apoc.hashing.fingerprint(props) AS __id \
             RETURN __id, labels(s) AS start_labels, start_props, labels(e) AS end_labels, end_props, props \
             ORDER BY __id"
        ));

        let mut result = graph.execute(query).await?;
        while let Some(row) = result.next().await? {
            let id: String = row.get("__id").unwrap_or_default();
            let start_labels: Vec<String> = row.get("start_labels").unwrap_or_default();
            let end_labels: Vec<String> = row.get("end_labels").unwrap_or_default();
            let start_props: Value = row.get("start_props").unwrap_or_default();
            let end_props: Value = row.get("end_props").unwrap_or_default();
            let rel_props: Value = row.get("props").unwrap_or_default();
            yield Rel {
                id,
                start: NodeRef { labels: start_labels, properties: value_to_props(&start_props) },
                end: NodeRef { labels: end_labels, properties: value_to_props(&end_props) },
                props: value_to_props(&rel_props),
            };
        }
    })
}

/// Compares two sorted streams via merge join. Items with matching IDs are compared for property
/// differences; unmatched items are reported as *source*-only or *target*-only.
async fn diff_stream<T: Diffable>(
    key: &str,
    mut source: BoxStream<'_, T>,
    mut target: BoxStream<'_, T>,
    writer: &mut dyn DiffWriter,
    max_diffs: Option<usize>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut src_item = source.next().await.transpose()?;
    let mut tgt_item = target.next().await.transpose()?;
    let mut count = 0usize;

    loop {
        if max_diffs.is_some_and(|max| count >= max) {
            break;
        }
        match (&src_item, &tgt_item) {
            (None, None) => break,
            (Some(s), None) => {
                writer.write(&s.source_diff(key)).await?;
                count += 1;
                src_item = source.next().await.transpose()?;
            }
            (None, Some(t)) => {
                writer.write(&t.target_diff(key)).await?;
                count += 1;
                tgt_item = target.next().await.transpose()?;
            }
            (Some(s), Some(t)) => match s.id().cmp(t.id()) {
                Ordering::Less => {
                    writer.write(&s.source_diff(key)).await?;
                    count += 1;
                    src_item = source.next().await.transpose()?;
                }
                Ordering::Greater => {
                    writer.write(&t.target_diff(key)).await?;
                    count += 1;
                    tgt_item = target.next().await.transpose()?;
                }
                Ordering::Equal => {
                    let changes = diff_props(s.props(), t.props());
                    if !changes.is_empty() {
                        writer.write(&s.modified_diff(key, changes)).await?;
                        count += 1;
                    }
                    src_item = source.next().await.transpose()?;
                    tgt_item = target.next().await.transpose()?;
                }
            },
        }
    }

    Ok(())
}

fn diff_props(
    left: &BTreeMap<String, Value>,
    right: &BTreeMap<String, Value>,
) -> Vec<PropertyDiff> {
    let mut changes = Vec::new();

    // find changed and removed (keys in left)
    for (k, lv) in left {
        match right.get(k) {
            Some(rv) if lv != rv => changes.push(PropertyDiff::Changed {
                key: k.clone(),
                old: lv.clone(),
                new: rv.clone(),
            }),
            None => changes.push(PropertyDiff::Removed {
                key: k.clone(),
                value: lv.clone(),
            }),
            _ => {}
        }
    }

    // find added (keys only in right)
    for (k, rv) in right {
        if !left.contains_key(k) {
            changes.push(PropertyDiff::Added {
                key: k.clone(),
                value: rv.clone(),
            });
        }
    }

    changes
}

/// Builds a *WHERE* clause that filters relationships by endpoint node labels.
fn build_label_filter(config: &DiffConfig) -> String {
    if config.include_node_labels.is_empty() && config.exclude_node_labels.is_empty() {
        return String::new();
    }

    let mut conditions = Vec::new();
    if !config.exclude_node_labels.is_empty() {
        let patterns = config
            .exclude_node_labels
            .0
            .iter()
            .map(|r| format!("'{}'", r.as_str()))
            .collect::<Vec<_>>()
            .join(",");
        conditions.push(format!("NOT any(p IN [{patterns}] WHERE lbl =~ p)"));
    }
    if !config.include_node_labels.is_empty() {
        let patterns = config
            .include_node_labels
            .0
            .iter()
            .map(|r| format!("'{}'", r.as_str()))
            .collect::<Vec<_>>()
            .join(",");
        conditions.push(format!("any(p IN [{patterns}] WHERE lbl =~ p)"));
    }

    let filter = conditions.join(" AND ");
    let start_ok = format!("(size(labels(s)) = 0 OR any(lbl IN labels(s) WHERE {filter}))");
    let end_ok = format!("(size(labels(e)) = 0 OR any(lbl IN labels(e) WHERE {filter}))");
    format!(" WHERE {start_ok} AND {end_ok}")
}

/// Builds a Cypher expression that computes a node's identity. Uses constraint-based
/// identifier properties when the node has a matching label; falls back to fingerprinting
/// all properties (with exclusions) otherwise.
fn build_node_identity_expr(
    node_var: &str,
    identifiers: &HashMap<String, Vec<String>>,
    exclude_patterns: &Patterns,
) -> String {
    // sort labels alphabetically for deterministic CASE ordering
    let mut labels: Vec<_> = identifiers.keys().collect();
    labels.sort();

    let mut cases = Vec::new();
    for label in labels {
        let props = &identifiers[label];
        if props.is_empty() {
            continue;
        }

        // condition: label present AND first id property is not null
        let condition = format!(
            "'{}' IN labels({}) AND {}.{} IS NOT NULL",
            label, node_var, node_var, props[0]
        );

        // identity: "Label:" + concatenated property values
        let prop_values: Vec<_> = props
            .iter()
            .map(|p| format!("coalesce(toString({}.{}), '')", node_var, p))
            .collect();
        let id_value = format!("'{}:' + {}", label, prop_values.join(" + '::' + "));

        cases.push(format!("WHEN {} THEN {}", condition, id_value));
    }

    // fallback: fingerprint of all properties (with exclusions applied)
    let fallback_props = build_props_expr(&format!("properties({})", node_var), exclude_patterns);
    let fallback = format!("apoc.hashing.fingerprint({})", fallback_props);

    if cases.is_empty() {
        fallback
    } else {
        format!("CASE {} ELSE {} END", cases.join(" "), fallback)
    }
}

/// Builds a Cypher expression that filters out excluded property keys from a map.
fn build_props_expr(map_var: &str, exclude_patterns: &Patterns) -> String {
    if exclude_patterns.is_empty() {
        return map_var.to_string();
    }

    let patterns = exclude_patterns
        .0
        .iter()
        .map(|r| format!("'{}'", r.as_str()))
        .collect::<Vec<_>>()
        .join(",");

    format!(
        "apoc.map.fromPairs([k IN keys({map_var}) WHERE NOT any(p IN [{patterns}] WHERE k =~ p) | [k, {map_var}[k]]])"
    )
}

/// Converts a JSON object Value to a property map; non-objects yield empty map.
fn value_to_props(data: &Value) -> BTreeMap<String, Value> {
    let mut props = BTreeMap::new();
    if let Some(obj) = data.as_object() {
        for (k, v) in obj {
            props.insert(k.clone(), v.clone());
        }
    }
    props
}

struct JsonLinesWriter<T: Write + Send> {
    output: T,
    summary: DiffSummary,
}

#[async_trait]
impl<W: Write + Send + Sync> DiffWriter for JsonLinesWriter<W> {
    async fn write(&mut self, diff: &Diff) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.summary.update(diff);
        writeln!(self.output, "{}", serde_json::to_string(diff)?)?;
        Ok(())
    }

    async fn summarize(&mut self) -> Result<DiffSummary, Box<dyn Error + Send + Sync>> {
        self.output.flush()?;
        Ok(self.summary.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex, OnceLock};
    use testcontainers::ContainerAsync;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::neo4j::{Neo4j, Neo4jImage, Neo4jLabsPlugin};
    use tokio::sync::Mutex as AsyncMutex;

    struct TestWriter {
        diffs: Arc<Mutex<Vec<Diff>>>,
    }

    impl TestWriter {
        fn new() -> Self {
            Self {
                diffs: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn diffs(&self) -> Vec<Diff> {
            self.diffs.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl DiffWriter for TestWriter {
        async fn write(&mut self, diff: &Diff) -> Result<(), Box<dyn Error + Send + Sync>> {
            self.diffs
                .lock()
                .map_err(|e| e.to_string())?
                .push(diff.clone());
            Ok(())
        }

        async fn summarize(&mut self) -> Result<DiffSummary, Box<dyn Error + Send + Sync>> {
            let diffs = self.diffs.lock().map_err(|e| e.to_string())?;
            let mut summary = DiffSummary::default();
            for diff in diffs.iter() {
                summary.update(diff);
            }
            Ok(summary)
        }
    }

    struct TestEnv {
        #[allow(dead_code)]
        source_container: ContainerAsync<Neo4jImage>,
        #[allow(dead_code)]
        target_container: ContainerAsync<Neo4jImage>,
        source_uri: String,
        target_uri: String,
    }

    impl TestEnv {
        async fn new() -> Result<Self, Box<dyn Error + Send + Sync>> {
            let neo4j = Neo4j::default().with_neo4j_labs_plugin(&[Neo4jLabsPlugin::Apoc]);
            let source_container = neo4j.clone().start().await?;
            let target_container = neo4j.start().await?;
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let source_uri = format!(
                "bolt://127.0.0.1:{}",
                source_container.get_host_port_ipv4(7687).await?
            );
            let target_uri = format!(
                "bolt://127.0.0.1:{}",
                target_container.get_host_port_ipv4(7687).await?
            );
            Ok(Self {
                source_container,
                target_container,
                source_uri,
                target_uri,
            })
        }

        async fn clear(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
            self.both(&["MATCH (n) DETACH DELETE n"]).await
        }

        async fn source(&self, queries: &[&str]) -> Result<(), Box<dyn Error + Send + Sync>> {
            run_queries(&self.source_uri, queries).await
        }

        async fn target(&self, queries: &[&str]) -> Result<(), Box<dyn Error + Send + Sync>> {
            run_queries(&self.target_uri, queries).await
        }

        async fn both(&self, queries: &[&str]) -> Result<(), Box<dyn Error + Send + Sync>> {
            self.source(queries).await?;
            self.target(queries).await
        }

        async fn diff(&self) -> Result<TestWriter, Box<dyn Error + Send + Sync>> {
            self.diff_with(DiffConfig::default()).await
        }

        async fn diff_with(
            &self,
            config: DiffConfig,
        ) -> Result<TestWriter, Box<dyn Error + Send + Sync>> {
            let source = GraphConfig::new(&self.source_uri, "neo4j", "password", None);
            let target = GraphConfig::new(&self.target_uri, "neo4j", "password", None);
            let mut writer = TestWriter::new();
            diff_graphs(&source, &target, &config, &mut writer).await?;
            Ok(writer)
        }
    }

    async fn run_queries(uri: &str, queries: &[&str]) -> Result<(), Box<dyn Error + Send + Sync>> {
        let config = ConfigBuilder::default()
            .uri(uri)
            .user("neo4j")
            .password("password")
            .build()?;
        let graph = Graph::connect(config).await?;
        for query in queries {
            if !query.trim().is_empty() {
                graph.run(Query::new(query.to_string())).await?;
            }
        }
        Ok(())
    }

    static TEST_ENV: OnceLock<AsyncMutex<Option<TestEnv>>> = OnceLock::new();

    async fn get_env()
    -> Result<tokio::sync::MutexGuard<'static, Option<TestEnv>>, Box<dyn Error + Send + Sync>> {
        let mutex = TEST_ENV.get_or_init(|| AsyncMutex::new(None));
        let mut guard = mutex.lock().await;
        if guard.is_none() {
            *guard = Some(TestEnv::new().await?);
        }
        Ok(guard)
    }

    #[test]
    fn test_diff_props_changes() {
        let left: BTreeMap<String, Value> =
            [("a".into(), Value::from(1)), ("b".into(), Value::from(2))].into();
        let right: BTreeMap<String, Value> =
            [("b".into(), Value::from(3)), ("c".into(), Value::from(4))].into();
        let changes = diff_props(&left, &right);
        assert_eq!(changes.len(), 3);
    }

    #[test]
    fn test_diff_props_identical() {
        let left: BTreeMap<String, Value> =
            [("a".into(), Value::from(1)), ("b".into(), Value::from(2))].into();
        let right: BTreeMap<String, Value> =
            [("a".into(), Value::from(1)), ("b".into(), Value::from(2))].into();
        assert!(diff_props(&left, &right).is_empty());
    }

    #[test]
    fn test_diff_props_empty() {
        let empty: BTreeMap<String, Value> = BTreeMap::new();
        assert!(diff_props(&empty, &empty).is_empty());
    }

    #[test]
    fn test_diff_props_all_added() {
        let empty: BTreeMap<String, Value> = BTreeMap::new();
        let right: BTreeMap<String, Value> =
            [("a".into(), Value::from(1)), ("b".into(), Value::from(2))].into();
        let changes = diff_props(&empty, &right);
        assert_eq!(changes.len(), 2);
        assert!(
            changes
                .iter()
                .all(|c| matches!(c, PropertyDiff::Added { .. }))
        );
    }

    #[test]
    fn test_diff_props_all_removed() {
        let left: BTreeMap<String, Value> =
            [("a".into(), Value::from(1)), ("b".into(), Value::from(2))].into();
        let empty: BTreeMap<String, Value> = BTreeMap::new();
        let changes = diff_props(&left, &empty);
        assert_eq!(changes.len(), 2);
        assert!(
            changes
                .iter()
                .all(|c| matches!(c, PropertyDiff::Removed { .. }))
        );
    }

    #[tokio::test]
    async fn test_diff_identical() -> Result<(), Box<dyn Error + Send + Sync>> {
        let guard = get_env().await?;
        let env = guard.as_ref().unwrap();
        env.clear().await?;
        env.both(&[
                "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE (p.name) IS UNIQUE",
                "CREATE (p:Person {name: 'Alice', born: 1990})",
                "CREATE (p:Person {name: 'Bob', born: 1985})",
                "CREATE (m:Movie {title: 'Test Movie', released: 2020})",
                "MATCH (a:Person {name: 'Alice'}), (m:Movie {title: 'Test Movie'}) CREATE (a)-[:ACTED_IN {roles: ['Lead']}]->(m)",
            ]).await?;
        let summary = env.diff().await?.summarize().await?;
        assert_eq!(summary.total(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_diff_node_added() -> Result<(), Box<dyn Error + Send + Sync>> {
        let guard = get_env().await?;
        let env = guard.as_ref().unwrap();
        env.clear().await?;
        env.both(&[
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE (p.name) IS UNIQUE",
            "CREATE (p:Person {name: 'Alice', born: 1990})",
        ])
        .await?;
        env.target(&["CREATE (p:Person {name: 'Bob', born: 1985})"])
            .await?;
        let summary = env.diff().await?.summarize().await?;
        assert_eq!(summary.nodes_added, 1);
        assert_eq!(summary.nodes_removed, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_diff_node_removed() -> Result<(), Box<dyn Error + Send + Sync>> {
        let guard = get_env().await?;
        let env = guard.as_ref().unwrap();
        env.clear().await?;
        env.both(&[
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE (p.name) IS UNIQUE",
            "CREATE (p:Person {name: 'Alice', born: 1990})",
        ])
        .await?;
        env.source(&["CREATE (p:Person {name: 'Bob', born: 1985})"])
            .await?;
        let summary = env.diff().await?.summarize().await?;
        assert_eq!(summary.nodes_removed, 1);
        assert_eq!(summary.nodes_added, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_diff_node_modified() -> Result<(), Box<dyn Error + Send + Sync>> {
        let guard = get_env().await?;
        let env = guard.as_ref().unwrap();
        env.clear().await?;
        env.both(&["CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE (p.name) IS UNIQUE"])
            .await?;
        env.source(&["CREATE (p:Person {name: 'Alice', born: 1990})"])
            .await?;
        env.target(&["CREATE (p:Person {name: 'Alice', born: 1991})"])
            .await?;
        let summary = env.diff().await?.summarize().await?;
        assert_eq!(summary.nodes_modified, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_diff_node_added_and_removed() -> Result<(), Box<dyn Error + Send + Sync>> {
        let guard = get_env().await?;
        let env = guard.as_ref().unwrap();
        env.clear().await?;
        env.both(&[
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE (p.name) IS UNIQUE",
            "CREATE (p:Person {name: 'Bob', born: 1985})",
        ])
        .await?;
        env.source(&["CREATE (p:Person {name: 'Alice', born: 1990})"])
            .await?;
        env.target(&["CREATE (p:Person {name: 'Charlie', born: 2000})"])
            .await?;
        let summary = env.diff().await?.summarize().await?;
        assert_eq!(summary.nodes_removed, 1);
        assert_eq!(summary.nodes_added, 1);
        assert_eq!(summary.nodes_modified, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_diff_rel_removed() -> Result<(), Box<dyn Error + Send + Sync>> {
        let guard = get_env().await?;
        let env = guard.as_ref().unwrap();
        env.clear().await?;
        env.both(&[
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE (p.name) IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (m:Movie) REQUIRE (m.title) IS UNIQUE",
            "CREATE (p:Person {name: 'Alice', born: 1990})",
            "CREATE (m:Movie {title: 'Test Movie', released: 2020})",
        ])
        .await?;
        env.source(&["MATCH (a:Person {name: 'Alice'}), (m:Movie {title: 'Test Movie'}) CREATE (a)-[:ACTED_IN {roles: ['Lead']}]->(m)"]).await?;
        let mut writer = env.diff().await?;
        let summary = writer.summarize().await?;
        assert_eq!(summary.relationships_removed, 1);
        let has_rel_type_diff = writer.diffs().iter().any(|d| {
                matches!(d, Diff::SourceRelationshipType { relationship_type } if relationship_type == "ACTED_IN")
            });
        assert!(has_rel_type_diff);
        Ok(())
    }

    #[tokio::test]
    async fn test_diff_rel_added() -> Result<(), Box<dyn Error + Send + Sync>> {
        let guard = get_env().await?;
        let env = guard.as_ref().unwrap();
        env.clear().await?;
        env.both(&[
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE (p.name) IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (m:Movie) REQUIRE (m.title) IS UNIQUE",
            "CREATE (p:Person {name: 'Alice', born: 1990})",
            "CREATE (m:Movie {title: 'Test Movie', released: 2020})",
        ])
        .await?;
        env.target(&["MATCH (a:Person {name: 'Alice'}), (m:Movie {title: 'Test Movie'}) CREATE (a)-[:ACTED_IN {roles: ['Lead']}]->(m)"]).await?;
        let summary = env.diff().await?.summarize().await?;
        assert_eq!(summary.relationships_added, 1);
        assert_eq!(summary.relationships_removed, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_diff_target_only_label() -> Result<(), Box<dyn Error + Send + Sync>> {
        let guard = get_env().await?;
        let env = guard.as_ref().unwrap();
        env.clear().await?;
        env.source(&["CREATE (p:Person {name: 'Alice'})"]).await?;
        env.target(&[
            "CREATE (p:Person {name: 'Alice'})",
            "CREATE (c:Company {name: 'Acme'})",
        ])
        .await?;
        let diffs = env.diff().await?.diffs();
        let has_target_label = diffs
            .iter()
            .any(|d| matches!(d, Diff::TargetNodeLabel { label } if label == "Company"));
        assert!(has_target_label);
        Ok(())
    }

    #[tokio::test]
    async fn test_diff_source_only_label() -> Result<(), Box<dyn Error + Send + Sync>> {
        let guard = get_env().await?;
        let env = guard.as_ref().unwrap();
        env.clear().await?;
        env.source(&[
            "CREATE (p:Person {name: 'Alice'})",
            "CREATE (c:Company {name: 'Acme'})",
        ])
        .await?;
        env.target(&["CREATE (p:Person {name: 'Alice'})"]).await?;
        let diffs = env.diff().await?.diffs();
        let has_source_label = diffs
            .iter()
            .any(|d| matches!(d, Diff::SourceNodeLabel { label } if label == "Company"));
        assert!(has_source_label);
        Ok(())
    }

    #[tokio::test]
    async fn test_diff_include_labels() -> Result<(), Box<dyn Error + Send + Sync>> {
        let guard = get_env().await?;
        let env = guard.as_ref().unwrap();
        env.clear().await?;
        env.source(&[
            "CREATE (p:Person {name: 'Alice'})",
            "CREATE (m:Movie {title: 'Test'})",
        ])
        .await?;
        env.target(&[
            "CREATE (p:Person {name: 'Bob'})",
            "CREATE (m:Movie {title: 'Test'})",
        ])
        .await?;
        let config = DiffConfig::new(vec!["^Movie$".into()], vec![], vec![], vec![], vec![], None)?;
        let summary = env.diff_with(config).await?.summarize().await?;
        assert_eq!(summary.total(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_diff_exclude_labels() -> Result<(), Box<dyn Error + Send + Sync>> {
        let guard = get_env().await?;
        let env = guard.as_ref().unwrap();
        env.clear().await?;
        env.source(&[
            "CREATE (p:Person {name: 'Alice'})",
            "CREATE (m:Movie {title: 'Test'})",
            "CREATE (i:Internal {data: 'secret'})",
        ])
        .await?;
        env.target(&[
            "CREATE (p:Person {name: 'Alice'})",
            "CREATE (m:Movie {title: 'Test'})",
            "CREATE (i:Internal {data: 'different'})",
        ])
        .await?;
        let config = DiffConfig::new(
            vec![],
            vec!["^Internal$".into()],
            vec![],
            vec![],
            vec![],
            None,
        )?;
        let summary = env.diff_with(config).await?.summarize().await?;
        assert_eq!(summary.total(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_diff_exclude_props() -> Result<(), Box<dyn Error + Send + Sync>> {
        let guard = get_env().await?;
        let env = guard.as_ref().unwrap();
        env.clear().await?;
        env.both(&["CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE (p.name) IS UNIQUE"])
            .await?;
        env.source(&["CREATE (p:Person {name: 'Alice', born: 1990, __created: timestamp()})"])
            .await?;
        env.target(&["CREATE (p:Person {name: 'Alice', born: 1990, __created: timestamp()})"])
            .await?;
        let config = DiffConfig::new(vec![], vec![], vec![], vec![], vec!["__.*$".into()], None)?;
        let summary = env.diff_with(config).await?.summarize().await?;
        assert_eq!(summary.total(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_diff_include_rel_types() -> Result<(), Box<dyn Error + Send + Sync>> {
        let guard = get_env().await?;
        let env = guard.as_ref().unwrap();
        env.clear().await?;
        env.source(&[
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})",
            "CREATE (a)-[:LIKES]->(c:Movie {title: 'Test'})",
        ])
        .await?;
        env.target(&[
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})",
            "CREATE (a)-[:LIKES]->(c:Movie {title: 'Different'})",
        ])
        .await?;
        let config = DiffConfig::new(vec![], vec![], vec!["^KNOWS$".into()], vec![], vec![], None)?;
        let diffs = env.diff_with(config).await?.diffs();
        let likes_diffs = diffs.iter().any(|d| {
                matches!(d, Diff::SourceRelationship { relationship_type, .. } | Diff::TargetRelationship { relationship_type, .. } if relationship_type == "LIKES")
            });
        assert!(!likes_diffs);
        Ok(())
    }
}
