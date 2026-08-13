//! Handing a plan back as indices, with enough of the endpoint graph to rebuild
//! the Python objects the rest of metasmith speaks.
//!
//! The endpoint table is the whole of the difficulty. A step's inputs and
//! outputs are endpoints, an endpoint's lineage is other endpoints, and some of
//! those are the caller's own objects -- the ancestors `inherent_parents` keeps
//! alive through `rectify`. So each row carries `source_node` when it came from
//! the payload, which lets the decoder hand back the object the caller already
//! has instead of an equal copy of it.
//!
//! Rows are emitted parents-first, so the decoder can build them in one pass.

use serde::Serialize;

use crate::det::{self, Map};
use crate::model::EpId;
use crate::problem::Problem;
use crate::search::{ApplId, Arena};

#[derive(Debug, Serialize)]
pub struct EncodedEndpoint {
    pub props: Vec<u32>,
    pub parents: Vec<u32>,
    /// The payload node this endpoint *is*, when it is one of the caller's.
    pub source_node: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct EncodedStep {
    pub transform: u32,
    pub timeline: i64,
    pub used: Vec<(u32, u32)>,
    pub produced: Vec<Vec<(u32, u32)>>,
}

#[derive(Debug, Serialize)]
pub struct SolveReply {
    pub wire_version: u32,
    pub complete: bool,
    pub no_path_possible: bool,
    pub endpoints: Vec<EncodedEndpoint>,
    pub steps: Vec<EncodedStep>,
    /// `[canonical, [equivalent, ...]]`, in the order the merges happened.
    pub merged: Vec<(u32, Vec<u32>)>,
    pub relevant_transforms: Vec<u32>,
    pub iterations: i64,
    pub refiner_iterations: Vec<(i64, i64)>,
}

/// Assigns dense indices to endpoints, parents before children.
pub struct EpTable {
    index: Map<EpId, u32>,
    pub rows: Vec<EncodedEndpoint>,
}

impl EpTable {
    pub fn new() -> Self { Self { index: det::map(), rows: Vec::new() } }

    pub fn add(&mut self, p: &Problem, ar: &Arena, e: EpId, node_of: &Map<EpId, u32>) -> u32 {
        if let Some(&i) = self.index.get(&e) { return i; }
        // Reserve nothing: recurse first, so a parent always has a lower index.
        // The lineage graph is acyclic by construction -- an endpoint's
        // signature is built from its parents' at creation, so a cycle could
        // never have been formed.
        let parents: Vec<u32> = ar
            .eps
            .parents(e)
            .to_vec()
            .into_iter()
            .map(|x| self.add(p, ar, x, node_of))
            .collect();
        if let Some(&i) = self.index.get(&e) { return i; }
        let i = self.rows.len() as u32;
        self.index.insert(e, i);
        self.rows.push(EncodedEndpoint {
            props: p.types.props(ar.eps.ty(e)),
            parents,
            source_node: node_of.get(&e).copied(),
        });
        i
    }
}

pub fn encode_plan(
    p: &Problem, ar: &Arena, steps: &[ApplId], merged: &[(u32, EpId, Vec<EpId>)],
    node_of: &Map<EpId, u32>, complete: bool, iterations: i64,
    refiner_iterations: Vec<(i64, i64)>, wire_version: u32,
) -> SolveReply {
    let mut table = EpTable::new();
    let mut out_steps = Vec::with_capacity(steps.len());
    for &s in steps {
        let a = ar.appl(s);
        out_steps.push(EncodedStep {
            transform: a.transform,
            timeline: a.timeline,
            used: a.used.0.iter().map(|&(d, e)| (d, table.add(p, ar, e, node_of))).collect(),
            produced: a
                .produced
                .iter()
                .map(|g| g.iter().map(|&(d, e)| (d, table.add(p, ar, e, node_of))).collect())
                .collect(),
        });
    }
    let merged = merged
        .iter()
        .map(|(_, canon, eq)| {
            (
                table.add(p, ar, *canon, node_of),
                eq.iter().map(|&x| table.add(p, ar, x, node_of)).collect(),
            )
        })
        .collect();
    SolveReply {
        wire_version,
        complete,
        no_path_possible: p.no_path_possible,
        endpoints: table.rows,
        steps: out_steps,
        merged,
        relevant_transforms: p.relevant_transforms.clone(),
        iterations,
        refiner_iterations,
    }
}
