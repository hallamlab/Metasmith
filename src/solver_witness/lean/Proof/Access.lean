/-
  The leaves. One lemma per non-recursive accessor, and every one of them total.

  ## Why there is no range hypothesis anywhere below

  `src/access.rs` is total by construction: an out-of-range id yields `NONE` or a
  zero count rather than a panic. The specification is total in the same way --
  `nd` and `ep` are `getD` against an empty node, `requiresOf` and `producesOf`
  match on `[t]?` and fall through to `[]`. The two only agree BECAUSE both are
  total, so a lemma stated under `d < nNodes p` would prove the agreement exactly
  where it is uninteresting and leave unproved the case that stops the
  specification being vacuous. Each lemma below therefore pins the return value
  for every id, in range or not.

  ## The recipe

  Every accessor here is a nest of `if in-range then index else default` over
  `alloc.vec.Vec`, so every proof is the same four moves:

      unfold <accessor>; dsimp only        -- `dsimp`: the body opens with a `let`
      split                                -- one `if` per index, outermost first
      rw [index_eq …]                      -- the monadic index becomes `ok`
      simp only [bind_tc_ok, <bridge>]     -- run the bind, rewrite the spec side
      … and the innermost branch closes with one `simp`

  `dsimp only` is what sees through the leading `let`, but it can be used ONLY
  before the first `rw [index_eq …]`. Afterwards the goal carries `↑v[i]`, whose
  coercion is not type-correct once `dsimp` unfolds `alloc.vec.Vec` -- `dsimp`
  then reports "made no progress" rather than the type error underneath. That is
  why every reduction after an index goes through `simp only [bind_tc_ok]`, which
  runs the bind and the remaining `let` without unfolding the `Vec`.

  `index_eq` and the `_lt`/`_ge` bridges below are what make the last step one
  `simp` rather than a page: they say, once, what the checker's `Vec` index and
  the specification's element lookup each come to on either side of the range
  test.

  ## Why the element lemmas read `l[i]?.getD NONE` and not `l.getD i NONE`

  The two are definitionally equal, but `simp` normalises the second into the
  first, so a bridge stated with `List.getD` stops matching the moment anything
  else in the same `simp` call has already normalised the goal. That is not
  hypothetical -- it is why `tr_group_slot`, the only accessor with two nested
  lookups, was the one that would not close while the other fifteen did. Stating
  every lookup in `simp`'s own normal form removes the ordering dependence.
-/

import SolverWitness.Types
import SolverWitness.Funs
import SolverWitness.Spec
import SolverWitness.Proof.Basis

namespace SolverProof

open Aeneas Aeneas.Std Result
open solver_witness

/-! ## Bridges

Not stated in terms of any one accessor: each says something about the *wire
representation* that all sixteen proofs below need. -/

/-- The extraction indexes a `Vec` through `Vec.index`, which is in the `Result`
monad and fails out of range. In range it is `ok` of the element, and that is the
only fact any proof here needs about it. -/
theorem index_eq {α : Type} (v : alloc.vec.Vec α) (i : Std.Usize)
    (h : i.val < v.val.length) :
    alloc.vec.Vec.index (core.slice.index.SliceIndexUsizeSlice α) v i
      = ok (v.val[i.val]'h) := by
  rw [alloc.vec.Vec.index_slice_index]
  exact eq_of_spec (alloc.vec.Vec.index_usize_spec v i (by simpa using h))

theorem nats_length (v : alloc.vec.Vec Std.Usize) :
    (SolverSpec.nats v).length = v.val.length := by
  simp [SolverSpec.nats]

theorem nats_getElem?_lt (v : alloc.vec.Vec Std.Usize) (i : Std.Usize)
    (h : i.val < v.val.length) :
    (SolverSpec.nats v)[i.val]? = some (v.val[i.val]'h).val := by
  simp [SolverSpec.nats, List.getElem?_map, List.getElem?_eq_getElem h]

theorem nats_getElem?_ge (v : alloc.vec.Vec Std.Usize) (i : Std.Usize)
    (h : ¬ i.val < v.val.length) :
    (SolverSpec.nats v)[i.val]? = none := by
  simp [SolverSpec.nats, List.getElem?_map,
        List.getElem?_eq_none (by omega : v.val.length ≤ i.val)]

theorem idLists_length (v : alloc.vec.Vec (alloc.vec.Vec Std.Usize)) :
    (SolverSpec.idLists v).length = v.val.length := by
  simp [SolverSpec.idLists]

theorem idLists_getElem?_lt (v : alloc.vec.Vec (alloc.vec.Vec Std.Usize)) (g : Std.Usize)
    (h : g.val < v.val.length) :
    (SolverSpec.idLists v)[g.val]? = some (SolverSpec.nats (v.val[g.val]'h)) := by
  simp [SolverSpec.idLists, List.getElem?_map, List.getElem?_eq_getElem h]

theorem idLists_getElem?_ge (v : alloc.vec.Vec (alloc.vec.Vec Std.Usize)) (g : Std.Usize)
    (h : ¬ g.val < v.val.length) :
    (SolverSpec.idLists v)[g.val]? = none := by
  simp [SolverSpec.idLists, List.getElem?_map,
        List.getElem?_eq_none (by omega : v.val.length ≤ g.val)]

theorem nd_eq (p : types.Problem) (d : Nat) (h : d < p.nodes.val.length) :
    SolverSpec.nd p d = p.nodes.val[d]'h := by
  simp [SolverSpec.nd, List.getElem?_eq_getElem h]

theorem nd_ne (p : types.Problem) (d : Nat) (h : ¬ d < p.nodes.val.length) :
    SolverSpec.nd p d = SolverSpec.emptyNode := by
  simp [SolverSpec.nd, List.getElem?_eq_none (by omega : p.nodes.val.length ≤ d)]

theorem ep_eq (q : types.Plan) (e : Nat) (h : e < q.endpoints.val.length) :
    SolverSpec.ep q e = q.endpoints.val[e]'h := by
  simp [SolverSpec.ep, List.getElem?_eq_getElem h]

theorem ep_ne (q : types.Plan) (e : Nat) (h : ¬ e < q.endpoints.val.length) :
    SolverSpec.ep q e = SolverSpec.emptyEndpoint := by
  simp [SolverSpec.ep, List.getElem?_eq_none (by omega : q.endpoints.val.length ≤ e)]

theorem requiresOf_eq (p : types.Problem) (t : Nat) (h : t < p.transforms.val.length) :
    SolverSpec.requiresOf p t = SolverSpec.nats (p.transforms.val[t]'h).requires := by
  simp [SolverSpec.requiresOf, List.getElem?_eq_getElem h]

theorem requiresOf_ne (p : types.Problem) (t : Nat) (h : ¬ t < p.transforms.val.length) :
    SolverSpec.requiresOf p t = [] := by
  simp [SolverSpec.requiresOf,
        List.getElem?_eq_none (by omega : p.transforms.val.length ≤ t)]

theorem producesOf_eq (p : types.Problem) (t : Nat) (h : t < p.transforms.val.length) :
    SolverSpec.producesOf p t = SolverSpec.idLists (p.transforms.val[t]'h).produces := by
  simp [SolverSpec.producesOf, List.getElem?_eq_getElem h]

theorem producesOf_ne (p : types.Problem) (t : Nat) (h : ¬ t < p.transforms.val.length) :
    SolverSpec.producesOf p t = [] := by
  simp [SolverSpec.producesOf,
        List.getElem?_eq_none (by omega : p.transforms.val.length ≤ t)]

/-! ## The three table sizes -/

theorem n_nodes_spec (p : types.Problem) :
    access.n_nodes p ⦃ n => n.val = SolverSpec.nNodes p ⦄ := by
  unfold access.n_nodes
  simp [SolverSpec.nNodes]

theorem n_transforms_spec (p : types.Problem) :
    access.n_transforms p ⦃ n => n.val = SolverSpec.nTransforms p ⦄ := by
  unfold access.n_transforms
  simp [SolverSpec.nTransforms]

theorem n_endpoints_spec (q : types.Plan) :
    access.n_endpoints q ⦃ n => n.val = SolverSpec.nEndpoints q ⦄ := by
  unfold access.n_endpoints
  simp [SolverSpec.nEndpoints]

/-! ## Nodes -/

theorem node_nprops_spec (p : types.Problem) (d : Std.Usize) :
    access.node_nprops p d ⦃ n => n.val = (SolverSpec.nodeProps p d.val).length ⦄ := by
  unfold access.node_nprops
  dsimp only
  split
  · next h =>
    have hd : d.val < p.nodes.val.length := by scalar_tac
    rw [index_eq p.nodes d hd]
    simp [SolverSpec.nodeProps, nd_eq p d.val hd, nats_length]
  · next h =>
    have hd : ¬ d.val < p.nodes.val.length := by scalar_tac
    simp [SolverSpec.nodeProps, nd_ne p d.val hd, SolverSpec.emptyNode, nats_length]

theorem node_prop_spec (p : types.Problem) (d i : Std.Usize) :
    access.node_prop p d i
      ⦃ x => x.val = ((SolverSpec.nodeProps p d.val)[i.val]?).getD access.NONE.val ⦄ := by
  unfold access.node_prop
  dsimp only
  split
  · next h =>
    have hd : d.val < p.nodes.val.length := by scalar_tac
    rw [index_eq p.nodes d hd]
    simp only [bind_tc_ok, SolverSpec.nodeProps, nd_eq p d.val hd]
    split
    · next h2 =>
      have hi : i.val < (p.nodes.val[d.val]'hd).props.val.length := by scalar_tac
      rw [index_eq _ i hi]
      simp [nats_getElem?_lt _ i hi]
    · next h2 =>
      have hi : ¬ i.val < (p.nodes.val[d.val]'hd).props.val.length := by scalar_tac
      simp [nats_getElem?_ge _ i hi]
  · next h =>
    have hd : ¬ d.val < p.nodes.val.length := by scalar_tac
    simp [SolverSpec.nodeProps, nd_ne p d.val hd, SolverSpec.emptyNode, SolverSpec.nats]

theorem node_nparents_spec (p : types.Problem) (d : Std.Usize) :
    access.node_nparents p d ⦃ n => n.val = (SolverSpec.nodeParents p d.val).length ⦄ := by
  unfold access.node_nparents
  dsimp only
  split
  · next h =>
    have hd : d.val < p.nodes.val.length := by scalar_tac
    rw [index_eq p.nodes d hd]
    simp [SolverSpec.nodeParents, nd_eq p d.val hd, nats_length]
  · next h =>
    have hd : ¬ d.val < p.nodes.val.length := by scalar_tac
    simp [SolverSpec.nodeParents, nd_ne p d.val hd, SolverSpec.emptyNode, nats_length]

theorem node_parent_spec (p : types.Problem) (d i : Std.Usize) :
    access.node_parent p d i
      ⦃ x => x.val = ((SolverSpec.nodeParents p d.val)[i.val]?).getD access.NONE.val ⦄ := by
  unfold access.node_parent
  dsimp only
  split
  · next h =>
    have hd : d.val < p.nodes.val.length := by scalar_tac
    rw [index_eq p.nodes d hd]
    simp only [bind_tc_ok, SolverSpec.nodeParents, nd_eq p d.val hd]
    split
    · next h2 =>
      have hi : i.val < (p.nodes.val[d.val]'hd).parents.val.length := by scalar_tac
      rw [index_eq _ i hi]
      simp [nats_getElem?_lt _ i hi]
    · next h2 =>
      have hi : ¬ i.val < (p.nodes.val[d.val]'hd).parents.val.length := by scalar_tac
      simp [nats_getElem?_ge _ i hi]
  · next h =>
    have hd : ¬ d.val < p.nodes.val.length := by scalar_tac
    simp [SolverSpec.nodeParents, nd_ne p d.val hd, SolverSpec.emptyNode, SolverSpec.nats]

/-! ## Endpoints -/

theorem ep_nprops_spec (q : types.Plan) (e : Std.Usize) :
    access.ep_nprops q e ⦃ n => n.val = (SolverSpec.epProps q e.val).length ⦄ := by
  unfold access.ep_nprops
  dsimp only
  split
  · next h =>
    have he : e.val < q.endpoints.val.length := by scalar_tac
    rw [index_eq q.endpoints e he]
    simp [SolverSpec.epProps, ep_eq q e.val he, nats_length]
  · next h =>
    have he : ¬ e.val < q.endpoints.val.length := by scalar_tac
    simp [SolverSpec.epProps, ep_ne q e.val he, SolverSpec.emptyEndpoint, nats_length]

theorem ep_prop_spec (q : types.Plan) (e i : Std.Usize) :
    access.ep_prop q e i
      ⦃ x => x.val = ((SolverSpec.epProps q e.val)[i.val]?).getD access.NONE.val ⦄ := by
  unfold access.ep_prop
  dsimp only
  split
  · next h =>
    have he : e.val < q.endpoints.val.length := by scalar_tac
    rw [index_eq q.endpoints e he]
    simp only [bind_tc_ok, SolverSpec.epProps, ep_eq q e.val he]
    split
    · next h2 =>
      have hi : i.val < (q.endpoints.val[e.val]'he).props.val.length := by scalar_tac
      rw [index_eq _ i hi]
      simp [nats_getElem?_lt _ i hi]
    · next h2 =>
      have hi : ¬ i.val < (q.endpoints.val[e.val]'he).props.val.length := by scalar_tac
      simp [nats_getElem?_ge _ i hi]
  · next h =>
    have he : ¬ e.val < q.endpoints.val.length := by scalar_tac
    simp [SolverSpec.epProps, ep_ne q e.val he, SolverSpec.emptyEndpoint, SolverSpec.nats]

theorem ep_nparents_spec (q : types.Plan) (e : Std.Usize) :
    access.ep_nparents q e ⦃ n => n.val = (SolverSpec.epParents q e.val).length ⦄ := by
  unfold access.ep_nparents
  dsimp only
  split
  · next h =>
    have he : e.val < q.endpoints.val.length := by scalar_tac
    rw [index_eq q.endpoints e he]
    simp [SolverSpec.epParents, ep_eq q e.val he, nats_length]
  · next h =>
    have he : ¬ e.val < q.endpoints.val.length := by scalar_tac
    simp [SolverSpec.epParents, ep_ne q e.val he, SolverSpec.emptyEndpoint, nats_length]

theorem ep_parent_spec (q : types.Plan) (e i : Std.Usize) :
    access.ep_parent q e i
      ⦃ x => x.val = ((SolverSpec.epParents q e.val)[i.val]?).getD access.NONE.val ⦄ := by
  unfold access.ep_parent
  dsimp only
  split
  · next h =>
    have he : e.val < q.endpoints.val.length := by scalar_tac
    rw [index_eq q.endpoints e he]
    simp only [bind_tc_ok, SolverSpec.epParents, ep_eq q e.val he]
    split
    · next h2 =>
      have hi : i.val < (q.endpoints.val[e.val]'he).parents.val.length := by scalar_tac
      rw [index_eq _ i hi]
      simp [nats_getElem?_lt _ i hi]
    · next h2 =>
      have hi : ¬ i.val < (q.endpoints.val[e.val]'he).parents.val.length := by scalar_tac
      simp [nats_getElem?_ge _ i hi]
  · next h =>
    have he : ¬ e.val < q.endpoints.val.length := by scalar_tac
    simp [SolverSpec.epParents, ep_ne q e.val he, SolverSpec.emptyEndpoint, SolverSpec.nats]

/-! ## Transforms -/

theorem tr_nrequires_spec (p : types.Problem) (t : Std.Usize) :
    access.tr_nrequires p t ⦃ n => n.val = (SolverSpec.requiresOf p t.val).length ⦄ := by
  unfold access.tr_nrequires
  dsimp only
  split
  · next h =>
    have ht : t.val < p.transforms.val.length := by scalar_tac
    rw [index_eq p.transforms t ht]
    simp [requiresOf_eq p t.val ht, nats_length]
  · next h =>
    have ht : ¬ t.val < p.transforms.val.length := by scalar_tac
    simp [requiresOf_ne p t.val ht]

theorem tr_require_spec (p : types.Problem) (t i : Std.Usize) :
    access.tr_require p t i
      ⦃ x => x.val = ((SolverSpec.requiresOf p t.val)[i.val]?).getD access.NONE.val ⦄ := by
  unfold access.tr_require
  dsimp only
  split
  · next h =>
    have ht : t.val < p.transforms.val.length := by scalar_tac
    rw [index_eq p.transforms t ht]
    simp only [bind_tc_ok, requiresOf_eq p t.val ht]
    split
    · next h2 =>
      have hi : i.val < (p.transforms.val[t.val]'ht).requires.val.length := by scalar_tac
      rw [index_eq _ i hi]
      simp [nats_getElem?_lt _ i hi]
    · next h2 =>
      have hi : ¬ i.val < (p.transforms.val[t.val]'ht).requires.val.length := by scalar_tac
      simp [nats_getElem?_ge _ i hi]
  · next h =>
    have ht : ¬ t.val < p.transforms.val.length := by scalar_tac
    simp [requiresOf_ne p t.val ht]

theorem tr_ngroups_spec (p : types.Problem) (t : Std.Usize) :
    access.tr_ngroups p t ⦃ n => n.val = (SolverSpec.producesOf p t.val).length ⦄ := by
  unfold access.tr_ngroups
  dsimp only
  split
  · next h =>
    have ht : t.val < p.transforms.val.length := by scalar_tac
    rw [index_eq p.transforms t ht]
    simp [producesOf_eq p t.val ht, idLists_length]
  · next h =>
    have ht : ¬ t.val < p.transforms.val.length := by scalar_tac
    simp [producesOf_ne p t.val ht]

theorem tr_ngroup_slots_spec (p : types.Problem) (t g : Std.Usize) :
    access.tr_ngroup_slots p t g
      ⦃ n => n.val = (((SolverSpec.producesOf p t.val)[g.val]?).getD []).length ⦄ := by
  unfold access.tr_ngroup_slots
  dsimp only
  split
  · next h =>
    have ht : t.val < p.transforms.val.length := by scalar_tac
    rw [index_eq p.transforms t ht]
    simp only [bind_tc_ok, producesOf_eq p t.val ht]
    split
    · next h2 =>
      have hg : g.val < (p.transforms.val[t.val]'ht).produces.val.length := by scalar_tac
      rw [index_eq _ g hg]
      simp [idLists_getElem?_lt _ g hg, nats_length]
    · next h2 =>
      have hg : ¬ g.val < (p.transforms.val[t.val]'ht).produces.val.length := by scalar_tac
      simp [idLists_getElem?_ge _ g hg]
  · next h =>
    have ht : ¬ t.val < p.transforms.val.length := by scalar_tac
    simp [producesOf_ne p t.val ht]

theorem tr_group_slot_spec (p : types.Problem) (t g i : Std.Usize) :
    access.tr_group_slot p t g i
      ⦃ x => x.val
          = ((((SolverSpec.producesOf p t.val)[g.val]?).getD [])[i.val]?).getD access.NONE.val ⦄ := by
  unfold access.tr_group_slot
  dsimp only
  split
  · next h =>
    have ht : t.val < p.transforms.val.length := by scalar_tac
    rw [index_eq p.transforms t ht]
    simp only [bind_tc_ok, producesOf_eq p t.val ht]
    split
    · next h2 =>
      have hg : g.val < (p.transforms.val[t.val]'ht).produces.val.length := by scalar_tac
      rw [index_eq _ g hg]
      simp only [bind_tc_ok, idLists_getElem?_lt _ g hg]
      split
      · next h3 =>
        have hi : i.val
            < ((p.transforms.val[t.val]'ht).produces.val[g.val]'hg).val.length := by
          scalar_tac
        rw [index_eq _ i hi]
        simp [nats_getElem?_lt _ i hi]
      · next h3 =>
        have hi : ¬ i.val
            < ((p.transforms.val[t.val]'ht).produces.val[g.val]'hg).val.length := by
          scalar_tac
        simp [nats_getElem?_ge _ i hi]
    · next h2 =>
      have hg : ¬ g.val < (p.transforms.val[t.val]'ht).produces.val.length := by scalar_tac
      simp [idLists_getElem?_ge _ g hg]
  · next h =>
    have ht : ¬ t.val < p.transforms.val.length := by scalar_tac
    simp [producesOf_ne p t.val ht]

/- Every obligation in this module, audited. `--lean-check` reads these lines
   out of the build log: a proof that reaches a `sorry` through the Aeneas
   standard library shows `sorryAx` here and nowhere else. -/
#print axioms SolverProof.index_eq
#print axioms SolverProof.nats_length
#print axioms SolverProof.nats_getElem?_lt
#print axioms SolverProof.nats_getElem?_ge
#print axioms SolverProof.idLists_length
#print axioms SolverProof.idLists_getElem?_lt
#print axioms SolverProof.idLists_getElem?_ge
#print axioms SolverProof.nd_eq
#print axioms SolverProof.nd_ne
#print axioms SolverProof.ep_eq
#print axioms SolverProof.ep_ne
#print axioms SolverProof.requiresOf_eq
#print axioms SolverProof.requiresOf_ne
#print axioms SolverProof.producesOf_eq
#print axioms SolverProof.producesOf_ne
#print axioms SolverProof.n_nodes_spec
#print axioms SolverProof.n_transforms_spec
#print axioms SolverProof.n_endpoints_spec
#print axioms SolverProof.node_nprops_spec
#print axioms SolverProof.node_prop_spec
#print axioms SolverProof.node_nparents_spec
#print axioms SolverProof.node_parent_spec
#print axioms SolverProof.ep_nprops_spec
#print axioms SolverProof.ep_prop_spec
#print axioms SolverProof.ep_nparents_spec
#print axioms SolverProof.ep_parent_spec
#print axioms SolverProof.tr_nrequires_spec
#print axioms SolverProof.tr_require_spec
#print axioms SolverProof.tr_ngroups_spec
#print axioms SolverProof.tr_ngroup_slots_spec
#print axioms SolverProof.tr_group_slot_spec

end SolverProof
