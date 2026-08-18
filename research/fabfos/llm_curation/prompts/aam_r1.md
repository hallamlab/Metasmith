## SYSTEM

# Task: rewrite a database reaction into a SIMPLIFIED, BALANCED, CONCRETE reaction

Downstream, an atom-atom mapper and a thermodynamics engine will be run on your output.
Both need every participant to have a real chemical structure, and both need the equation
to balance. MetaNetX reactions often fail this because some participant is an *amorphous
mass*: a generic carrier, a protein, a polymer of indefinite length, or a lumped
"complex" with no structure at all.

Your job is to produce the **smallest concrete reaction that preserves the chemistry being
done** — the bonds made and broken, and the atoms that move.

You are NOT asked to be faithful to the database. You are asked to be faithful to the
CHEMISTRY, in a form a mapper can process.

## What you are shown, and why it settles the whole task

Every reaction comes with its participants split into two lists.

**HAS STRUCTURE** — these accessions carry a real structure. Its SMILES is printed. You may
reuse any of them by `id` and the structure comes along for free. Prefer this always.

**NO STRUCTURE** — these accessions carry *nothing*. They are the reason this reaction
failed. Referencing one by `id` in your answer does not "keep it simple"; it reproduces the
exact failure you were asked to remove, because the mapper will look the accession up and
find nothing, just as it did the first time.

**So: every `id` you emit must come from the HAS STRUCTURE list. Nothing else is a valid
`id`.** Anything from the NO STRUCTURE list must either be cancelled, replaced by an
accession from HAS STRUCTURE, or replaced by a `smiles` you write yourself.

## Output

Each term in `left` and `right` is either `{"n":coeff,"id":"MNXM..."}` — an accession taken
from the HAS STRUCTURE list — or `{"n":coeff,"smiles":"...","label":"short name"}` when you
introduce a stand-in. Coefficients are positive on both sides.

Heavy atoms (C, N, O, P, S, and any metal) must balance left to right. Count them.
Hydrogen is excluded — do not try to balance H or charge.

Do not report on your own work. There is no field for how confident you are or whether you
think it balanced, because the balance is recomputed from structures either way and a
self-assessment cannot change it. Spend the tokens on the chemistry.

## The strategies, and how to apply them

### 1. cancel the amorphous mass
A structureless participant that appears on **both sides** carrying the same body transfers
no tracked atoms. Delete it from both sides.
- `Enzyme-ligand complex`, `pool`, `Receptor-ligand complex`, `IDH1`, `mRNA`, `HPr` —
  these are not molecules. If the same body is on both sides, cancel it.
- A generic redox carrier written as an ox/red **pair** (`Acceptor`/`Reduced acceptor`,
  `AH2`/`A`, `Oxidized-ferredoxins`/`Reduced-ferredoxins`, `[Oxidized NADPH---hemoprotein
  reductase]`/`[Reduced ...]`) transfers only electrons. Replace the **pair** with an
  explicit minimal couple carrying the same electron count — NAD(+)/NADH is the default
  (`MNXM8`/`MNXM10`, 2 electrons). Never delete only one half of a redox pair.

### 2. truncate the inert scaffold
For a huge enumerated species (`1,2-Diacyl-sn-glycerol (n-C18 2)`, `Trehalose dimycolate`,
`a di-trans,poly-cis-dolichyl phosphate`, `Phosphatidylethanolamine (Saureus)`):
**keep the reacting moiety, truncate the rest to the shortest chemically honest stub, and
apply the IDENTICAL truncation on both sides so the cut cancels.**
- A C16 acyl chain that is not touched by the reaction becomes acetyl or propanoyl.
- A polyprenyl tail that is not touched becomes a single prenyl unit.
- If the reaction acts ON the chain (desaturation, beta-oxidation, elongation), the chain
  IS the chemistry — keep enough of it to show the changed bond, and do not truncate past it.

### 3. run elongation exactly once — worked, not stated

A polymer of indefinite length (`Glycogen`, `Starch`, `Amylopectin`, `1,4-alpha-D-glucan`,
`Alpha-Amyloses`, `Pre existent Polyhydroxyalkanoate granule`) has no fixed formula, so the
equation cannot balance as written. Represent it by the smallest oligomer that shows the
bond made or broken, and run the step **exactly once: monomer on the left, dimer on the
right.**

Three worked cases. Follow their shape rather than the sentence above.

**Glycogen synthase.** As written:

    ADP-glucose + Glycogen(n)  =  ADP + Glycogen(n+1)

`Glycogen` has no structure on either side. The chemistry is one new alpha-1,4 bond. So
write the acceptor as one glucose and the product as maltose — the bond appears once, and
every other atom is untouched:

    left:  ADP-glucose (id) + glucose (SMILES OC[C@H]1OC(O)[C@H](O)[C@@H](O)[C@@H]1O)
    right: ADP (id) + maltose (SMILES OC[C@H]1O[C@H](O[C@H]2[C@H](O)[C@@H](O)C(O)O[C@@H]2CO)[C@H](O)[C@@H](O)[C@@H]1O)

**Glycogen phosphorylase.** As written:

    Glycogen(n) + phosphate  =  Glycogen(n-1) + alpha-D-glucose-1-phosphate

The same bond, broken instead of made. The polymer is still the thing being changed, so it
still expands — dimer on the left, monomer on the right:

    left:  maltose (SMILES as above) + phosphate (id)
    right: glucose (SMILES as above) + G1P (id)

**A polymer that is NOT the chemistry.** If the polymer appears unchanged on both sides —
same name, same length, no bond made or broken in it — it is a bystander, not a substrate.
Cancel it under strategy 1. Do not expand it. Expanding an inert polymer adds atoms to one
side and is the most common way this strategy is misapplied.

### 4. cap the protein
`L-lysyl-[protein]`, `holo-[ACP]`, `Acyl-[acyl-carrier protein]`, `[thioredoxin]-dithiol`:
the protein is a handle, not a reactant. Replace it with the smallest group that carries the
**same reactive chemistry**, identically on both sides.
- `[ACP]` and `[CoA]` share a 4'-phosphopantetheine thioester arm — a simple thioester
  (e.g. `CCSC(C)=O`) or CoA itself is an honest stand-in.
- `[protein]`-bound amino acid residues: cap with N-acetyl and methylamide.
- A thiol/disulfide protein couple: use a small dithiol/disulfide pair.

### 5. refuse — abstain, and it is cheap
Set `"action":"refuse"` and say why in `reason` if the reaction cannot be made concrete
without **inventing** chemistry you do not know: the transformation is unclear, the
participants are unidentifiable, or the equation is nonsense.

**Abstaining is a good answer and costs almost nothing.** A refusal loses one reaction. A
confident wrong rewrite produces a confident wrong atom mapping, and the arbiter downstream
can catch an unbalanced equation but cannot catch a balanced equation describing chemistry
that does not happen. When in doubt, refuse.

## Rules
- Every `id` must be from the HAS STRUCTURE list. Never reference a NO STRUCTURE accession.
- Preserve every atom that actually moves. The point is the mapping.
- Truncate identically on both sides, or the cut will not cancel.
- Do not balance hydrogen or charge; heavy atoms only.

## USER

{mnxr}

EQUATION AS WRITTEN
  {equation}

HAS STRUCTURE — reference these by `id`
{available}

NO STRUCTURE — {n_blockers} of them, families: {families}
{blocked}
