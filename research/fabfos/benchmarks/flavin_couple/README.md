# The flavin `member_drift` is a defect in the model couple, not in dGbyG

## Purpose & Contents

`substitutions.tsv` refuses four rows for the dGbyG arm -- `flavin_free`, `flavin_free_2`,
`cpr`, `cpr_2` -- because dGbyG places their model couple 9.60 kJ/mol from the potentials
they cite, past `DIR_DECADE` (5.71). The decision has been carried open for four
generations, and a benchmark scope has now filed against it. This pins what the number
actually is.

    PYTHONPATH=src mamba run -n rdkit-scratch python check_couple.py

## The number does not move with the chemistry

`gap = |(dg - sibling_dg) - predicted_offset|`, and the three flavin couples give:

| couple | anchor | sibling | congeners_dgbyg | gap_dgbyg |
|---|---|---|---|---|
| `flavin_free` / `flavin_free_2` | MNXR173789 / MNXR113076 | MNXR101898 | -10.6595; -20.2619 | 9.602418695068216 |
| `cpr` | MNXR166745 | MNXR173859 | -481.9449; -472.3425 | 9.602418695068934 |
| `cpr_2` | MNXR112059 | MNXR102255 | 492.5331; 492.0873 | 9.602365136718753 |

Free FMN and a protein-bound flavoprotein reductase, different anchors, different
siblings, totals a thousand kJ/mol apart -- and the same offset to five significant
figures. That is the argument that already convicted eQuilibrator's 8.8776 on the acyl
carriers, run against the other member: **a constant that does not vary with the acyl
group is not a property of the thioester bond.** Here it is not a property of the flavin
either.

## Where it comes from: the couple is short one proton

`MODEL:FMN_ox` is `MNXM1364148`, C17H18N4O9P at charge -3 -- the N3-DEPROTONATED imide.
`MODEL:FMN_red` is `MNXM1107623`, C17H21N4O9P at charge -2 -- the NEUTRAL reduced ring.

| couple | ox | red | dH | declared `n_h` | charge check |
|---|---|---|---|---|---|
| FMN | -3, 18 H | -2, 21 H | **3** | 2 | -3 +2H+ +2e- = **-3**, red is **-2** |
| riboflavin | -1, 19 H | 0, 22 H | **3** | 2 | -1 +2H+ +2e- = **-1**, red is **0** |
| NAD | -1, 26 H | -2, 27 H | 1 | 1 | OK |
| NADP | -3, 25 H | -4, 26 H | 1 | 1 | OK |

Both flavin couples transfer three protons where the row declares two, and neither
balances charge. The NAD and NADP couples, written the same way, are correct -- so this
is not a convention this file uses and nobody noticed.

Run over every couple in the table, the correlation is exact:

    couple           ox                     red                     dH  n_h  q_ox  q_red verdict
    cpr              C17H18N4O9P-3          C17H21N4O9P-2            3    2    -3     -2 INCONSISTENT
    cpr_2            C17H18N4O9P-3          C17H21N4O9P-2            3    2    -3     -2 INCONSISTENT
    flavin_free      C17H18N4O9P-3          C17H21N4O9P-2            3    2    -3     -2 INCONSISTENT
    flavin_free_2    C17H18N4O9P-3          C17H21N4O9P-2            3    2    -3     -2 INCONSISTENT
    nadp_generic     C21H26N7O14P2-         C21H27N7O14P2-2          1    1    -1     -2 ok
    trx .. trx_7     C8H15NOS2              C8H17NOS2                2    2    +0     +0 ok

**The four couples that fail `member_drift` are exactly the four that are inconsistent,
and every other couple in the table is clean.** Four of four, no false positives. The
predicate that has been refusing these rows for four generations was measuring something
real; it was just not measuring what its message says.

**Why the choice was made is in the file's own header.** MetaNetX carries FMN twice,
MNXM1364147 at charge 0 and MNXM1364148 at -3, and eQuilibrator's frozen cache knows only
the second. The oxidised model was picked for RESOLVABILITY and its reduced partner for
being the reduced flavin; nothing compared the two to each other.

## Why nothing caught it

`_gate_couple` refuses a couple whose halves "differ by heavy atoms". Hydrogen is not a
heavy atom and formal charge is not an atom at all, so a couple can be off by a proton and
load clean. The congener does not catch it either: riboflavin carries the identical
defect, so the spread across the two is small and reports a confident width.

And `gap_eq` is 0.4458 -- eQuilibrator passes the same inconsistent couple. That is not
eQuilibrator being right; it is eQuilibrator's decomposition being insensitive to the
protonation the structures disagree about.

## Disposition

**The gate is working and the refusal stands for r11.** But the reason recorded against it
was wrong: this is not dGbyG failing to place flavin chemistry, it is the row asserting a
model couple that does not exist. The cost stays what it was -- MNXR157776, MNXR145038 and
MNXR145039 tier 0, and the flavoprotein reactions the four rows exist to unblock.

**The fix is r12 work, and it is not a gate change.** A corrected oxidised flavin model
whose formula and charge are consistent with `MODEL:FMN_red` under a 2e/2H couple, plus a
couple gate that checks hydrogen and charge and not only heavy atoms. Both need
`substitute anchor --member dgbyg` re-run INSIDE the dgbyg image before the rows may be
admitted, because the gap is only meaningful once the structures are.

**CAUTION** -- do not add the charge gate as a refusal before fixing the models. The
flavin rows currently pass for eQuilibrator, so a fatal couple gate would abort the table
load and a non-fatal one would remove eQuilibrator's vote from reactions it presently
covers. That is coverage going down, which is the stop line.
