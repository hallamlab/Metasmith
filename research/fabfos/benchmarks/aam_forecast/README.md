# Can a pre-filter replace a finished run?

`aam_partial` used to run after every mapper because its target set was *reactions that
ended with nothing* — a fact about a run, computed by subtracting six finished member
tables. That cost three sequential mapper passes and nine lanes. `aam_forecast` predicts
the same set from the submission string and from the previous bake's own records, which is
what lets the members run once.

The question this measures is not whether the prediction is *accurate*. It is whether it
is wrong in the cheap direction. Layers are additive and ordered and `aam_layers`' gates
refuse rather than warn, so a reduced submission built for a reaction that maps fine is
never claimed by anything: **a false positive costs mapper time and a false negative costs
coverage.** Recall is the number; precision only prices it.

Reproduce: `PYTHONPATH=src mamba run -n rdkit-scratch python measure_recall.py`

## The threshold is measured, not converted

RXNMapper's transformer accepts 512 tokens. Its tokenizer is a SMILES regex, so tokens and
characters are the same order — but "the same order" is not a threshold, so the number was
taken against the prior run's 44,547 recorded outcomes joined to each reaction's length.

| | n | p10 | median | p90 |
|---|---:|---:|---:|---:|
| returned a map | 42,459 | 72 | **251** | 497 |
| returned nothing | 2,088 | 702 | **1,040** | 3,202 |

The two populations barely overlap, which is what makes a length rule work at all here.

| threshold | recall of the silences | universe offered | precision |
|---:|---:|---:|---:|
| 384 | 98.1% | 25.1% | 18.3% |
| 448 | 97.3% | 17.9% | 25.5% |
| **512** | **97.2%** | **13.2%** | **34.5%** |
| 600 | 95.7% | 8.5% | 52.7% |
| 700 | 90.1% | 5.5% | 76.3% |
| 800 | 79.0% | 4.1% | 89.3% |

512 is where the token limit says the wall is *and* where the curve says it is. Below it
the offer grows about twice as fast as the recall; above 700 the recall falls off a cliff.
The successful population's p90 is 497, so almost nothing that maps today is offered a
reduction it will not use.

## The two halves catch different things, which is why there are two

| recorded silence | n | caught by the string alone |
|---|---:|---:|
| RXNMapper returned nothing | 2,088 | 97.2% |
| Indigo timed out | 457 | 96.5% |
| Indigo errored | 84 | **36.9%** |
| Indigo hung — attempted, never returned | 8 | **50.0%** |

A timeout is a size effect and the string sees it. An **error** is not: Indigo declining a
structure is a property of the chemistry, and two thirds of those reactions are short
enough that no length threshold will ever reach them. Nothing predicts a hang either. That
is the whole case for the empirical half, and it is the case for *keeping* it rather than
tuning the threshold down — pushing the string rule to 256 buys 1.4 points of RXNMapper
recall for four times the offer and still catches none of the errors.

Against the reactions the prior run reached, the empirical half is exact by construction —
it *is* those records. What it adds beyond the string rule is **94 reactions**.

## What the prior run cannot speak for

| | |
|---|---:|
| reactions with a buildable string | 57,593 |
| …with any record from the prior run | 44,580 (77.4%) |
| …with none | **13,013 (22.6%)** |
| offered by the string rule | 7,438 |
| added by the records | 94 |
| offered by both | 7,532 |

The prior run was killed with Indigo two thirds through and its worklist stopped at 20,000
of 83,795, so **a reaction absent from every table was never asked** — not answered
successfully. For that 22.6% only the deterministic rules speak, and the forecast's
`summary.tsv` reports that denominator on every run rather than letting a rate taken over
the recorded half read as a rate over the universe.

This is also why the offer is deliberately generous. If a finished ledger still shows
`mapped_nothing` above a threshold, the answer is a broader offer rule or an explicit
top-up part — not a cleverer predictor.
