- `14:44:53` **solver** score `weighted` -> solved 57/81, steps 1501, iters 3175, moved 0 — **TIE** vs baseline
- `14:47:12` **solver-c** score `weighted` -> solved 57/81, steps 1501, iters 3175, moved 0 — **TIE** vs baseline
- `14:47:13` **solver-a** score `weighted` -> solved 57/81, steps 1501, iters 3175, moved 0 — **TIE** vs baseline
- `14:47:13` **solver-b** score `weighted` -> solved 57/81, steps 1501, iters 3175, moved 0 — **TIE** vs baseline
- `14:50:50` **solver-a** score `puct` -> solved 76/81, steps 1486, iters 2188, moved 74 — **WIN** vs baseline
- `14:51:08` **solver-a** score `puct` -> solved 76/81, steps 1486, iters 2188, moved 74 — **WIN** vs baseline

### SUBMIT `puct-base` — solver-a @ 7c8b2578
PUCT in both phases: priors from normalised score channels, per-transform statistics, progress-delta reward

solved **76/81** (baseline 57), steps **1486** (baseline 1501), iters **2188** (baseline 3175) — **WIN**
policy=`puct` puct=`None`

- `14:52:57` **solver-a** score `puct` -> solved 76/81, steps 1486, iters 2188, moved 0 — **WIN** vs baseline, **TIE** vs head
- `14:53:26` **solver-a** score `puct` `c_puct=0.25` -> solved 74/81, steps 1486, iters 2175, moved 31 — **WIN** vs baseline, **LOSS** vs head
- `14:53:32` **solver-b** score `puct` `use_value=false` -> solved 76/81, steps 1502, iters 2338, moved 41 — **WIN** vs baseline, **LOSS** vs head
- `14:53:37` **solver-a** score `puct` `c_puct=0.5` -> solved 75/81, steps 1486, iters 2189, moved 29 — **WIN** vs baseline, **LOSS** vs head
- `14:53:47` **solver-a** score `puct` `c_puct=1.0` -> solved 76/81, steps 1489, iters 2225, moved 24 — **WIN** vs baseline, **LOSS** vs head
- `14:54:01` **solver-a** score `puct` `c_puct=2.0` -> solved 77/81, steps 1489, iters 2256, moved 20 — **WIN** vs baseline, **WIN** vs head
- `14:54:12` **solver-a** score `puct` `c_puct=2.5` -> solved 77/81, steps 1488, iters 2220, moved 32 — **WIN** vs baseline, **WIN** vs head
- `14:54:24` **solver-a** score `puct` `c_puct=4.0` -> solved 77/81, steps 1504, iters 2392, moved 37 — **WIN** vs baseline, **WIN** vs head
- `14:54:28` **solver-b** score `puct` `use_value=false` -> solved 76/81, steps 1502, iters 2338, moved 41 — **WIN** vs baseline, **LOSS** vs head

### SUBMIT `no-value` — solver-b @ 2d2ef3ec
use_value=false loses, and it loses only above ~33 targets: identical steps and iters on ladder-3..32, +9/+23/+30/+47/+36 iters and +5/+6 steps on ladder-33..68. The estimator is inert on small problems (report 02) and load-bearing on large ones (report 04)

solved **76/81** (baseline 57), steps **1502** (baseline 1501), iters **2338** (baseline 3175) — **WIN**
policy=`puct` puct=`use_value=false`

- `14:54:35` **solver-a** score `puct` `c_puct=8.0` -> solved 77/81, steps 1496, iters 2418, moved 43 — **WIN** vs baseline, **WIN** vs head
- `14:54:48` **solver-c** score `puct` -> solved 76/81, steps 1486, iters 2188, moved 0 — **WIN** vs baseline, **TIE** vs head
- `14:54:51` **solver-a** score `puct` `c_puct=1.75` -> solved 77/81, steps 1502, iters 2238, moved 13 — **WIN** vs baseline, **WIN** vs head
- `14:55:02` **solver-a** score `puct` `c_puct=2.25` -> solved 77/81, steps 1496, iters 2248, moved 30 — **WIN** vs baseline, **WIN** vs head
- `14:55:08` **solver-b** score `puct` `decay=0.995` -> solved 76/81, steps 1486, iters 2188, moved 1 — **WIN** vs baseline, **WIN** vs head
- `14:55:14` **solver-a** score `puct` `c_puct=3.0` -> solved 77/81, steps 1492, iters 2284, moved 33 — **WIN** vs baseline, **WIN** vs head
- `14:55:19` **solver-b** score `puct` `decay=0.99` -> solved 77/81, steps 1486, iters 2186, moved 4 — **WIN** vs baseline, **WIN** vs head
- `14:55:27` **solver-a** score `puct` `c_puct=3.5` -> solved 77/81, steps 1495, iters 2313, moved 37 — **WIN** vs baseline, **WIN** vs head
- `14:55:32` **solver-b** score `puct` `decay=0.97` -> solved 77/81, steps 1489, iters 2193, moved 12 — **WIN** vs baseline, **WIN** vs head
- `14:55:39` **solver-a** score `puct` `c_puct=16.0` -> solved 76/81, steps 1504, iters 2391, moved 40 — **WIN** vs baseline, **LOSS** vs head
- `14:55:43` **solver-b** score `puct` `decay=0.95` -> solved 76/81, steps 1489, iters 2189, moved 12 — **WIN** vs baseline, **LOSS** vs head
- `14:55:54` **solver-b** score `puct` `decay=0.9` -> solved 75/81, steps 1489, iters 2183, moved 17 — **WIN** vs baseline, **LOSS** vs head
- `14:55:57` **solver-a** score `puct` `temperature=0.1` -> solved 73/81, steps 1500, iters 2576, moved 50 — **WIN** vs baseline, **LOSS** vs head
- `14:56:08` **solver-a** score `puct` `temperature=0.25` -> solved 74/81, steps 1474, iters 2260, moved 39 — **WIN** vs baseline, **LOSS** vs head
- `14:56:12` **solver-b** score `puct` `decay=0.993` -> solved 76/81, steps 1486, iters 2188, moved 1 — **WIN** vs baseline, **WIN** vs head
- `14:56:19` **solver-a** score `puct` `temperature=1.0` -> solved 77/81, steps 1503, iters 2399, moved 38 — **WIN** vs baseline, **WIN** vs head
- `14:56:21` **solver-c** score `puct` `dup=1.0` -> solved 72/81, steps 1449, iters 2384, moved 41 — **WIN** vs baseline, **LOSS** vs head
- `14:56:24` **solver-b** score `puct` `decay=0.99` -> solved 77/81, steps 1486, iters 2186, moved 4 — **WIN** vs baseline, **WIN** vs head
- `14:56:31` **solver-a** score `puct` `temperature=2.0` -> solved 75/81, steps 1502, iters 2409, moved 40 — **WIN** vs baseline, **LOSS** vs head
- `14:56:35` **solver-b** score `puct` `decay=0.985` -> solved 77/81, steps 1486, iters 2183, moved 5 — **WIN** vs baseline, **WIN** vs head
- `14:56:36` **solver-c** score `puct` `dup=0.25` -> solved 75/81, steps 1486, iters 2243, moved 22 — **WIN** vs baseline, **LOSS** vs head

### SUPERVISOR — the PUCT head validated end to end (round 1, mid-flight)

Not the harness's own gate; the real Python planning path with `MSM_SOLVER_POLICY=puct` set,
adjudicated by `research/metasmith/witness_sweep/run_sweep.py`.

| arm | cases | accepted (weighted / puct) | decoys caught / missed | steps w -> p |
|---|---|---|---|---|
| templates | 11 | 11 / 11 | 109 / 0 | 159 -> 156 |
| fabfos | 3 | 3 / 3 | 29 / 0 | 23 -> 23 |
| aspire | 7 | 7 / 7 | 70 / 0 | 183 -> 183 |

0 reference disagreements on every arm. `pytest tests/metasmith/solver` at the head: 252 passed,
224 skipped, 1 xfailed.

So PUCT is sound on the real workflows, not only on the ratchet corpus, and it shortens the
eleven shipped templates by 3 steps. Keep going.

- `14:56:43` **solver-a** score `puct` `temperature=0.05` -> solved 73/81, steps 1500, iters 2833, moved 51 — **WIN** vs baseline, **LOSS** vs head
- `14:56:47` **solver-b** score `puct` `decay=0.98` -> solved 76/81, steps 1489, iters 2174, moved 10 — **WIN** vs baseline, **LOSS** vs head
- `14:56:48` **solver-c** score `puct` `dup=0.5` -> solved 75/81, steps 1490, iters 2399, moved 35 — **WIN** vs baseline, **LOSS** vs head
- `14:57:00` **solver-c** score `puct` `dup=0.75` -> solved 73/81, steps 1469, iters 2445, moved 40 — **WIN** vs baseline, **LOSS** vs head
- `14:57:00` **solver-a** score `puct` `temperature=0.75` -> solved 77/81, steps 1497, iters 2356, moved 32 — **WIN** vs baseline, **WIN** vs head
- `14:57:04` **solver-b** score `puct` `decay=0.988` -> solved 77/81, steps 1486, iters 2186, moved 4 — **WIN** vs baseline, **WIN** vs head
- `14:57:11` **solver-a** score `puct` `temperature=1.5` -> solved 76/81, steps 1504, iters 2399, moved 39 — **WIN** vs baseline, **LOSS** vs head
- `14:57:15` **solver-b** score `puct` `decay=0.983` -> solved 77/81, steps 1489, iters 2174, moved 11 — **WIN** vs baseline, **WIN** vs head
- `14:57:25` **solver-a** score `puct` `fpu=0.0` -> solved 75/81, steps 1516, iters 2453, moved 39 — **WIN** vs baseline, **LOSS** vs head
- `14:57:27` **solver-b** score `puct` `decay=0.982` -> solved 77/81, steps 1489, iters 2174, moved 11 — **WIN** vs baseline, **WIN** vs head
- `14:57:36` **solver-a** score `puct` `fpu=0.25` -> solved 77/81, steps 1486, iters 2400, moved 36 — **WIN** vs baseline, **WIN** vs head
- `14:57:43` **solver-b** score `puct` `decay=0.985` -> solved 77/81, steps 1486, iters 2183, moved 5 — **WIN** vs baseline, **WIN** vs head

### SUBMIT `puct-decay` — solver-b @ 2d2ef3ec
decay=0.985 solves 77/81 at steps 1486, iters 2183; a recency-weighted Q wins across a plateau 0.982-0.99 (all 77) and falls back to 76 at 0.98 and below, so the estimator's value is in the recent evidence, not the running mean

solved **77/81** (baseline 57), steps **1486** (baseline 1501), iters **2183** (baseline 3175) — **WIN**
policy=`puct` puct=`decay=0.985`

- `14:57:47` **solver-a** score `puct` `fpu=1.0` -> solved 76/81, steps 1486, iters 2188, moved 25 — **WIN** vs baseline, **LOSS** vs head
- `14:58:03` **solver-c** score `puct` `dup=1.0` -> solved 75/81, steps 1491, iters 2589, moved 40 — **WIN** vs baseline, **LOSS** vs head
- `14:58:04` **solver-a** score `puct` `fpu=0.15` -> solved 77/81, steps 1515, iters 2444, moved 38 — **WIN** vs baseline, **WIN** vs head
- `14:58:15` **solver-c** score `puct` `dup=2.0` -> solved 72/81, steps 1458, iters 2469, moved 42 — **WIN** vs baseline, **LOSS** vs head
- `14:58:15` **solver-a** score `puct` `fpu=0.35` -> solved 76/81, steps 1492, iters 2299, moved 24 — **WIN** vs baseline, **LOSS** vs head
- `14:58:21` **solver** score `puct` `c_puct=2.0` -> solved 77/81, steps 1489, iters 2256, moved 20 — **WIN** vs baseline, **WIN** vs head
- `14:58:26` **solver-c** score `puct` `dup=3.0` -> solved 70/81, steps 1376, iters 2236, moved 46 — **WIN** vs baseline, **LOSS** vs head
- `14:58:27` **solver-a** score `puct` `decay=0.99` -> solved 77/81, steps 1486, iters 2186, moved 4 — **WIN** vs baseline, **WIN** vs head
- `14:58:33` **solver** score `puct` `decay=0.985` -> solved 77/81, steps 1486, iters 2183, moved 5 — **WIN** vs baseline, **WIN** vs head
- `14:58:39` **solver-a** score `puct` `decay=0.9` -> solved 75/81, steps 1489, iters 2183, moved 17 — **WIN** vs baseline, **LOSS** vs head
- `14:58:44` **solver** score `puct` `c_puct=2.0,decay=0.985` -> solved 78/81, steps 1492, iters 2252, moved 23 — **WIN** vs baseline, **WIN** vs head
- `14:58:50` **solver-a** score `puct` `decay=0.7` -> solved 73/81, steps 1493, iters 2157, moved 20 — **WIN** vs baseline, **LOSS** vs head
- `14:58:55` **solver** score `puct` `c_puct=2.5,decay=0.985` -> solved 78/81, steps 1493, iters 2226, moved 35 — **WIN** vs baseline, **WIN** vs head
- `14:59:05` **solver-a** score `puct` `decay=0.995` -> solved 76/81, steps 1486, iters 2188, moved 1 — **WIN** vs baseline, **WIN** vs head
- `14:59:08` **solver** score `puct` `c_puct=2.0,decay=0.988` -> solved 78/81, steps 1492, iters 2252, moved 23 — **WIN** vs baseline, **WIN** vs head
- `14:59:18` **solver-a** score `puct` `decay=0.98` -> solved 76/81, steps 1489, iters 2174, moved 10 — **WIN** vs baseline, **LOSS** vs head

### SUPERVISOR — arm A's and arm B's wins STACK (round 1 adjudication, provisional)

Scored in the supervisor worktree, same corpus, same gates:

| config | solved | steps | iters |
|---|---|---|---|
| (head default) | 76/81 | 1486 | 2188 |
| `c_puct=2.0` (arm A) | 77/81 | 1489 | 2256 |
| `decay=0.985` (arm B) | 77/81 | 1486 | 2183 |
| **`c_puct=2.0,decay=0.985`** | **78/81** | 1492 | 2252 |
| `c_puct=2.5,decay=0.985` | 78/81 | 1493 | 2226 |

Two independently-found knobs, +1 each alone, +2 together. Exploration weight and Q recency are
doing different jobs and neither subsumes the other.

Note the tension: every arrangement that reaches 78 pays for it in steps (1486 -> 1492). Solved is
the first ranking key so 78 wins, but **plan length is now the open axis** — the corpus says nothing
above 77 is free. If you are looking for the next thing to try, it is steps at fixed solve count.

- `14:59:28` **solver-a** score `puct` `decay=0.95` -> solved 76/81, steps 1489, iters 2189, moved 12 — **WIN** vs baseline, **LOSS** vs head
- `14:59:40` **solver-a** score `puct` `top_k=2` -> solved 78/81, steps 1524, iters 2776, moved 74 — **WIN** vs baseline, **WIN** vs head
- `14:59:40` **solver-c** score `puct` `cap=1` -> solved 76/81, steps 1486, iters 2188, moved 2 — **WIN** vs baseline, **WIN** vs head
- `14:59:51` **solver-a** score `puct` `top_k=3` -> solved 78/81, steps 1550, iters 2994, moved 79 — **WIN** vs baseline, **WIN** vs head
- `14:59:51` **solver-c** score `puct` `cap=2` -> solved 76/81, steps 1486, iters 2188, moved 2 — **WIN** vs baseline, **WIN** vs head
- `15:00:02` **solver-c** score `puct` `cap=4` -> solved 76/81, steps 1486, iters 2188, moved 0 — **WIN** vs baseline, **TIE** vs head
- `15:00:07` **solver-a** score `puct` `top_k=4` -> solved 77/81, steps 1557, iters 3843, moved 75 — **WIN** vs baseline, **WIN** vs head
- `15:00:13` **solver-c** score `puct` `cap=8` -> solved 76/81, steps 1486, iters 2188, moved 0 — **WIN** vs baseline, **TIE** vs head
- `15:00:18` **solver-a** score `puct` `top_k=5` -> solved 78/81, steps 1547, iters 3840, moved 75 — **WIN** vs baseline, **WIN** vs head
- `15:00:29` **solver-a** score `puct` `top_k=8` -> solved 76/81, steps 1566, iters 4583, moved 76 — **WIN** vs baseline, **LOSS** vs head
- `15:00:29` **solver-c** score `puct` `cap=2` -> solved 76/81, steps 1486, iters 2188, moved 2 — **WIN** vs baseline, **WIN** vs head
- `15:00:40` **solver-a** score `puct` `epsilon_milli=25` -> solved 77/81, steps 1495, iters 2497, moved 42 — **WIN** vs baseline, **WIN** vs head
- `15:00:45` **solver-c** score `puct` `cap=1` -> solved 76/81, steps 1486, iters 2188, moved 2 — **WIN** vs baseline, **WIN** vs head
- `15:00:50` **solver-a** score `puct` `epsilon_milli=100` -> solved 78/81, steps 1506, iters 2311, moved 63 — **WIN** vs baseline, **WIN** vs head
- `15:00:56` **solver-c** score `puct` `cap=3` -> solved 76/81, steps 1486, iters 2188, moved 0 — **WIN** vs baseline, **TIE** vs head
- `15:01:05` **solver-a** score `puct` `epsilon_milli=50` -> solved 78/81, steps 1498, iters 2541, moved 49 — **WIN** vs baseline, **WIN** vs head
- `15:01:09` **solver-b** score `puct` `reward_scale=10` -> solved 72/81, steps 1465, iters 2275, moved 38 — **WIN** vs baseline, **LOSS** vs head
- `15:01:15` **solver-c** score `puct` `cap=1,dup=1.0` -> solved 75/81, steps 1491, iters 2589, moved 40 — **WIN** vs baseline, **LOSS** vs head
- `15:01:16` **solver-a** score `puct` `epsilon_milli=150` -> solved 78/81, steps 1502, iters 2641, moved 73 — **WIN** vs baseline, **WIN** vs head
- `15:01:20` **solver-b** score `puct` `reward_scale=69` -> solved 71/81, steps 1473, iters 2377, moved 39 — **WIN** vs baseline, **LOSS** vs head
- `15:01:28` **solver-c** score `puct` `cap=1,top_k=3` -> solved 77/81, steps 1530, iters 3165, moved 78 — **WIN** vs baseline, **WIN** vs head
- `15:01:29` **solver-a** score `puct` `epsilon_milli=200` -> solved 79/81, steps 1509, iters 2676, moved 73 — **WIN** vs baseline, **WIN** vs head
- `15:01:33` **solver-b** score `puct` `reward_scale=5` -> solved 73/81, steps 1478, iters 2154, moved 31 — **WIN** vs baseline, **LOSS** vs head
- `15:01:39` **solver-c** score `puct` `cap=1,dup_lin=1.0` -> solved 72/81, steps 1449, iters 2384, moved 41 — **WIN** vs baseline, **LOSS** vs head
- `15:01:40` **solver-a** score `puct` `epsilon_milli=300` -> solved 78/81, steps 1504, iters 2477, moved 72 — **WIN** vs baseline, **WIN** vs head
- `15:01:51` **solver-c** score `puct` `top_k=3` -> solved 78/81, steps 1550, iters 2994, moved 79 — **WIN** vs baseline, **WIN** vs head
- `15:01:52` **solver-a** score `puct` `epsilon_milli=500` -> solved 77/81, steps 1495, iters 2710, moved 74 — **WIN** vs baseline, **WIN** vs head
- `15:02:06` **solver-a** score `puct` `epsilon_milli=175` -> solved 78/81, steps 1507, iters 2604, moved 74 — **WIN** vs baseline, **WIN** vs head
- `15:02:07` **solver-c** score `puct` `top_k=2` -> solved 78/81, steps 1524, iters 2776, moved 74 — **WIN** vs baseline, **WIN** vs head
- `15:02:17` **solver-a** score `puct` `epsilon_milli=225` -> solved 78/81, steps 1508, iters 2702, moved 72 — **WIN** vs baseline, **WIN** vs head
- `15:02:18` **solver-c** score `puct` `top_k=4` -> solved 77/81, steps 1557, iters 3843, moved 75 — **WIN** vs baseline, **WIN** vs head
- `15:02:28` **solver-a** score `puct` `epsilon_milli=250` -> solved 78/81, steps 1516, iters 2690, moved 72 — **WIN** vs baseline, **WIN** vs head
- `15:02:30` **solver-c** score `puct` `top_k=5` -> solved 78/81, steps 1547, iters 3840, moved 75 — **WIN** vs baseline, **WIN** vs head
- `15:02:39` **solver-a** score `puct` `epsilon_milli=190` -> solved 79/81, steps 1509, iters 2676, moved 73 — **WIN** vs baseline, **WIN** vs head
- `15:02:41` **solver-c** score `puct` `top_k=3,cap=2` -> solved 78/81, steps 1546, iters 3019, moved 79 — **WIN** vs baseline, **WIN** vs head
- `15:02:52` **solver-a** score `puct` `epsilon_milli=210` -> solved 78/81, steps 1508, iters 2655, moved 72 — **WIN** vs baseline, **WIN** vs head
- `15:03:04` **solver-c** score `puct` `epsilon_milli=200` -> solved 79/81, steps 1509, iters 2676, moved 73 — **WIN** vs baseline, **WIN** vs head
- `15:03:12` **solver-a** score `puct` `epsilon_milli=200` -> solved 79/81, steps 1509, iters 2676, moved 73 — **WIN** vs baseline, **WIN** vs head

### SUBMIT `puct-eps200` — solver-a @ 2d2ef3ec
epsilon_milli=200: a 20% uniform-draw arm on top of PUCT solves 79/81

solved **79/81** (baseline 57), steps **1509** (baseline 1501), iters **2676** (baseline 3175) — **WIN**
policy=`puct` puct=`epsilon_milli=200`

- `15:03:15` **solver-c** score `puct` `epsilon_milli=200,cap=1` -> solved 79/81, steps 1509, iters 2676, moved 73 — **WIN** vs baseline, **WIN** vs head
- `15:03:17` **solver-b** score `puct` `reward_scale=0` -> solved 76/81, steps 1499, iters 2187, moved 23 — **WIN** vs baseline, **LOSS** vs head
- `15:03:27` **solver-c** score `puct` `epsilon_milli=200,cap=4` -> solved 79/81, steps 1509, iters 2676, moved 73 — **WIN** vs baseline, **WIN** vs head
- `15:03:29` **solver-b** score `puct` `reward_scale=0.5` -> solved 76/81, steps 1498, iters 2222, moved 21 — **WIN** vs baseline, **LOSS** vs head
- `15:03:33` **solver-a** score `puct` `epsilon_milli=200,c_puct=2.5` -> solved 77/81, steps 1505, iters 2694, moved 69 — **WIN** vs baseline, **WIN** vs head
- `15:03:40` **solver-b** score `puct` `reward_scale=0,decay=0.985` -> solved 77/81, steps 1499, iters 2187, moved 24 — **WIN** vs baseline, **WIN** vs head
- `15:03:44` **solver-a** score `puct` `epsilon_milli=200,temperature=1.0` -> solved 76/81, steps 1495, iters 2757, moved 72 — **WIN** vs baseline, **WIN** vs head
- `15:03:46` **solver** score `puct` `epsilon_milli=200,decay=0.985` -> solved 78/81, steps 1508, iters 2668, moved 72 — **WIN** vs baseline, **WIN** vs head
- `15:03:53` **solver-b** score `puct` `fpu=0.7,decay=0.985` -> solved 78/81, steps 1486, iters 2197, moved 21 — **WIN** vs baseline, **WIN** vs head
- `15:03:57` **solver-a** score `puct` `epsilon_milli=200,fpu=0.25` -> solved 78/81, steps 1496, iters 2664, moved 71 — **WIN** vs baseline, **WIN** vs head
- `15:03:58` **solver** score `puct` `epsilon_milli=200,c_puct=2.0,decay=0.985` -> solved 79/81, steps 1517, iters 2674, moved 71 — **WIN** vs baseline, **WIN** vs head
- `15:03:58` **solver-c** score `puct` `cap=1` -> solved 76/81, steps 1486, iters 2188, moved 2 — **WIN** vs baseline, **WIN** vs head

### SUBMIT `frontier-cap` — solver-c @ 80eeb93f
Progressive widening by transform: only the first cap frontier slots of each key are eligible. Cuts 3 steps on two payloads, loses nothing; subsumed by epsilon_milli.

solved **76/81** (baseline 57), steps **1486** (baseline 1501), iters **2188** (baseline 3175) — **WIN**
policy=`puct` puct=`cap=1`

- `15:04:04` **solver-b** score `puct` `fpu=0.3,decay=0.985` -> solved 78/81, steps 1494, iters 2217, moved 33 — **WIN** vs baseline, **WIN** vs head
- `15:04:09` **solver-a** score `puct` `epsilon_milli=200,decay=0.99` -> solved 79/81, steps 1508, iters 2668, moved 73 — **WIN** vs baseline, **WIN** vs head
- `15:04:10` **solver** score `puct` `epsilon_milli=150,decay=0.985` -> solved 78/81, steps 1502, iters 2661, moved 72 — **WIN** vs baseline, **WIN** vs head
- `15:04:14` **solver-c** score `puct` `dup_lin=0.75` -> solved 73/81, steps 1469, iters 2445, moved 40 — **WIN** vs baseline, **LOSS** vs head

### SUBMIT `dup-split-negative` — solver-c @ 80eeb93f
NEGATIVE: the 5 unsolved are a self-feeding-transform frontier explosion (714 of 818 slots are one transform on sink26-103); splitting the prior across duplicates reaches one of them and loses four. Concentration is the wrong lever - randomisation is.

solved **73/81** (baseline 57), steps **1469** (baseline 1501), iters **2445** (baseline 3175) — **WIN**
policy=`puct` puct=`dup_lin=0.75`

- `15:04:17` **solver-b** score `puct` `fpu=1.0,decay=0.985` -> solved 78/81, steps 1486, iters 2188, moved 28 — **WIN** vs baseline, **WIN** vs head
- `15:04:21` **solver-a** score `puct` `epsilon_milli=200,top_k=2` -> solved 78/81, steps 1535, iters 3096, moved 73 — **WIN** vs baseline, **WIN** vs head
- `15:04:22` **solver** score `puct` `epsilon_milli=100,c_puct=2.0,decay=0.985` -> solved 78/81, steps 1504, iters 2320, moved 65 — **WIN** vs baseline, **WIN** vs head
- `15:04:33` **solver** score `puct` `epsilon_milli=200,c_puct=2.5,decay=0.988` -> solved 78/81, steps 1506, iters 2682, moved 70 — **WIN** vs baseline, **WIN** vs head
- `15:04:35` **solver-b** score `puct` `fpu=1.0` -> solved 76/81, steps 1486, iters 2188, moved 25 — **WIN** vs baseline, **LOSS** vs head
- `15:04:38` **solver-a** score `puct` `epsilon_milli=200,c_puct=1.0` -> solved 78/81, steps 1500, iters 2583, moved 72 — **WIN** vs baseline, **WIN** vs head
- `15:04:46` **solver-b** score `puct` `fpu=1.5,decay=0.985` -> solved 78/81, steps 1486, iters 2184, moved 30 — **WIN** vs baseline, **WIN** vs head
- `15:04:49` **solver-a** score `puct` `epsilon_milli=200,c_puct=0.5` -> solved 78/81, steps 1503, iters 2541, moved 71 — **WIN** vs baseline, **WIN** vs head
- `15:04:57` **solver-b** score `puct` `fpu=2.0,decay=0.985` -> solved 78/81, steps 1486, iters 2184, moved 30 — **WIN** vs baseline, **WIN** vs head
- `15:05:00` **solver-a** score `puct` `epsilon_milli=200,temperature=0.35` -> solved 79/81, steps 1516, iters 2558, moved 74 — **WIN** vs baseline, **WIN** vs head
- `15:05:08` **solver-b** score `puct` `fpu=1.2,decay=0.985` -> solved 78/81, steps 1486, iters 2184, moved 29 — **WIN** vs baseline, **WIN** vs head
- `15:05:11` **solver-a** score `puct` `epsilon_milli=200,temperature=0.25` -> solved 79/81, steps 1517, iters 2511, moved 75 — **WIN** vs baseline, **WIN** vs head
- `15:05:19` **solver-b** score `puct` `fpu=1.0,decay=0.99` -> solved 77/81, steps 1486, iters 2185, moved 28 — **WIN** vs baseline, **WIN** vs head
- `15:05:24` **solver-a** score `puct` `epsilon_milli=200,decay=0.99,fpu=0.25` -> solved 77/81, steps 1505, iters 2667, moved 72 — **WIN** vs baseline, **WIN** vs head
- `15:05:38` **solver-b** score `puct` `fpu=1.5,decay=0.985` -> solved 78/81, steps 1486, iters 2184, moved 30 — **WIN** vs baseline, **WIN** vs head

### SUBMIT `puct-novelty` — solver-b @ 47cb875b
fpu=1.5,decay=0.985 solves 78/81 at steps 1486, iters 2184: the value head earns its cost as first-play urgency, not as a ranking -- raising the unobserved-key value above every attainable reward and decaying stale evidence both widen the novelty gap, while scaling the reward up to a real ranking (reward_scale=5/10/69) costs 3-5 solves

solved **78/81** (baseline 57), steps **1486** (baseline 1501), iters **2184** (baseline 3175) — **WIN**
policy=`puct` puct=`fpu=1.5,decay=0.985`

- `15:05:44` **solver-a** score `puct` `w0=1.0,w1=0.0` -> solved 78/81, steps 1493, iters 2085, moved 49 — **WIN** vs baseline, **WIN** vs head
- `15:05:55` **solver-a** score `puct` `w0=0.0,w1=1.0` -> solved 70/81, steps 1396, iters 3466, moved 76 — **WIN** vs baseline, **LOSS** vs head
- `15:06:00` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=0` -> solved 78/81, steps 1498, iters 2192, moved 36 — **WIN** vs baseline, **WIN** vs head
- `15:06:06` **solver-a** score `puct` `w0=0.5,w1=0.5` -> solved 78/81, steps 1479, iters 3009, moved 74 — **WIN** vs baseline, **WIN** vs head
- `15:06:11` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2` -> solved 78/81, steps 1483, iters 2206, moved 32 — **WIN** vs baseline, **WIN** vs head
- `15:06:18` **solver-a** score `puct` `w0=1.5,w1=0.4` -> solved 75/81, steps 1519, iters 2484, moved 38 — **WIN** vs baseline, **LOSS** vs head
- `15:06:22` **solver-b** score `puct` `fpu=1.5,decay=0.985,c_puct=2.5` -> solved 78/81, steps 1491, iters 2207, moved 36 — **WIN** vs baseline, **WIN** vs head
- `15:06:29` **solver-a** score `puct` `w0=0.4,w1=0.1` -> solved 77/81, steps 1497, iters 2427, moved 40 — **WIN** vs baseline, **WIN** vs head
- `15:06:33` **solver-b** score `puct` `fpu=1.5,decay=0.985,c_puct=1.0` -> solved 78/81, steps 1489, iters 2211, moved 35 — **WIN** vs baseline, **WIN** vs head
- `15:06:45` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200` -> solved 79/81, steps 1498, iters 2256, moved 73 — **WIN** vs baseline, **WIN** vs head
- `15:06:54` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=1.5` -> solved 78/81, steps 1489, iters 2210, moved 34 — **WIN** vs baseline, **WIN** vs head
- `15:06:56` **solver-a** score `puct` `w0=1.0,w1=0.1` -> solved 75/81, steps 1486, iters 2180, moved 32 — **WIN** vs baseline, **LOSS** vs head
- `15:07:05` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2.5` -> solved 78/81, steps 1492, iters 2179, moved 35 — **WIN** vs baseline, **WIN** vs head
- `15:07:08` **solver-a** score `puct` `w0=1.0,w1=0.4` -> solved 78/81, steps 1498, iters 2490, moved 43 — **WIN** vs baseline, **WIN** vs head
- `15:07:17` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=3` -> solved 78/81, steps 1486, iters 2190, moved 34 — **WIN** vs baseline, **WIN** vs head
- `15:07:19` **solver-a** score `puct` `w0=2.0,w1=0.0` -> solved 75/81, steps 1502, iters 2425, moved 51 — **WIN** vs baseline, **LOSS** vs head
- `15:07:28` **solver-b** score `puct` `fpu=2.5,decay=0.985,reward_scale=2` -> solved 78/81, steps 1483, iters 2206, moved 33 — **WIN** vs baseline, **WIN** vs head
- `15:07:30` **solver-a** score `puct` `w0=0.5,w1=0.0` -> solved 77/81, steps 1494, iters 2294, moved 51 — **WIN** vs baseline, **WIN** vs head
- `15:07:46` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,decay=0.99` -> solved 79/81, steps 1498, iters 2253, moved 73 — **WIN** vs baseline, **WIN** vs head
- `15:07:46` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2` -> solved 78/81, steps 1483, iters 2206, moved 32 — **WIN** vs baseline, **WIN** vs head

### SUBMIT `puct-novelty-2` — solver-b @ 47cb875b
fpu=1.5,decay=0.985,reward_scale=2 solves 78/81 at steps 1483, iters 2206; with the novelty gap widened the progress reward can be given real weight again, and it buys 15 steps (reward_scale=0 at the same fpu/decay still solves 78 but at 1498 steps)

solved **78/81** (baseline 57), steps **1483** (baseline 1501), iters **2206** (baseline 3175) — **WIN**
policy=`puct` puct=`fpu=1.5,decay=0.985,reward_scale=2`

- `15:07:57` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=150` -> solved 79/81, steps 1499, iters 2438, moved 74 — **WIN** vs baseline, **WIN** vs head
- `15:08:08` **solver-b** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,fpu=1.5,decay=0.985,reward_scale=2` -> solved 78/81, steps 1503, iters 2353, moved 69 — **WIN** vs baseline, **WIN** vs head
- `15:08:09` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=250` -> solved 79/81, steps 1500, iters 2280, moved 73 — **WIN** vs baseline, **WIN** vs head
- `15:08:19` **solver-b** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,fpu=1.5,decay=0.985` -> solved 78/81, steps 1502, iters 2328, moved 70 — **WIN** vs baseline, **WIN** vs head
- `15:08:20` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=100` -> solved 79/81, steps 1507, iters 2196, moved 63 — **WIN** vs baseline, **WIN** vs head
- `15:08:31` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=300` -> solved 79/81, steps 1502, iters 2358, moved 72 — **WIN** vs baseline, **WIN** vs head
- `15:08:50` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,decay=0.99` -> solved 79/81, steps 1498, iters 2253, moved 73 — **WIN** vs baseline, **WIN** vs head

### SUBMIT `puct-w1zero` — solver-a @ 2d2ef3ec
dropping the refiner's broken second prior channel (w1=0) plus a 20% uniform arm: 79/81

solved **79/81** (baseline 57), steps **1498** (baseline 1501), iters **2253** (baseline 3175) — **WIN**
policy=`puct` puct=`w0=1.0,w1=0.0,epsilon_milli=200,decay=0.99`

- `15:09:17` **solver** score `puct` `epsilon_milli=200` -> solved 79/81, steps 1509, iters 2676, moved 73 — **WIN** vs baseline, **WIN** vs head
- `15:09:22` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=400` -> solved 78/81, steps 1507, iters 2401, moved 75 — **WIN** vs baseline, **WIN** vs head
- `15:09:33` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=600` -> solved 79/81, steps 1518, iters 2792, moved 77 — **WIN** vs baseline, **WIN** vs head
- `15:09:44` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,top_k=2` -> solved 77/81, steps 1590, iters 3209, moved 74 — **WIN** vs baseline, **WIN** vs head
- `15:09:55` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,fpu=0.25` -> solved 79/81, steps 1505, iters 2429, moved 71 — **WIN** vs baseline, **WIN** vs head
- `15:10:06` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,c_puct=2.5` -> solved 79/81, steps 1512, iters 2387, moved 72 — **WIN** vs baseline, **WIN** vs head
- `15:10:24` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,temperature=0.4` -> solved 79/81, steps 1506, iters 2394, moved 72 — **WIN** vs baseline, **WIN** vs head
- `15:10:35` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,temperature=0.6` -> solved 78/81, steps 1503, iters 2357, moved 68 — **WIN** vs baseline, **WIN** vs head
- `15:10:46` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,temperature=0.3` -> solved 79/81, steps 1523, iters 2515, moved 72 — **WIN** vs baseline, **WIN** vs head
- `15:10:57` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,temperature=0.7` -> solved 79/81, steps 1503, iters 2326, moved 70 — **WIN** vs baseline, **WIN** vs head
- `15:11:08` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,decay=0.99,fpu=0.4` -> solved 79/81, steps 1509, iters 2395, moved 72 — **WIN** vs baseline, **WIN** vs head
- `15:11:09` **solver** score `weighted` -> solved 57/81, steps 1501, iters 3175, moved 74 — **TIE** vs baseline, **LOSS** vs head
- `15:11:26` **solver-a** score `puct` `w0=0.5,w1=0.5,epsilon_milli=200` -> solved 77/81, steps 1540, iters 3465, moved 78 — **WIN** vs baseline, **WIN** vs head
- `15:11:37` **solver-a** score `puct` `w0=1.0,w1=0.5,epsilon_milli=200,decay=0.99` -> solved 77/81, steps 1503, iters 2883, moved 74 — **WIN** vs baseline, **WIN** vs head
- `15:11:48` **solver-a** score `puct` `w0=1.0,w1=0.2,epsilon_milli=200,decay=0.99` -> solved 78/81, steps 1516, iters 2547, moved 71 — **WIN** vs baseline, **WIN** vs head
- `15:11:49` **solver** score `puct` -> solved 76/81, steps 1486, iters 2188, moved 0 — **WIN** vs baseline, **TIE** vs head
- `15:11:59` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,decay=0.99,c_puct=1.2` -> solved 80/81, steps 1501, iters 2326, moved 73 — **WIN** vs baseline, **WIN** vs head
- `15:12:00` **solver** score `puct` `c_puct=2.0,decay=0.985` -> solved 78/81, steps 1492, iters 2252, moved 23 — **WIN** vs baseline, **WIN** vs head
- `15:12:11` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=175,decay=0.99` -> solved 79/81, steps 1501, iters 2416, moved 75 — **WIN** vs baseline, **WIN** vs head
- `15:12:11` **solver** score `puct` `epsilon_milli=200` -> solved 79/81, steps 1509, iters 2676, moved 73 — **WIN** vs baseline, **WIN** vs head
- `15:12:23` **solver** score `puct` `fpu=1.5,decay=0.985,reward_scale=2` -> solved 78/81, steps 1483, iters 2206, moved 32 — **WIN** vs baseline, **WIN** vs head
- `15:12:27` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,decay=0.99,c_puct=1.1` -> solved 79/81, steps 1501, iters 2353, moved 71 — **WIN** vs baseline, **WIN** vs head
- `15:12:34` **solver** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,cap=1` -> solved 78/81, steps 1490, iters 2206, moved 33 — **WIN** vs baseline, **WIN** vs head
- `15:12:38` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,decay=0.99,c_puct=1.25` -> solved 80/81, steps 1506, iters 2337, moved 72 — **WIN** vs baseline, **WIN** vs head
- `15:12:45` **solver** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,epsilon_milli=200` -> solved 78/81, steps 1498, iters 2533, moved 71 — **WIN** vs baseline, **WIN** vs head
- `15:12:49` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,decay=0.99,c_puct=1.3` -> solved 79/81, steps 1508, iters 2311, moved 72 — **WIN** vs baseline, **WIN** vs head
- `15:13:00` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,decay=0.99,c_puct=1.35` -> solved 79/81, steps 1501, iters 2289, moved 71 — **WIN** vs baseline, **WIN** vs head
- `15:13:14` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,decay=0.99,c_puct=1.4` -> solved 79/81, steps 1504, iters 2302, moved 71 — **WIN** vs baseline, **WIN** vs head
- `15:13:31` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,decay=0.99,c_puct=1.2` -> solved 80/81, steps 1501, iters 2326, moved 73 — **WIN** vs baseline, **WIN** vs head

### SUBMIT `puct-tuned` — solver-a @ 2d2ef3ec
tuned PUCT (w1=0, eps=200, decay=0.99, c_puct=1.2): 80/81 solved, 1501 steps

solved **80/81** (baseline 57), steps **1501** (baseline 1501), iters **2326** (baseline 3175) — **WIN**
policy=`puct` puct=`w0=1.0,w1=0.0,epsilon_milli=200,decay=0.99,c_puct=1.2`

- `15:13:57` **solver** score `puct` -> solved 78/81, steps 1483, iters 2206, moved 32 — **WIN** vs baseline, **WIN** vs head
- `15:14:00` **solver-a** score `puct` `w0=0.0,w1=1.0` -> solved 70/81, steps 1396, iters 3466, moved 76 — **WIN** vs baseline, **LOSS** vs head

### SUBMIT `neg-w1only` — solver-a @ 2d2ef3ec
NEGATIVE: prior channel 1 alone gives the shortest plans ever seen (1396 steps) but solves only 70/81 - it is a length signal, not a reachability one

solved **70/81** (baseline 57), steps **1396** (baseline 1501), iters **3466** (baseline 3175) — **WIN**
policy=`puct` puct=`w0=0.0,w1=1.0`

- `15:14:14` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,top_k=2` -> solved 77/81, steps 1590, iters 3209, moved 74 — **WIN** vs baseline, **WIN** vs head

### SUBMIT `neg-topk2` — solver-a @ 2d2ef3ec
NEGATIVE: top_k=2 alone reaches 78/81 but does not stack - with w1=0 and epsilon it drops to 77/81 and costs 90 extra steps

solved **77/81** (baseline 57), steps **1590** (baseline 1501), iters **3209** (baseline 3175) — **WIN**
policy=`puct` puct=`w0=1.0,w1=0.0,epsilon_milli=200,top_k=2`

- `15:14:25` **solver** score `puct` -> solved 78/81, steps 1483, iters 2206, moved 32 — **WIN** vs baseline, **WIN** vs head
- `15:15:09` **solver** score `puct` -> solved 78/81, steps 1483, iters 2206, moved 0 — **WIN** vs baseline, **TIE** vs head
- `15:15:20` **solver** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,decay=0.99,c_puct=1.2` -> solved 78/81, steps 1502, iters 2413, moved 76 — **LOSS** vs baseline, **LOSS** vs head
- `15:15:30` **solver** score `puct` `w0=1.0,w1=0.0` -> solved 77/81, steps 1496, iters 2181, moved 45 — **LOSS** vs baseline, **LOSS** vs head
- `15:15:41` **solver** score `puct` `w0=0.0,w1=1.0` -> solved 72/81, steps 1461, iters 3620, moved 75 — **LOSS** vs baseline, **LOSS** vs head
- `15:15:52` **solver** score `puct` `w0=1.0,w1=0.0,decay=0.985,fpu=1.5,reward_scale=2` -> solved 77/81, steps 1496, iters 2181, moved 45 — **LOSS** vs baseline, **LOSS** vs head
- `15:16:14` **solver** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,decay=0.99,c_puct=1.2,fpu=0.5,reward_scale=1` -> solved 80/81, steps 1501, iters 2326, moved 76 — **LOSS** vs baseline, **LOSS** vs head
- `15:16:25` **solver** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,decay=0.99,c_puct=1.2,fpu=1.5,reward_scale=2` -> solved 78/81, steps 1502, iters 2413, moved 76 — **LOSS** vs baseline, **LOSS** vs head
- `15:16:36` **solver** score `puct` `w0=1.0,w1=0.0,epsilon_milli=100,decay=0.985,fpu=1.5,reward_scale=2,c_puct=1.2` -> solved 79/81, steps 1514, iters 2260, moved 62 — **LOSS** vs baseline, **LOSS** vs head

### SUPERVISOR — round 1 closed. What ratcheted, what did not, and why

**Head is now `efdd0705`+: PUCT defaults `fpu=1.5, decay=0.985, reward_scale=2`.**
78/81 solved, real-workflow steps 693 (baseline 700), total steps 1483, iters 2206.

Per family, joint-with-baseline steps:

| config | ladder | meta | sink solved | total |
|---|---|---|---|---|
| baseline (shipped rule) | 563 | 137 | 40/64 | 57/81 |
| **head (new defaults)** | **563** | **130** | 61/64 | **78/81** |
| arm A `w0=1,w1=0,eps=200,decay=0.99,c_puct=1.2,fpu=0.5,reward_scale=1` | 563 | 141 | **63/64** | **80/81** |
| arm A `eps=200` alone | 572 | 143 | 62/64 | 79/81 |
| arm A `w1=1,w0=0` | 578 | 143 | 55/64 | 72/81 |

**The ranking changed mid-round and it changed the answer.** Solved-count-first would have taken
`epsilon_milli=200`, which buys generated cases by lengthening every real plan. It now reads as a
LOSS. Arm A's 80/81 is the best capability result anyone has produced and it is *not* the head,
because it costs 11 metagenomics steps against the head. It is recorded here as a real alternative,
not discarded — if capability on hard instances is what you want, that config is the answer.

**Careful: arm A's 80/81 depends on the OLD defaults.** It does not pin `fpu` or `reward_scale`, so
re-running it now silently inherits the new ones and gives 78/81. Pin every knob you care about.

**The pattern across all three arms is one trade.** Every route to more solved cases —
`epsilon_milli`, `top_k>1`, `temperature`, `w1=0` — is the same lever: more randomness in the draw.
It reaches the self-feeding-frontier cases and it lengthens real plans. Nobody has yet bought
capability without paying plan length.

**That is round 2's mission: break the trade.** The lead is arm C's — the five unsolved cases fail
because a self-feeding transform floods the frontier and a no-progress step earns reward 0, so Q
collapses and selection degenerates to the prior. A *structural* fix (a negative reward for a
no-progress expansion, or a self-feeding flag as a prior channel) should reach those cases without
paying the global randomisation tax that epsilon pays.

- `15:18:56` **solver-a** score `puct` -> solved 78/81, steps 1483, iters 2206, moved 0 — **WIN** vs baseline, **TIE** vs head

### SUPERVISOR — what the head is actually worth in wall clock (honesty check)

Serial, interleaved, two reps, `systemd-run MemoryMax=12G`, head vs the pre-seam reference binary:

| payload | budget | baseline | head | steps |
|---|---|---|---|---|
| ladder-53 | refine=32 | 0.16 s / 10.1 MB | 0.18 s / 10.2 MB | 72 both |
| ladder-68 | refine=32 | 0.30 s / 15.5 MB | 0.30 s / 15.7 MB | 81 both |
| unpinned | refine=32 | 0.45-0.51 s / 31.7 MB | 0.52 s / 31.9 MB | 35 both |

**At the shipped budgets there is no wall-clock win on the real workflows, and the report must say
so.** They already solve in half a second since the refiner's lineage prune landed (4m06s -> 1.76s,
journal 3e7ddb2a), so there was no time left for a selection policy to save. Report 04's
553s -> 12.5s was measured on the *unpruned* refiner; that headroom is already spent.

What the head IS worth, measured:

- **capability** — 57/81 -> 78/81 solved, and 80/81 under arm A's exploration config.
- **search effort** — 3175 -> 2206 mcts iterations (-31%).
- **plan length on real metagenomics** — 137 -> 130 steps, and it is per-arm rather than one
  outlier: `unpinned` 36 -> 33, `orfs_only` 35 -> 32, `bins_only` 35 -> 34, `shipped` 31 -> 31.
  The ladder is unchanged at 563 across every config anyone has found.

The 3 steps off `unpinned` are worth a look by whoever picks this up: that arm is the *duplicate
work* case (`research/metasmith/witness_sweep/duplicate_work.py`), where an underspecified workflow
runs one tool twice. If the shorter plan is the duplicate going away, that is a user-visible result
and not just a number.

- `15:19:15` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,decay=0.99,c_puct=1.2,fpu=0.5,reward_scale=1` -> solved 80/81, steps 1501, iters 2326, moved 76 — **LOSS** vs baseline, **LOSS** vs head
- `15:19:29` **solver-a** score `puct` `w0=1.0,w1=0.0,epsilon_milli=200,decay=0.99,c_puct=1.2,fpu=0.5,reward_scale=1` -> solved 80/81, steps 1501, iters 2326, moved 76 — **LOSS** vs baseline, **LOSS** vs head
- `15:19:51` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2` -> solved 78/81, steps 1483, iters 2206, moved 0 — **WIN** vs baseline, **TIE** vs head
- `15:20:07` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,no_progress=0.05` -> solved 78/81, steps 1483, iters 2206, moved 3 — **WIN** vs baseline, **TIE** vs head
- `15:20:18` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,no_progress=0.25` -> solved 78/81, steps 1483, iters 2195, moved 7 — **WIN** vs baseline, **WIN** vs head
- `15:20:29` **solver-a** score `puct` `w0=1.0,w1=0.0,decay=0.99,c_puct=1.2,fpu=0.5,reward_scale=1,epsilon_milli=200,eps_sched=linear,eps_horizon=60` -> solved 78/81, steps 1507, iters 2419, moved 71 — **LOSS** vs baseline, **LOSS** vs head
- `15:20:30` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,no_progress=1.0` -> solved 78/81, steps 1496, iters 2191, moved 11 — **WIN** vs baseline, **LOSS** vs head

### SUPERVISOR — CORRECTION to the wall-clock entry above, and the best result of the run

**The previous entry measured the head with `MSM_SOLVER_POLICY` unset.** PUCT is opt-in, so that
compared the baseline against the baseline: identical code paths, identical numbers, and the
conclusion drawn from it ("no wall-clock win on real workflows") was wrong. Corrected below.

Interleaved, 3 reps, mean, `MSM_SOLVER_POLICY=puct` actually set:

| payload | budget | baseline | head (PUCT) | speedup |
|---|---|---|---|---|
| unpinned | refine=8 | 0.513 s | **0.080 s** | **6.4x** |
| unpinned | refine=32 | 0.547 s | **0.103 s** | **5.3x** |
| ladder-68 | refine=8 | 0.253 s | 0.083 s | 3.0x |
| ladder-68 | refine=32 | 0.320 s | 0.173 s | 1.8x |
| ladder-53 | refine=8 | 0.070 s | 0.063 s | 1.1x |
| ladder-53 | refine=32 | 0.160 s | 0.127 s | 1.3x |

**And the mechanism is the duplicate-work pathology going away.** On the `unpinned` metagenomics
arm, transforms applied more than once:

    baseline       36 steps, 50 its   t70x2  t46x2  t64x2  t62x4  t76x3
    head + PUCT    33 steps, 43 its   t70x2         t62x3  t76x3

t46 and t64 stop being duplicated outright and t62 loses one application. That is exactly the case
`.awm/context.md` records as structurally beyond the refiner -- "`expand_node` yields `base +
[appl]` ... every candidate has exactly as many steps as its parent. No move deletes a step" -- with
the remedy assigned to the author, via a target-level anchor on every target that could diverge.
**PUCT does not merge the duplicate; it never creates it.** That is a user-visible result on a
shipped workflow, not a corpus number.

Read the earlier entry as retracted on wall clock. Its other three claims (capability, iteration
count, per-arm step counts) came from `score` with the policy correctly set and still stand.

- `15:20:40` **solver-a** score `puct` `w0=1.0,w1=0.0,decay=0.99,c_puct=1.2,fpu=0.5,reward_scale=1,epsilon_milli=0,eps_end_milli=200,eps_sched=step,eps_horizon=60` -> solved 77/81, steps 1499, iters 2165, moved 51 — **LOSS** vs baseline, **LOSS** vs head
- `15:20:52` **solver-a** score `puct` `w0=1.0,w1=0.0,decay=0.99,c_puct=1.2,fpu=0.5,reward_scale=1,epsilon_milli=200,eps_sched=step,eps_horizon=60` -> solved 79/81, steps 1501, iters 2291, moved 75 — **LOSS** vs baseline, **LOSS** vs head
- `15:20:55` **solver-c** score `puct` -> solved 78/81, steps 1483, iters 2206, moved 0 — **WIN** vs baseline, **TIE** vs head
- `15:20:55` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,no_progress=0.25,np_ramp=0.005` -> solved 76/81, steps 1483, iters 2195, moved 11 — **WIN** vs baseline, **LOSS** vs head
- `15:21:07` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,no_progress=0.25,np_ramp=0.02` -> solved 76/81, steps 1483, iters 2195, moved 11 — **WIN** vs baseline, **LOSS** vs head
- `15:21:09` **solver-a** score `puct` `w0=1.0,w1=0.0,decay=0.99,c_puct=1.2,fpu=0.5,reward_scale=1` -> solved 77/81, steps 1497, iters 2157, moved 51 — **LOSS** vs baseline, **LOSS** vs head
- `15:21:12` **solver-c** score `puct` `w2=1` -> solved 81/81, steps 1536, iters 2211, moved 35 — **WIN** vs baseline, **WIN** vs head
- `15:21:19` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,no_progress=0.25,np_ramp=0.05` -> solved 77/81, steps 1483, iters 2197, moved 10 — **WIN** vs baseline, **LOSS** vs head
- `15:21:22` **solver-a** score `puct` `w0=1.0,w1=0.0,decay=0.99,c_puct=1.2` -> solved 77/81, steps 1495, iters 2206, moved 42 — **LOSS** vs baseline, **LOSS** vs head
- `15:21:24` **solver-c** score `puct` `w2=2` -> solved 80/81, steps 1550, iters 2205, moved 40 — **WIN** vs baseline, **WIN** vs head
- `15:21:33` **solver-a** score `puct` `epsilon_milli=200` -> solved 78/81, steps 1498, iters 2533, moved 75 — **LOSS** vs baseline, **LOSS** vs head
- `15:21:35` **solver-c** score `puct` `w2=3` -> solved 80/81, steps 1550, iters 2205, moved 40 — **WIN** vs baseline, **WIN** vs head
- `15:21:44` **solver-a** score `puct` `epsilon_milli=200,eps_sched=step,eps_horizon=60` -> solved 78/81, steps 1498, iters 2592, moved 72 — **LOSS** vs baseline, **LOSS** vs head
- `15:21:46` **solver-c** score `puct` `w2=5` -> solved 80/81, steps 1550, iters 2205, moved 40 — **WIN** vs baseline, **WIN** vs head
- `15:21:55` **solver-a** score `puct` `epsilon_milli=0,eps_end_milli=200,eps_sched=step,eps_horizon=60` -> solved 78/81, steps 1483, iters 2204, moved 8 — **WIN** vs baseline, **LOSS** vs head
- `15:22:02` **solver-c** score `puct` `w2=0.5` -> solved 79/81, steps 1487, iters 2172, moved 28 — **WIN** vs baseline, **WIN** vs head
- `15:22:12` **solver-c** score `puct` `w2=1` -> solved 81/81, steps 1536, iters 2211, moved 35 — **WIN** vs baseline, **WIN** vs head
- `15:22:24` **solver-c** score `puct` `w2=2` -> solved 80/81, steps 1550, iters 2205, moved 40 — **WIN** vs baseline, **WIN** vs head
- `15:22:24` **solver-a** score `puct` `epsilon_milli=0,eps_end_milli=500,eps_sched=step,eps_horizon=60` -> solved 77/81, steps 1488, iters 2198, moved 9 — **WIN** vs baseline, **LOSS** vs head
- `15:22:35` **solver-c** score `puct` `w2=3` -> solved 80/81, steps 1550, iters 2205, moved 40 — **WIN** vs baseline, **WIN** vs head
- `15:22:35` **solver-a** score `puct` `epsilon_milli=0,eps_end_milli=1000,eps_sched=step,eps_horizon=60` -> solved 75/81, steps 1523, iters 2479, moved 10 — **LOSS** vs baseline, **LOSS** vs head
- `15:22:46` **solver-c** score `puct` `w2=5` -> solved 80/81, steps 1550, iters 2205, moved 40 — **WIN** vs baseline, **WIN** vs head
- `15:22:46` **solver-a** score `puct` `epsilon_milli=0,eps_end_milli=200,eps_sched=step,eps_horizon=30` -> solved 77/81, steps 1495, iters 2330, moved 33 — **LOSS** vs baseline, **LOSS** vs head
- `15:22:57` **solver-c** score `puct` `w2=8` -> solved 80/81, steps 1550, iters 2205, moved 40 — **WIN** vs baseline, **WIN** vs head
- `15:22:57` **solver-a** score `puct` `epsilon_milli=0,eps_end_milli=500,eps_sched=step,eps_horizon=30` -> solved 77/81, steps 1494, iters 2248, moved 38 — **LOSS** vs baseline, **LOSS** vs head
- `15:23:09` **solver-a** score `puct` `epsilon_milli=0,eps_end_milli=200,eps_sched=step,eps_horizon=120` -> solved 77/81, steps 1483, iters 2206, moved 1 — **WIN** vs baseline, **LOSS** vs head
- `15:23:14` **solver-c** score `puct` `w2=0.6` -> solved 80/81, steps 1487, iters 2194, moved 31 — **WIN** vs baseline, **WIN** vs head
- `15:23:24` **solver-c** score `puct` `w2=0.7` -> solved 80/81, steps 1529, iters 2222, moved 32 — **WIN** vs baseline, **WIN** vs head
- `15:23:35` **solver-c** score `puct` `w2=0.75` -> solved 80/81, steps 1529, iters 2222, moved 32 — **WIN** vs baseline, **WIN** vs head
- `15:23:46` **solver-c** score `puct` `w2=0.8` -> solved 80/81, steps 1529, iters 2222, moved 32 — **WIN** vs baseline, **WIN** vs head
- `15:23:48` **solver-a** score `puct` `epsilon_milli=200,eps_phase=mcts` -> solved 78/81, steps 1498, iters 2533, moved 75 — **LOSS** vs baseline, **LOSS** vs head
- `15:23:58` **solver-c** score `puct` `w2=0.9` -> solved 81/81, steps 1535, iters 2229, moved 34 — **WIN** vs baseline, **WIN** vs head
- `15:23:59` **solver-a** score `puct` `epsilon_milli=200,eps_phase=refine` -> solved 78/81, steps 1486, iters 2206, moved 1 — **WIN** vs baseline, **LOSS** vs head
- `15:24:09` **solver-c** score `puct` `w2=1.1` -> solved 81/81, steps 1550, iters 2219, moved 41 — **WIN** vs baseline, **WIN** vs head
- `15:24:11` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,widen=0.25` -> solved 77/81, steps 1486, iters 2157, moved 15 — **WIN** vs baseline, **LOSS** vs head
- `15:24:20` **solver-c** score `puct` `w2=1.25` -> solved 81/81, steps 1550, iters 2205, moved 41 — **WIN** vs baseline, **WIN** vs head
- `15:24:23` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,widen=0.5` -> solved 77/81, steps 1488, iters 2225, moved 24 — **WIN** vs baseline, **LOSS** vs head
- `15:24:32` **solver-c** score `puct` `w2=1.5` -> solved 80/81, steps 1550, iters 2205, moved 40 — **WIN** vs baseline, **WIN** vs head
- `15:24:34` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,widen=1.0` -> solved 77/81, steps 1494, iters 2269, moved 31 — **LOSS** vs baseline, **LOSS** vs head
- `15:24:35` **solver-a** score `puct` `epsilon_milli=200,eps_min_len=50` -> solved 77/81, steps 1491, iters 2206, moved 4 — **LOSS** vs baseline, **LOSS** vs head
- `15:24:46` **solver-a** score `puct` `epsilon_milli=200,eps_min_len=100` -> solved 78/81, steps 1491, iters 2206, moved 3 — **LOSS** vs baseline, **LOSS** vs head
- `15:24:51` **solver-c** score `puct` `w2=0.85` -> solved 80/81, steps 1535, iters 2229, moved 33 — **WIN** vs baseline, **WIN** vs head
- `15:24:52` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,no_progress=0.1` -> solved 78/81, steps 1483, iters 2213, moved 6 — **WIN** vs baseline, **WIN** vs head
- `15:24:58` **solver-a** score `puct` `epsilon_milli=200,eps_min_len=200` -> solved 78/81, steps 1491, iters 2206, moved 3 — **LOSS** vs baseline, **LOSS** vs head
- `15:25:03` **solver-c** score `puct` `w2=0.88` -> solved 80/81, steps 1535, iters 2229, moved 33 — **WIN** vs baseline, **WIN** vs head
- `15:25:04` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,no_progress=0.5` -> solved 78/81, steps 1493, iters 2202, moved 9 — **WIN** vs baseline, **LOSS** vs head
- `15:25:09` **solver-a** score `puct` `epsilon_milli=200,eps_min_len=400` -> solved 78/81, steps 1491, iters 2206, moved 2 — **LOSS** vs baseline, **LOSS** vs head
- `15:25:14` **solver-c** score `puct` `w2=0.92` -> solved 81/81, steps 1535, iters 2213, moved 34 — **WIN** vs baseline, **WIN** vs head
- `15:25:15` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,no_progress=0.25,np_phase=mcts` -> solved 78/81, steps 1483, iters 2195, moved 7 — **WIN** vs baseline, **WIN** vs head
- `15:25:26` **solver-c** score `puct` `w2=0.95` -> solved 81/81, steps 1535, iters 2213, moved 34 — **WIN** vs baseline, **WIN** vs head
- `15:25:26` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,no_progress=0.25,np_phase=refine` -> solved 78/81, steps 1483, iters 2206, moved 0 — **WIN** vs baseline, **TIE** vs head
- `15:25:29` **solver-a** score `puct` `epsilon_milli=200,eps_min_len=800` -> solved 78/81, steps 1486, iters 2206, moved 1 — **WIN** vs baseline, **LOSS** vs head
- `15:25:40` **solver-a** score `puct` `epsilon_milli=200,eps_min_len=1500` -> solved 78/81, steps 1483, iters 2206, moved 0 — **WIN** vs baseline, **TIE** vs head
- `15:25:46` **solver-c** score `puct` `w2=0.95` -> solved 81/81, steps 1535, iters 2213, moved 34 — **WIN** vs baseline, **WIN** vs head

### SUBMIT `selffeed-prior` — solver-c @ 3bc45c85
w2=0.95: a structural third prior channel solves 81/81 at real-workflow steps 693 -- unchanged. A transform whose product has the SAME TYPE as one of its own requirements re-enables itself forever; the flag is computed once in problem.rs from the transform signature, and it is identically 0 on all 17 real payloads and 1-5 on every generated one, so the penalty cannot move a real plan at any weight. This breaks round 1's capability-for-plan-length trade: dup/cap read the frontier population after the runaway has grown, this reads the signature before the search starts.

solved **81/81** (baseline 57), steps **1535** (baseline 1501), iters **2213** (baseline 3175) — **WIN**
policy=`puct` puct=`w2=0.95`

- `15:25:51` **solver-a** score `puct` `epsilon_milli=500,eps_min_len=800` -> solved 78/81, steps 1486, iters 2206, moved 1 — **WIN** vs baseline, **LOSS** vs head
- `15:26:03` **solver-a** score `puct` `epsilon_milli=1000,eps_min_len=800` -> solved 78/81, steps 1486, iters 2206, moved 1 — **WIN** vs baseline, **LOSS** vs head
- `15:26:06` **solver-c** score `puct` `w2=0.95,cap=1` -> solved 81/81, steps 1542, iters 2213, moved 37 — **WIN** vs baseline, **LOSS** vs head
- `15:26:17` **solver-c** score `puct` `w2=0.95,c_puct=1.2` -> solved 81/81, steps 1544, iters 2200, moved 37 — **LOSS** vs baseline, **LOSS** vs head
- `15:26:29` **solver-a** score `puct` `epsilon_milli=200,w0=1.0,w1=0.0` -> solved 78/81, steps 1503, iters 2353, moved 73 — **LOSS** vs baseline, **LOSS** vs head
- `15:26:29` **solver-c** score `puct` `w2=0.95,c_puct=2.0` -> solved 79/81, steps 1541, iters 2226, moved 40 — **WIN** vs baseline, **LOSS** vs head
- `15:26:30` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,widen=2` -> solved 79/81, steps 1496, iters 2142, moved 35 — **LOSS** vs baseline, **LOSS** vs head
- `15:26:34` **solver** score `puct` -> solved 78/81, steps 1483, iters 2206, moved 0 — **WIN** vs baseline, **TIE** vs head
- `15:26:42` **solver-a** score `puct` `epsilon_milli=200,c_puct=1.2` -> solved 78/81, steps 1500, iters 2540, moved 74 — **LOSS** vs baseline, **LOSS** vs head
- `15:26:42` **solver-c** score `puct` `w2=0.95,reward_scale=3` -> solved 81/81, steps 1538, iters 2190, moved 37 — **WIN** vs baseline, **LOSS** vs head
- `15:26:42` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,widen=4` -> solved 77/81, steps 1497, iters 2236, moved 39 — **LOSS** vs baseline, **LOSS** vs head
- `15:26:46` **solver** score `puct` `w2=0.95` -> solved 81/81, steps 1535, iters 2213, moved 34 — **WIN** vs baseline, **WIN** vs head
- `15:26:55` **solver-c** score `puct` `w2=0.95,temperature=0.35` -> solved 79/81, steps 1544, iters 2151, moved 45 — **LOSS** vs baseline, **LOSS** vs head
- `15:26:55` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,widen=2,no_progress=0.25` -> solved 77/81, steps 1496, iters 2117, moved 36 — **LOSS** vs baseline, **LOSS** vs head
- `15:26:55` **solver-a** score `puct` `epsilon_milli=200,decay=0.99` -> solved 78/81, steps 1500, iters 2536, moved 75 — **LOSS** vs baseline, **LOSS** vs head
- `15:26:59` **solver** score `puct` `w2=0.5` -> solved 79/81, steps 1487, iters 2172, moved 28 — **WIN** vs baseline, **WIN** vs head
- `15:27:06` **solver-c** score `puct` `w2=0.95,dup=0.5` -> solved 81/81, steps 1541, iters 2247, moved 47 — **WIN** vs baseline, **LOSS** vs head
- `15:27:07` **solver-a** score `puct` `epsilon_milli=200,fpu=0.5` -> solved 78/81, steps 1501, iters 2579, moved 73 — **LOSS** vs baseline, **LOSS** vs head
- `15:27:11` **solver** score `puct` `w2=1.5` -> solved 80/81, steps 1550, iters 2205, moved 40 — **WIN** vs baseline, **WIN** vs head
- `15:27:16` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,no_progress=0.2` -> solved 78/81, steps 1483, iters 2196, moved 5 — **WIN** vs baseline, **WIN** vs head
- `15:27:19` **solver-a** score `puct` `epsilon_milli=200,reward_scale=1` -> solved 78/81, steps 1502, iters 2555, moved 72 — **LOSS** vs baseline, **LOSS** vs head
- `15:27:27` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,no_progress=0.3` -> solved 78/81, steps 1483, iters 2205, moved 6 — **WIN** vs baseline, **WIN** vs head
- `15:27:38` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,no_progress=0.25,np_floor=-0.25` -> solved 78/81, steps 1483, iters 2195, moved 7 — **WIN** vs baseline, **WIN** vs head
- `15:27:45` **solver** score `puct` `w2=0.8` -> solved 80/81, steps 1529, iters 2222, moved 32 — **WIN** vs baseline, **WIN** vs head
- `15:27:54` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,no_progress=0.25` -> solved 78/81, steps 1483, iters 2195, moved 7 — **WIN** vs baseline, **WIN** vs head

### SUBMIT `no-progress-penalty` — solver-b @ e5012426
a no-progress expansion charged -0.25 instead of 0 holds real steps at 693 and total steps at 1483 while cutting 11 iterations; it does NOT reach any unsolved case, because on sink26-103 249 of 256 observations already carry reward 0 -- every key looks equally bad, so a penalty has nothing to differentiate

solved **78/81** (baseline 57), steps **1483** (baseline 1501), iters **2195** (baseline 3175) — **WIN**
policy=`puct` puct=`fpu=1.5,decay=0.985,reward_scale=2,no_progress=0.25`

- `15:27:56` **solver** score `puct` `w2=0.9` -> solved 81/81, steps 1535, iters 2229, moved 34 — **WIN** vs baseline, **WIN** vs head
- `15:28:03` **solver-c** score `puct` -> solved 78/81, steps 1483, iters 2206, moved 0 — **WIN** vs baseline, **TIE** vs head
- `15:28:07` **solver-a** score `puct` `epsilon_milli=200,eps_dup_frac=0.5` -> solved 78/81, steps 1500, iters 2327, moved 31 — **LOSS** vs baseline, **LOSS** vs head
- `15:28:08` **solver** score `puct` `w2=1.0` -> solved 81/81, steps 1536, iters 2211, moved 35 — **WIN** vs baseline, **WIN** vs head
- `15:28:14` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,widen=2` -> solved 79/81, steps 1496, iters 2142, moved 35 — **LOSS** vs baseline, **LOSS** vs head

### SUBMIT `neg-widen-multiplicity` — solver-b @ e5012426
NEGATIVE: arm C's diagnosis inverted -- with PUCT the self-feeding transform takes 17 of 256 selections on sink26-103, not 714, because per-transform keys make 774 distinct slots share one visit count and selection round-robins over transform identity; boosting a flooded key's prior by (1+ln count)^2 reaches sink26-104 in 126 iterations and lifts solved to 79/81, but costs 13 real-workflow steps (706 vs 693) -- the same tax epsilon pays, so the trade is not epsilon-specific, it is the price of re-selecting any key

solved **79/81** (baseline 57), steps **1496** (baseline 1501), iters **2142** (baseline 3175) — **LOSS**
policy=`puct` puct=`fpu=1.5,decay=0.985,reward_scale=2,widen=2`

- `15:28:19` **solver-a** score `puct` `epsilon_milli=200,eps_by_key=true` -> solved 79/81, steps 1500, iters 2461, moved 74 — **LOSS** vs baseline, **LOSS** vs head
- `15:28:19` **solver-c** score `puct` `w2=0.95` -> solved 81/81, steps 1535, iters 2213, moved 34 — **WIN** vs baseline, **WIN** vs head
- `15:28:20` **solver** score `puct` `w2=1.1` -> solved 81/81, steps 1550, iters 2219, moved 41 — **WIN** vs baseline, **WIN** vs head
- `15:28:30` **solver-a** score `puct` `epsilon_milli=200,eps_dup_frac=0.5,eps_by_key=true` -> solved 78/81, steps 1494, iters 2346, moved 27 — **LOSS** vs baseline, **LOSS** vs head
- `15:28:31` **solver-c** score `puct` `w2=0.95,w2_super=0.95` -> solved 81/81, steps 1535, iters 2213, moved 34 — **WIN** vs baseline, **WIN** vs head
- `15:28:31` **solver** score `puct` `w2=1.2` -> solved 81/81, steps 1550, iters 2219, moved 41 — **WIN** vs baseline, **WIN** vs head
- `15:28:42` **solver-c** score `puct` `w2=0,w2_super=0.95` -> solved 78/81, steps 1483, iters 2206, moved 0 — **WIN** vs baseline, **TIE** vs head
- `15:28:48` **solver-a** score `puct` `epsilon_milli=200,eps_by_key=true,eps_dup_frac=0.7` -> solved 78/81, steps 1483, iters 2206, moved 0 — **WIN** vs baseline, **TIE** vs head
- `15:28:51` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,no_progress=0.25,widen=2,widen_phase=mcts` -> solved 77/81, steps 1493, iters 2117, moved 36 — **LOSS** vs baseline, **LOSS** vs head
- `15:28:55` **solver-c** score `puct` `w2=0.95,w2_super=0.3` -> solved 81/81, steps 1535, iters 2213, moved 34 — **WIN** vs baseline, **WIN** vs head
- `15:29:00` **solver-a** score `puct` `epsilon_milli=200,eps_by_key=true,eps_dup_frac=0.85` -> solved 78/81, steps 1483, iters 2206, moved 0 — **WIN** vs baseline, **TIE** vs head
- `15:29:02` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,no_progress=0.25,widen=2,widen_phase=refine` -> solved 78/81, steps 1483, iters 2195, moved 7 — **WIN** vs baseline, **WIN** vs head
- `15:29:11` **solver-a** score `puct` `epsilon_milli=100,eps_by_key=true` -> solved 78/81, steps 1494, iters 2430, moved 59 — **LOSS** vs baseline, **LOSS** vs head
- `15:29:18` **solver-c** score `puct` `w2=0,w2_super=5` -> solved 78/81, steps 1483, iters 2206, moved 0 — **WIN** vs baseline, **TIE** vs head
- `15:29:22` **solver-a** score `puct` `epsilon_milli=50,eps_by_key=true` -> solved 78/81, steps 1493, iters 2473, moved 47 — **LOSS** vs baseline, **LOSS** vs head
- `15:29:27` **solver-b** score `puct` `fpu=1.5,decay=0.985,reward_scale=2,no_progress=0.25,widen=2,widen_phase=mcts` -> solved 77/81, steps 1493, iters 2117, moved 36 — **LOSS** vs baseline, **LOSS** vs head

### SUBMIT `neg-widen-mcts-only` — solver-b @ 36f718ff
NEGATIVE, and it localises the trade: phase-gating the multiplicity boost shows widen_phase=refine is a byte-exact no-op (the refiner's 8-iteration budget never puts two slots of one key in a frontier), so the whole effect lives in the mcts phase -- where +1 solved costs 10 real-workflow steps. Any lever that lets one key be re-selected pays in the mcts phase and nowhere else

solved **77/81** (baseline 57), steps **1493** (baseline 1501), iters **2117** (baseline 3175) — **LOSS**
policy=`puct` puct=`fpu=1.5,decay=0.985,reward_scale=2,no_progress=0.25,widen=2,widen_phase=mcts`

- `15:29:31` **solver** score `weighted` -> solved 57/81, steps 1501, iters 3175, moved 76 — **TIE** vs baseline, **LOSS** vs head
- `15:29:31` **solver-c** score `puct` `w2=0,w2_super=20` -> solved 78/81, steps 1483, iters 2206, moved 0 — **WIN** vs baseline, **TIE** vs head
- `15:29:34` **solver-a** score `puct` `epsilon_milli=400,eps_by_key=true` -> solved 78/81, steps 1509, iters 2798, moved 75 — **LOSS** vs baseline, **LOSS** vs head
- `15:29:42` **solver-c** score `puct` `w2=0.95,w2_super=5` -> solved 81/81, steps 1535, iters 2213, moved 34 — **WIN** vs baseline, **WIN** vs head
- `15:29:46` **solver** score `puct` -> solved 78/81, steps 1483, iters 2206, moved 0 — **WIN** vs baseline, **TIE** vs head
- `15:29:58` **solver** score `puct` `w2=0.95` -> solved 81/81, steps 1535, iters 2213, moved 34 — **WIN** vs baseline, **WIN** vs head
- `15:30:03` **solver-a** score `puct` `epsilon_milli=0,eps_end_milli=200,eps_sched=step,eps_horizon=60,eps_by_key=true` -> solved 77/81, steps 1489, iters 2226, moved 9 — **WIN** vs baseline, **LOSS** vs head
- `15:30:06` **solver-c** score `puct` `w2=0,w2_super=20` -> solved 78/81, steps 1483, iters 2206, moved 0 — **WIN** vs baseline, **TIE** vs head

### SUBMIT `selffeed-super-inert` — solver-c @ db788768
NEGATIVE and a correction to my own claim: the OTHER flavour of self-feeding -- product a strict SUPERSET of a requirement, the enrichment step -- is inert at every weight. w2_super=0.95/5/20 all reproduce the head byte for byte (78/81, 1483, 2206). The 17 real payloads carry exactly one such transform and the 64 generated ones carry none, so I expected the equality test to be what keeps real plans free; in fact that transform is never a contested frontier pick, so a plain is_a channel would have been just as safe HERE. w2 and w2_super are not interchangeable: w2 alone is the whole 78->81, and adding w2_super on top of it changes nothing.

solved **78/81** (baseline 57), steps **1483** (baseline 1501), iters **2206** (baseline 3175) — **WIN**
policy=`puct` puct=`w2=0,w2_super=20`

- `15:30:09` **solver** score `puct` `no_progress=0.25` -> solved 78/81, steps 1483, iters 2195, moved 7 — **WIN** vs baseline, **WIN** vs head
- `15:30:14` **solver-a** score `puct` `epsilon_milli=200,eps_sched=linear,eps_horizon=256,eps_by_key=true` -> solved 79/81, steps 1508, iters 2593, moved 70 — **LOSS** vs baseline, **LOSS** vs head
- `15:30:20` **solver** score `puct` `w2=0.95,no_progress=0.25` -> solved 81/81, steps 1535, iters 2218, moved 36 — **WIN** vs baseline, **WIN** vs head
- `15:30:25` **solver-c** score `puct` `w2=0.95` -> solved 81/81, steps 1535, iters 2213, moved 34 — **WIN** vs baseline, **WIN** vs head
- `15:30:26` **solver-a** score `puct` `epsilon_milli=200,eps_by_key=true,eps_min_len=100` -> solved 77/81, steps 1488, iters 2210, moved 4 — **WIN** vs baseline, **LOSS** vs head
- `15:30:32` **solver** score `puct` `w2=1.0,no_progress=0.25` -> solved 81/81, steps 1536, iters 2216, moved 38 — **WIN** vs baseline, **WIN** vs head
- `15:30:37` **solver-a** score `puct` `epsilon_milli=200,eps_by_key=true,eps_dup_frac=0.6` -> solved 78/81, steps 1503, iters 2205, moved 6 — **LOSS** vs baseline, **LOSS** vs head
- `15:30:48` **solver-a** score `puct` `epsilon_milli=300,eps_by_key=true,eps_dup_frac=0.55` -> solved 78/81, steps 1491, iters 2199, moved 4 — **WIN** vs baseline, **LOSS** vs head
- `15:31:02` **solver** score `puct` -> solved 81/81, steps 1535, iters 2213, moved 34 — **WIN** vs baseline, **WIN** vs head
- `15:31:04` **solver-c** score `puct` `w2=0.95,w2_refine=0` -> solved 81/81, steps 1535, iters 2213, moved 34 — **WIN** vs baseline, **WIN** vs head
- `15:31:14` **solver-c** score `puct` `w2=0,w2_refine=0.95` -> solved 78/81, steps 1483, iters 2206, moved 0 — **WIN** vs baseline, **TIE** vs head
- `15:31:25` **solver-c** score `puct` `w2=0.95,w2_refine=2` -> solved 81/81, steps 1535, iters 2213, moved 34 — **WIN** vs baseline, **WIN** vs head
- `15:31:29` **solver-a** score `puct` `ref_w0=0.0,ref_w1=1.0` -> solved 78/81, steps 1490, iters 2206, moved 5 — **WIN** vs baseline, **LOSS** vs head
