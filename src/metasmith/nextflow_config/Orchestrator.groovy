import groovy.json.JsonOutput

class Orchestrator {
    // Reserved index key carrying a leaf's canonical instance_id from the
    // Python given-lineage seed. postIn relocates it to index[<name>] and
    // strips it, so it never propagates as a lineage key. Kept in lockstep
    // with workflow.py's given-seed (SELF_ID_KEY).
    public static final String SELF_ID_KEY = "__self__"

    // Reserved index keys the task reads and nothing downstream may see.
    // FILES is the staged path per slot; PROV is the per-item index maps
    // behind those paths, kept un-flattened so a protocol can ask which item
    // of one grouped slot another descends from. Both are added on the way
    // into a task and stripped on the way out (_debatch) -- leaving either in
    // would propagate it through every descendant index forever and put a
    // nested value into promoted shard manifests. Kept in lockstep with
    // models/lineage.py's LinPayload.FILES_KEY / PROV_KEY.
    public static final String FILES_KEY = "FILES"
    public static final String PROV_KEY = "PROV"
    // KEY is the member's cache key, stamped by _route before submission and
    // read by the task to name and promote its products. Kept in lockstep
    // with LinPayload.KEY_KEY.
    public static final String KEY_KEY = "KEY"
    // SIBS is the sibling stamp [by, n, i] _post puts on every produced item:
    // the producing member was grouped by `by` and delivered n items into
    // this slot, of which this is ordinal i. It is the only count early
    // release trusts, because no plan-time count exists: outputs are globs
    // and optional branches can emit nothing. Stripped from PROV and publish
    // so no cache key or manifest ever sees it. Kept in lockstep with
    // LinPayload.SIBS_KEY.
    public static final String SIBS_KEY = "SIBS"
    public static final List RESERVED_KEYS = [FILES_KEY, PROV_KEY, KEY_KEY, SIBS_KEY]

    // Raised when a stream `classify()` proved to be a descendant of the
    // by-stream delivers an item whose index does not carry the by-key.
    //
    // Thrown rather than logged because the alternative is invisible: a
    // dropped item makes the join emit nothing, an empty channel is not an
    // error in nextflow, and the DAG simply ends early with every submitted
    // task at exit 0. Two production runs lost days to exactly that -- one
    // truncated a nine-step workflow after seven steps, one lost 2 of 34 group
    // members -- and in both the only trace was a `_dispatchLog` row nothing
    // reads. A task that is never created cannot fail, so no error strategy
    // can see it.
    static class LineageViolation extends RuntimeException {
        LineageViolation(String message) { super(message) }
    }

    // The lineage-only view of a task's output index, used by _debatch on the
    // way out of every process.
    //
    // Returns a fresh map and never writes to its argument. A process
    // declaring N output tuples binds the SAME index object to all N output
    // channels, and each channel is a separate dataflow operator on its own
    // thread, so this runs N times over one unsynchronized LinkedHashMap. The
    // in-place remove() this replaced therefore raced its sibling streams and
    // _post's `[:]+index` read, and a lost race there does not corrupt the map
    // visibly -- it yields an EMPTY copy, whose product reaches the next
    // o.group carrying only its own key and is dropped for lineage violation,
    // taking the branch of the DAG below it with it. Concurrent readers of a
    // map nobody writes to are safe; that is the whole fix.
    //
    // Shallow by design. The value lists are shared by reference across many
    // descendant indexes (_collateBatch and _post both copy shallowly) and no
    // production path mutates one.
    public static Map stripReserved(index) {
        return index.findAll((k, v) -> !(k in RESERVED_KEYS))
    }

    private Map index_history
    private Map child2parent
    private def one_null
    private List _dispatchLog

    // The release registries. Written only by the workflow body (group, post,
    // postIn, mix), read only by operator closures, and the seal between the
    // two is what makes a read see every write: a producer posted after a
    // consumer's group() still counts, because nothing reads before o.seal().
    // That ordering rests on Nextflow starting data flow only after the body
    // returns, which is an implementation invariant and not a documented
    // contract, so a read before the seal throws instead of trusting it.
    private Map group_by_of
    private Map producers_of
    private Set mixed_streams
    private volatile boolean sealed = false
    private static final String GROUP_BY_CONFLICT = "\u0000conflict"

    Orchestrator(one_null) {
        this.index_history = new java.util.concurrent.ConcurrentHashMap()
        this.child2parent = new java.util.concurrent.ConcurrentHashMap()
        this.one_null = one_null
        this._dispatchLog = Collections.synchronizedList(new ArrayList())
        this.group_by_of = new java.util.concurrent.ConcurrentHashMap()
        this.producers_of = new java.util.concurrent.ConcurrentHashMap()
        this.mixed_streams = java.util.concurrent.ConcurrentHashMap.newKeySet()
    }

    // The last statement of the workflow body.
    public void seal() {
        this.sealed = true
    }

    private void _assertUnsealed(String what) {
        if (this.sealed) {
            throw new IllegalStateException(
                "Orchestrator: ${what} after o.seal(). Every group, post, postIn "
                + "and mix call belongs in the workflow body, before o.seal()."
            )
        }
    }

    private void _assertSealed(String what) {
        if (!this.sealed) {
            throw new IllegalStateException(
                "Orchestrator: ${what} was read before o.seal(). The workflow "
                + "body must end with o.seal(), so that no early-release "
                + "decision is made from a registry the body is still writing."
            )
        }
    }

    private void _countProducer(String name) {
        this._assertUnsealed("post of [${name}]")
        this.producers_of.merge(name, 1, (a, b) -> a + b)
    }

    private void _recordGroupBy(String target, String by) {
        this._assertUnsealed("group into [${target}]")
        def prev = this.group_by_of.putIfAbsent(target, by)
        if (prev != null && prev != by) this.group_by_of[target] = GROUP_BY_CONFLICT
    }

    private String _groupByOf(String name) {
        this._assertSealed("the group_by of [${name}]")
        def by = this.group_by_of.get(name)
        return (by == GROUP_BY_CONFLICT) ? null : by
    }

    private boolean _soleProducer(String name) {
        this._assertSealed("the producer count of [${name}]")
        return this.producers_of.getOrDefault(name, 0) == 1 && !this.mixed_streams.contains(name)
    }

    // The stamp an item may be released on for join key `key`, or null when
    // it may only flush at close. All three conditions are needed:
    //   - the producing member was grouped by `key`, so it is THE member for
    //     one key value and not one of several feeding it;
    //   - the item carries exactly one `key` hash, so no other member's items
    //     carry it too (a member that pulled in another key's item stamps
    //     every output with both hashes, which fails this test on both sides);
    //   - the stream has one producing post, so one member is all there is.
    // The registry is read last, so an unstamped item (a given, which may
    // flow during the body) never reads it.
    private List _releaseStamp(String name, String key, Map index) {
        def stamp = index[SIBS_KEY]
        if (!(stamp instanceof List) || stamp.size() != 3 || stamp[0] != key) return null
        def hashes = index[key]
        if (!(hashes instanceof List) || hashes.size() != 1) return null
        if (!this._soleProducer(name)) return null
        return stamp
    }

    public List getDispatchLog() {
        return new ArrayList(this._dispatchLog)
    }

    private void _logDispatch(name, relation, by_hash, item_hash) {
        this._dispatchLog.add([name, relation, by_hash, item_hash])
    }

    // The lineage keys of an index, without the reserved entries. FILES holds
    // absolute task-workdir paths and PROV holds nested maps, so an unfiltered
    // render buries the one fact the reader needs -- which keys the item
    // actually carries -- under kilobytes of noise.
    private static String _renderLineage(index) {
        if (!(index instanceof Map)) return "${index}"
        return "${stripReserved(index)}"
    }

    public void seedParents(Map data) {
        data.each { k, parents ->
            def s = java.util.concurrent.ConcurrentHashMap.newKeySet()
            s.addAll(parents)
            this.child2parent[k] = s
        }
    }

    private synchronized def registerIndexHistory(String name, Map index) {
        def hist = this.index_history.get(name, Collections.synchronizedList(new ArrayList())) // sets if $name not in index_history
        hist.add(index)
    }

    public List _post(streams, names, slot_ids) {
        // streams is a list of each of the channels produced:
        // output:
        //      tuple val(index),path("*i") <- stream 1
        //      tuple val(index),path("*j") <- stream 2
        // the index is then copied for multiple files.
        //
        // The on-channel per-file identity is md5("<slot_id>::<filename>"),
        // byte-identical to the canonical off-channel file id
        // (LinPayload.mint_file_id in Python). slot_ids[i] is the compile-time
        // slot identity threaded in by the generator (workflow.py). When it is
        // absent (null/empty) the channel name stands in, so the id stays
        // deterministic and per-file for direct/test callers.
        //
        // One incoming tuple is one member's whole delivery into this slot,
        // so its size is the sibling count the stamp carries. Nextflow emits
        // a task's outputs once, on success, and a member runs in exactly one
        // of a process and its `_cached` twin, so no second tuple for the
        // same member can follow.
        names.each { n -> this._countProducer(n as String) }
        return [names, streams, slot_ids].transpose().collect((name, stream, slot_id) -> {
            def sid = (slot_id == null || slot_id == "") ? name : slot_id
            return new Tuple2(
                name,
                stream.flatMap((index, group) -> {
                    if (!(group instanceof List)) {
                        group = [group]
                    }
                    def stamp_by = this._groupByOf(name as String)
                    def n = group.size()
                    return (0..<n).collect((i) -> {
                        def item = group[i]
                        // The batch position is where the member sat in the
                        // task that ran it, not part of what the file is.
                        // Mirrors LinPayload.canonical_output_name.
                        def cname = item.name.replaceFirst(/^\d+-/, "1-")
                        def v = "${sid}::${cname}".md5()
                        def out = [:] + index
                        out[name] = [v]
                        out.remove(SIBS_KEY)
                        if (stamp_by != null) out[SIBS_KEY] = [stamp_by, n, i]
                        this.registerIndexHistory(name, out)
                        return [out, item]
                    })
                })
            )
        })
    }

    public List post(streams, names, slot_ids = null) {
        def sids = (slot_ids == null) ? names.collect { null } : slot_ids
        return this._post(_debatch(streams), names, sids)
    }

    public List postIn(streams, names) {
        // Leaves (given inputs). The per-file identity is the leaf's canonical
        // instance_id, threaded in from the Python given-lineage seed under
        // SELF_ID_KEY and relocated here to index[name] — byte-identical to
        // the off-channel instance_id (single point of provenance). Direct/test
        // callers that pass no seed fall back to the full-path md5 so the id
        // stays deterministic and per-file.
        names.each { n -> this._countProducer(n as String) }
        return [names, streams].transpose().collect((name, stream) -> {
            return new Tuple2(
                name,
                stream.flatMap((index, group) -> {
                    if (!(group instanceof List)) {
                        group = [group]
                    }
                    return group.collect((item) -> {
                        index = [:]+index // copy the hashmap
                        index.remove(SIBS_KEY)
                        def self_id = index.remove(SELF_ID_KEY)
                        index[name] = (self_id != null) ? self_id : ["$item".md5()]
                        this.registerIndexHistory(name, index)
                        return [index, item]
                    })
                })
            )
        })
    }

    // Replaces `[*process_call(...)]` in generated workflow.nf. Nextflow
    // 26's strict parser rejects the spread-in-list-literal form, so the
    // generator emits `o.asStreams(process_call(...))` instead. Handles both
    // single-output processes (returns a Channel) and multi-output ones
    // (returns an iterable ChannelOut).
    //
    // NOTHING INSERTED BETWEEN HERE AND _debatch MAY WRITE TO AN INDEX. The
    // streams returned by a multi-output process all carry the SAME index
    // object, so an operator added here -- a .map{} that stamps a key, a
    // .view{} that sorts a value for printing -- runs once per stream on its
    // own thread over one shared map, and the losing thread's product is
    // silently dropped downstream. Read freely; copy before you write.
    public List asStreams(out) {
        if (out instanceof Iterable) {
            def result = []
            for (ch in out) result << ch
            return result
        }
        return [out]
    }

    private def combineIndexes(indexes) {
        def combined_index = [:]
        // Reserved keys hold nested structures, not hash lists; unioning them
        // would be meaningless. Unreachable today (they are stripped before
        // anything re-enters here), but it makes "an index value is a list of
        // hashes" true locally instead of true by argument elsewhere.
        def keys = indexes.inject([:].keySet(), (result, i) -> result+i.keySet()) - RESERVED_KEYS // reduce
        for (key : keys) {
            // if any is missing, use the remainder
            // if remainder different, skip
            // if remainder same, add
            // def candidates = 
            combined_index[key] = indexes
            .collect(index -> index[key])
            .inject([], (all, values) -> values==null? all : all+values)
            .unique() // collect == map, findAll == filter
            // if (candidates.size()==1) {
            // } else if (candidates.size()>1) {
            //     combined_index[key] = 0
            // }
        }
        return combined_index
    }

    // One stream's bags inside one group() call, keyed by the join key's
    // hash, and the early-release bookkeeping over them. A key completes when
    // its stamped ordinals are exactly 0..n-1; anything that could make that
    // count wrong (an unstamped item, a second n, an ordinal out of range or
    // repeated by another file) poisons the key, and a poisoned key waits for
    // the close-flush. Counting ordinals rather than bag size means a
    // duplicate cannot stand in for a missing file.
    //
    // A completed key takes no further item. One that arrives anyway was
    // missing from a group already sent downstream, so it throws: the stamp
    // made a promise the data broke, and continuing would be a quietly wrong
    // result.
    static class ReleaseBags {
        final String stream
        final String key
        private final Map items = [:]
        private final Map seen = [:]
        private final Map ordinals = [:]
        private final Map sizes = [:]
        private final Set poisoned = new HashSet()
        private final Set complete = new HashSet()

        ReleaseBags(String stream, String key) {
            this.stream = stream
            this.key = key
        }

        // True when this item completes h.
        synchronized boolean add(h, item, String item_hash, List stamp) {
            if (this.complete.contains(h)) {
                throw new LineageViolation(
                    "stream [${this.stream}] delivered ${item[-1]} for [${this.key}] "
                    + "${h} after that key was released early as complete. Its "
                    + "sibling stamp promised every item of the key had arrived, "
                    + "so this one is missing from a group already sent "
                    + "downstream. Something upstream delivered one member twice."
                )
            }
            if (!this.seen.computeIfAbsent(h, x -> new HashSet()).add(item_hash)) return false
            this.items.computeIfAbsent(h, x -> []).add(item)
            if (this.poisoned.contains(h)) return false
            if (stamp == null) {
                this.poisoned.add(h)
                return false
            }
            def n = stamp[1]
            def i = stamp[2]
            if (!(n instanceof Integer) || !(i instanceof Integer) || i < 0 || i >= n) {
                this.poisoned.add(h)
                return false
            }
            def prev_n = this.sizes.putIfAbsent(h, n)
            def ords = this.ordinals.computeIfAbsent(h, x -> new HashSet())
            if ((prev_n != null && prev_n != n) || !ords.add(i)) {
                this.poisoned.add(h)
                return false
            }
            if (ords.size() == n) {
                this.complete.add(h)
                return true
            }
            return false
        }

        synchronized boolean isComplete(h) {
            return this.complete.contains(h)
        }

        synchronized List bag(h) {
            return new ArrayList(this.items.getOrDefault(h, []))
        }

        // Every key the close-flush still owes, with its bag.
        synchronized Map incomplete() {
            return this.items
                .findAll((h, v) -> !this.complete.contains(h))
                .collectEntries((h, v) -> [h, new ArrayList(v)])
        }
    }

    private boolean isParent(String parent, String child) {
        if (parent==child) return false
        if (!(child in this.child2parent)) return false
        def parents = this.child2parent[child]
        if (parent in parents) return true
        return parents.any(p -> this.isParent(parent, p))
    }

    // Walk the parent graph from `name` and collect every transitive ancestor.
    private Set _collectAncestors(String name) {
        def result = new HashSet()
        def visited = new HashSet()
        def stack = [name]
        while (!stack.isEmpty()) {
            def cur = stack.remove(stack.size() - 1)
            if (visited.contains(cur)) continue
            visited.add(cur)
            def parents = this.child2parent.get(cur)
            if (parents == null) continue
            parents.each((p) -> {
                result.add(p)
                stack.add(p)
            })
        }
        return result
    }

    // The shared ancestor a SIBLING join keys on: one no other shared
    // ancestor descends from, ties broken by name. A higher ancestor groups
    // too coarsely (every sample under one project), and set iteration order
    // made the old pick differ between runs. Null when there is none.
    private String _nearestSharedAncestor(String a, String b) {
        if (a == b) return null
        def common = this._collectAncestors(a).intersect(this._collectAncestors(b))
        def nearest = common.findAll(c -> !common.any(d -> d != c && this.isParent(c, d)))
        return nearest.isEmpty() ? null : nearest.sort()[0]
    }

    // Classify a non-by stream's lineage relationship to the by-stream. Each
    // class drives a distinct dispatch path inside group(). Construction-time
    // (one call per stream per group() invocation), not per-item.
    private String classify(String name, String by_name) {
        if (this.isParent(name, by_name)) return "PARENT_OF_BY"
        if (this.isParent(by_name, name)) return "DESCENDANT_OF_BY"
        if (this._nearestSharedAncestor(name, by_name) != null) return "SIBLING"
        return "WILDCARD"
    }

    // A key leaves the moment it is provably whole, by the sibling stamps
    // (see ReleaseBags and _releaseStamp), and otherwise when its stream
    // closes. The close-flush through the `one_null` sentinel is
    // unconditional, and it has to be: `errorStrategy 'ignore'` is
    // process-wide (local.nf, slurm.nf), so a dropped task means a key whose
    // stamps never complete, and that must degrade to a late flush rather
    // than a hang.
    private def _grouped(by, streams, targets) {
        def parents = streams.collect((k, s) -> k) as Set
        for (t : targets) {
            def existing = this.child2parent.get(t, java.util.concurrent.ConcurrentHashMap.newKeySet())
            existing.addAll(parents)
            this.child2parent[t] = existing
            this._recordGroupBy(t as String, by as String)
        }

        def original_order = streams.collect(s -> s[0]).withIndex().collectEntries((item, i) -> [item, i])
        def by_channel = streams.find(s -> s[0]==by)
        def (by_name, by_stream) = by_channel
        // println("g $by_name")

        def to_group = streams.findAll(stream -> {
            def (name, _stream) = stream
            return name!=by_name
        })

        // Classify each non-by stream once, up front. The result drives the
        // dispatch branch each stream takes in `to_group.collect` below.
        // Every branch emits ONE tuple per by-key carrying that key's whole
        // list of matched items — see the contract note on `_batch`.
        def stream_relations = [:]
        to_group.each { stream ->
            def (s_name, s_chan) = stream
            stream_relations[s_name] = this.classify(s_name, by_name)
        }

        def by_parsed = by_stream.map(item -> {
            def (index, value) = item
            def group_k = index[by_name]
            return [
                new Tuple3(group_k, by_name, [new Tuple2(index, value)])
            ]
        })

        return to_group
        .collect((stream) -> {
            def (name, _stream) = stream
            def relation = stream_relations[name]
            this._logDispatch(name, relation, null, null)

            if (relation == "PARENT_OF_BY") {
                // PARENT branch (unchanged): combine(by:0) on the parent's own
                // hash, which by-items carry as idx[parent_name].
                return _stream.map((item) -> {
                    def (_index, _value) = item
                    def k = _index[name]
                    return new Tuple2(k, item)
                })
                .combine(by_stream.map((item) -> {
                    def (_index, _value) = item
                    def k = _index[name]
                    return new Tuple2(k, _index[by]) // pass through the "by index"
                }), by: 0)
                .map((combined) -> {
                    def (_, item, key) = combined
                    return [new Tuple3(key, name, [item])]
                })
            }

            if (relation == "DESCENDANT_OF_BY") {
                // DESCENDANT branch: S items carry the by-hashes they descend
                // from in idx[by_name]. Mirror of the PARENT branch with the
                // join key inverted, then combine(by:0) against by_stream's
                // own hash.
                //
                // Items are AGGREGATED per by-hash before the join, so the
                // branch emits one tuple per key carrying that key's whole
                // list. Emitting one tuple per (key, item) — as this branch
                // did between bbbb599 and the fix — multiplies through the
                // cartesian `inject` fold below and forces `_batch` to try to
                // reassemble the group afterwards, which is what shattered a
                // collecting transform's input into singletons.
                def _name = name
                def bags = new ReleaseBags(_name as String, by_name as String)
                return _stream.concat(this.one_null)
                .flatMap((item) -> {
                    if (item == null) {
                        return bags.incomplete().collect((h, items) -> new Tuple2([h], items))
                    }
                    def (_index, _value) = item
                    def by_hashes = _index[by_name]
                    // An ABSENT key and an EMPTY list are the same defect and
                    // are treated the same way. The empty list is the more
                    // dangerous of the two: `by_hashes.each` below iterates
                    // zero times, so before this check the item vanished
                    // without even reaching the dispatch log -- which is why
                    // the run that hit it reported no violations logged while
                    // items were disappearing.
                    if (by_hashes == null || by_hashes.size() == 0) {
                        this._logDispatch(_name, "LINEAGE_VIOLATION", null, null)
                        throw new LineageViolation(
                            "stream [${_name}] is a declared descendant of "
                            + "[${by_name}], so every item must carry "
                            + "[${by_name}] in its index, but [${_value}] "
                            + "arrived with ${_renderLineage(_index)}. "
                            + "Grouping it would drop it, and a dropped item "
                            + "silently truncates the DAG. Fix the producer of "
                            + "[${_name}] so it propagates its input index."
                        )
                    }
                    def item_hash = "$_value".md5()
                    def stamp = this._releaseStamp(_name as String, by_name as String, _index)
                    def ready = []
                    by_hashes.each((h) -> {
                        if (bags.add(h, item, item_hash, stamp)) {
                            ready.add(new Tuple2([h], bags.bag(h)))
                        }
                    })
                    return ready
                })
                .combine(by_stream.map((item) -> {
                    def (_index, _value) = item
                    def by_h = _index[by_name]
                    if (by_h == null || by_h.size() != 1) {
                        return new Tuple2([], _index[by])
                    }
                    return new Tuple2([by_h[0]], _index[by])
                }), by: 0)
                .map((combined) -> {
                    def (_, items, key) = combined
                    return [new Tuple3(key, _name, items)]
                })
            }

            if (relation == "SIBLING") {
                // SIBLING branch: S and the by-stream share an ancestor, and a
                // by-item takes every S item sharing one of its ancestor
                // hashes. Both sides go through ONE operator so each by-item
                // is matched once, over the union of its ancestors' bags. A
                // coassembly carries every sample's reads hash; joining it
                // once per hash, as a combine(by:0) on the ancestor hash did,
                // made one partial member per sample under one key.
                //
                // A by-item leaves once every one of its ancestor hashes is
                // complete by the sibling stamps, otherwise at close. The key
                // it leaves under is its own hash, which the fold below
                // matches against by_parsed.
                def anc_key = this._nearestSharedAncestor(name as String, by_name as String)
                def _name = name
                def bags = new ReleaseBags(_name as String, anc_key)
                def waiting = []
                def waiting_on = [:]
                def member = (Map b) -> {
                    def union = [:]
                    b.hashes.each((h) -> bags.bag(h).each((x) -> union.putIfAbsent("${x[-1]}".md5(), x)))
                    return new ArrayList(union.values())
                }
                def release = (List candidates) -> {
                    def out = []
                    candidates.each((b) -> {
                        if (b.done || !b.hashes.every((h) -> bags.isComplete(h))) return
                        b.done = true
                        out.add([new Tuple3(b.key, _name, member(b))])
                    })
                    return out
                }
                return _stream.map((x) -> ["S", x])
                .mix(by_stream.map((x) -> ["B", x]))
                .concat(this.one_null)
                .flatMap((msg) -> {
                    if (msg == null) {
                        return waiting
                        .findAll((b) -> !b.done)
                        .collect((b) -> new Tuple2(b, member(b)))
                        .findAll((bm) -> bm[1].size() > 0)
                        .collect((bm) -> [new Tuple3(bm[0].key, _name, bm[1])])
                    }
                    def (side, item) = msg
                    def (_index, _value) = item
                    def anc_hashes = _index[anc_key]
                    if (side == "B") {
                        if (anc_hashes == null || anc_hashes.size() == 0) return []
                        def b = [key: _index[by], hashes: new ArrayList(anc_hashes), done: false]
                        waiting.add(b)
                        anc_hashes.each((h) -> waiting_on.computeIfAbsent(h, (x) -> []).add(b))
                        return release([b])
                    }
                    // Logged, not thrown, unlike DESCENDANT_OF_BY above: an
                    // item can relate to the by-stream through a shared
                    // ancestor other than the nearest one keyed on here. The
                    // drop is still worth seeing, because a stream that drops
                    // every item is the same truncated DAG wearing a weaker
                    // relation.
                    if (anc_hashes == null || anc_hashes.size() == 0) {
                        this._logDispatch(_name, "LINEAGE_VIOLATION", null, null)
                        return []
                    }
                    def item_hash = "$_value".md5()
                    def stamp = this._releaseStamp(_name as String, anc_key, _index)
                    def completed = anc_hashes.findAll((h) -> bags.add(h, item, item_hash, stamp))
                    return release(completed.collectMany((h) -> waiting_on.getOrDefault(h, [])))
                })
            }

            // WILDCARD (relation == "WILDCARD"): no lineage relationship
            // between this stream and the by-stream. Cartesian fan-out is
            // semantically correct here — every by-item is paired with
            // every S-item — so we must wait for the upstream to close.
            // Bag-insertion dedup guards against retry/replay duplication.
            def pending_groups = [:]
            def seen_per_group = [:]
            return _stream.concat(this.one_null)
            .flatMap((item) -> {
                if (item==null) {
                    return pending_groups
                    .collect((key, value) -> {
                        return new Tuple3(key, name, value)
                    })
                } else {
                    def (index, value) = item
                    def group_k = index[by_name]
                    def item_hash = "$value".md5()
                    def seen_for_group = seen_per_group.get(group_k, new HashSet())
                    if (seen_for_group.contains(item_hash)) {
                        return []
                    }
                    seen_for_group.add(item_hash)
                    seen_per_group[group_k] = seen_for_group
                    def group = pending_groups.get(group_k, [])
                    group.add(new Tuple2(index, value))
                    pending_groups[group_k] = group
                    return []
                }
            })
            .map(x -> [x])
        })
        .inject(by_parsed, (result, channel) -> { // reduce (to channel)
            // cant use ${combine(by: 0)} since when k not in index,
            // it should be treated as wildcard, not a specific value
            // x = Channel.fromList([[['a', 1]], [['a', 2]]])
            // y = Channel.fromList([[['b', 3]], [['b', 4]]])
            // x.combine(y).view()
            // [['a', 1], ['b', 3]]
            // [['a', 1], ['b', 4]]
            // [['a', 2], ['b', 3]]
            // [['a', 2], ['b', 4]]
            return result
            .combine(channel)
            .filter((_result) -> {
                def keys = _result
                .collect(x -> x[0])
                .findAll(x -> x!=null)
                if (keys.size()==0) return true
                // use intersection instead of equality to handle aggregate-then-distribute patterns
                // where a merged item carries all sample hashes but each individual item carries only its own
                def common = keys.inject(keys[0] as Set, (acc, k) -> acc.intersect(k as Set))
                return common.size() > 0
            })
        })
        // .view(v -> by_name=='b'? "^ $v" : null)
        .map((_result) -> { // we are a channel now, so we can map()
            // each channel is [key, name, group]
            _result = _result.sort((a, b) -> { // back to original order
                return original_order[a[1]] <=> original_order[b[1]]
            })
            def groups = _result.collect(channel -> channel[-1]) 
            groups.collect(channel -> channel.collect(xx -> {
                def (key, name, gg) = xx
                // println(" . $by_name // $key // $name // $gg")
            }))
            // Per-item indexes, kept UN-flattened. combineIndexes unions and
            // uniques these into one map, which is the right answer for the
            // task's own lineage and destroys the only record of which item
            // came from where. Built from the same `groups` in the same
            // closure as `values` below, so PROV[s][i] describes values[s][i]
            // by construction rather than by an ordering to maintain.
            // Defensive copy: these maps are the ones _post handed out and are
            // also held by index_history, and the task serialises them -- the
            // shared-collection race the .view/formatMap trap is made of.
            // stripReserved makes the copy and drops the sibling stamp, which
            // the cache member key reads PROV for and must never see.
            def per_item = groups.collect(channel -> channel.collect(group -> stripReserved(group[0])))
            def common_index = this.combineIndexes(per_item.flatten())
            def values = groups.collect(channel -> channel.collect(group -> group[-1]))
            common_index[PROV_KEY] = per_item
            return [common_index, *values]
        })
    }

    public def group(by, streams, targets, batch_size) {
        return this._batch(batch_size, this._grouped(by, streams, targets))
    }

    // The cached form. `cache` names the step for the key helper:
    //   tk, sig      the transform key and signature the key folds
    //   slk          the channel name of each required slot, in slot order
    //   cache_root   where the shards are, in this process's coordinates
    //   cacheable    false sends every member to the real process
    //   helper       the command that runs metasmith.caching.invocation
    //   hits_log     the file one JSON line per hit member is appended to
    //   step, step_name
    // Returns [misses, hits]: two batched channels in the shape _batch
    // emits. A miss batch feeds the real process; a hit batch feeds its
    // `_cached` twin as [indexes, sources], where each source is
    // [position, shard file, name without its position].
    public def group(by, streams, targets, batch_size, Map cache) {
        return this._route(batch_size, this._grouped(by, streams, targets), cache)
    }

    // One helper call per batch decides every member of it. The Python side
    // is the only key implementation; this side only carries its verdict.
    // A member whose row is missing, a helper that fails, or a non-zero exit
    // is a miss: the cost of a wrong miss is compute, the cost of a wrong hit
    // is a wrong result.
    public static List probeMembers(helper, Map spec) {
        def json = JsonOutput.toJson(spec)
        def proc = new ProcessBuilder(helper as List<String>).start()
        proc.outputStream.withWriter("UTF-8") { w -> w << json }
        def out = new StringBuilder()
        def err = new StringBuilder()
        proc.waitForProcessOutput(out, err)
        if (proc.exitValue() != 0) {
            throw new RuntimeException("cache helper failed (${proc.exitValue()}): ${err}")
        }
        def rows = out.toString().split("\n").findAll(l -> l.trim().size() > 0).collect(l -> {
            def parts = l.trim().split(/\|/, 3) as List
            while (parts.size() < 3) parts << ""
            return parts
        })
        if (rows.size() != spec.members.size()) {
            throw new RuntimeException("cache helper answered ${rows.size()} rows for ${spec.members.size()} members")
        }
        return rows
    }

    private def _decide(batch, cache) {
        def members = batch.collect(item -> item[0])
        def rows = null
        if (cache.cacheable == true) {
            try {
                rows = probeMembers(cache.helper, [
                    tk: cache.tk, sig: cache.sig, slk: cache.slk,
                    cache_root: cache.cache_root, members: members,
                ])
            } catch (Exception e) {
                System.err.println("[metasmith] step ${cache.step} (${cache.step_name}): ${e.message}; running every member")
                rows = null
            }
        }
        return [batch, (0..<batch.size())].transpose().collect((item, i) -> {
            def index = [:] + item[0]
            def row = (rows == null) ? ["-", "-", ""] : rows[i]
            index[KEY_KEY] = row[0]
            def hit = (row[1] == "hit")
            if (hit) this._logHit(cache, row[0], row[2], index)
            return [item: [index, *item[1..-1]], hit: hit, shard: row[2]]
        })
    }

    private synchronized void _logHit(cache, key, shard, index) {
        if (cache.hits_log == null) return
        def f = new File(cache.hits_log as String)
        f.parentFile?.mkdirs()
        f << JsonOutput.toJson([
            step: cache.step, step_name: cache.step_name, key: key, shard: shard,
            entry: index.findAll((k, v) -> k != FILES_KEY),
        ]) << "\n"
    }

    private def _collateHits(rows) {
        def collated = this._collateBatch(rows.collect(r -> r.item))
        def indexes = collated[0]
        def sources = []
        rows.eachWithIndex { r, i ->
            def out = new File(r.shard as String, "out")
            def files = out.listFiles() ?: []
            files.sort { a, b -> a.name <=> b.name }.each { f ->
                sources << [i + 1, f.absolutePath, f.name.replaceFirst(/^\d+-/, "")]
            }
        }
        return [indexes, sources]
    }

    public def _route(size, channel, cache) {
        def decided = channel.collate(size).map(batch -> this._decide(batch, cache))
        def misses = decided
            .map(rows -> rows.findAll(r -> !r.hit))
            .filter(rows -> rows.size() > 0)
            .map(rows -> this._collateBatch(rows.collect(r -> r.item)))
        def hits = decided
            .map(rows -> rows.findAll(r -> r.hit))
            .filter(rows -> rows.size() > 0)
            .map(rows -> this._collateHits(rows))
        return [misses, hits]
    }

    // The outputs of a process and of its `_cached` twin, one mixed channel
    // per output slot, so downstream sees one producer.
    public List mixOuts(a, b) {
        return [a, b].transpose().collect((x, y) -> x.mix(y))
    }

    private def _collateBatch(batch) {
        def streams = batch.collect(item -> {
            def index = [:]+item[0] // copy to avoid mutating shared state
            def values = item[1..-1]
            index[FILES_KEY] = values.collect(group -> group*.toString())
            return [index, *values]
        }).transpose()
        def indexes = streams[0]
        // careful, this unique() could remove real file collisions as well!
        // this is needed for cases where reference dbs are passed multiple times per batch
        def values = streams[1..-1].collect(stream -> stream.flatten().unique())
        return [indexes, *values]
    }

    // `size` is the GROUP-COUNT axis, never the within-group member count:
    // every branch of group() emits one result per by-key holding that key's
    // whole list, so collating `size` of them folds `size` whole groups into
    // one task. This is what `batch_size` means everywhere else in the system
    // — `plan_oracle` predicts `ceil(len(group_by_instances) / batch_size)`
    // tasks, `cache_decisions` and `virtual_runtime` both chunk
    // `group_by_instances` by it, and `checkm`/`gtdbtk` pair `group_by=asm`
    // with `batch_size=25`/`100` while iterating `context.AsBatch()`.
    public def _batch(size, channel) {
        return channel.collate(size).map(batch -> this._collateBatch(batch))
    }

    public def _debatch(streams) {
        // streams is a list of each of the channels produced:
        // output:
        //      tuple val(index),path("*i") <- stream 1
        //      tuple val(index),path("*j") <- stream 2
        // * this is identical to _post()
        return streams.collect(stream -> {
            return stream.flatMap((indexes, bag) -> {
                // since process was batched, bag is a mix of groups and batches
                // while index is a list of indexes
                def is_batched = indexes instanceof List
                indexes = is_batched ? indexes : [indexes]
                indexes = indexes.collect(index -> Orchestrator.stripReserved(index))
                bag = (bag instanceof List)? bag : [bag]
                if (!is_batched) {
                    // Non-batched: return the single item directly without numeric-prefix parsing
                    return [new Tuple2(indexes[0], bag.size() == 1 ? bag[0] : bag)]
                }
                def batches = bag.groupBy(path -> {
                    return (path.name.split('-', 2)[0] as Integer) - 1
                })
                // println("${bag.collect(x -> x.name)}")
                return batches.collect((i, group) -> {
                    return new Tuple2(indexes[i], group)
                })
            })
        })
    }

    // The mixed stream carries the first stream's name but every stream's
    // items, so that name's producer count no longer describes it.
    public def mix(streams) {
        def (name, _) = streams[0]
        this._assertUnsealed("mix into [${name}]")
        if (streams.size() > 1) this.mixed_streams.add(name as String)
        return new Tuple2(
            name,
            streams
            .collect((_name, _stream) -> _stream)
            .inject((result, _stream) -> {
                return result.mix(_stream)
            })
        )
    }

    // public def unify(streams) {
    //     return streams
    //     .collect((stream) -> { // map
    //         def (name, _stream) = stream
    //         return _stream.collect(flat: false).map(x -> [x])
    //         // .view(v -> "  .${v}")

    //     })
    //     .inject((result, channel) -> { // reduce (to channel)
    //         return result
    //         .combine(channel)
    //         // .view(v -> "  .${v}")
    //     })
    //     .map((_result) -> {
    //         def indexes = _result.collect(channel -> channel.collect(item -> item[0])).flatten()
    //         def values = _result.collect(channel -> channel.collect(item -> item[-1]))
    //         return [this.combineIndexes(indexes), *values]
    //     })
    // }

    // public def xross(streams) {
    //     return streams
    //     .collect((stream) -> { // map
    //         def (name, _stream) = stream
    //         return _stream
    //         .map(item -> [item])

    //     })
    //     .inject((result, channel) -> { // reduce (to channel)
    //         return result
    //         .combine(channel)
    //     })
    //     .map((_result) -> {
    //         def indexes = _result.collect(item -> item[0])
    //         def values = _result.collect(item -> [item[-1]])
    //         return [combineIndexes(indexes), *values]
    //     })
    // }

    public static String JsonforEcho(map) {
        return JsonOutput.toJson(map).replace(/"/,"\\\"")    
    }

    public def publish(stream) {
        def (name, _stream) = stream
        return _stream.map((index, item) -> {
            return new Tuple2(JsonOutput.toJson(stripReserved(index)), item)
        })
    }
}
