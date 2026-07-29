import groovy.json.JsonOutput

class Orchestrator {
    // Reserved index key carrying a leaf's canonical instance_id from the
    // Python given-lineage seed. postIn relocates it to index[<name>] and
    // strips it, so it never propagates as a lineage key. Kept in lockstep
    // with workflow.py's given-seed (SELF_ID_KEY).
    public static final String SELF_ID_KEY = "__self__"

    private Map index_history
    private Map child2parent
    private def one_null
    private List _dispatchLog

    Orchestrator(one_null) {
        this.index_history = new java.util.concurrent.ConcurrentHashMap()
        this.child2parent = new java.util.concurrent.ConcurrentHashMap()
        this.one_null = one_null
        this._dispatchLog = Collections.synchronizedList(new ArrayList())
    }

    public List getDispatchLog() {
        return new ArrayList(this._dispatchLog)
    }

    private void _logDispatch(name, relation, by_hash, item_hash) {
        this._dispatchLog.add([name, relation, by_hash, item_hash])
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
        return [names, streams, slot_ids].transpose().collect((name, stream, slot_id) -> {
            def sid = (slot_id == null || slot_id == "") ? name : slot_id
            return new Tuple2(
                name,
                stream.flatMap((index, group) -> {
                    if (!(group instanceof List)) {
                        group = [group]
                    }
                    return group.collect((item) -> { // map
                        def v = "${sid}::${item.name}".md5()
                        // println("post: <$name> $v $item")
                        index = [:]+index // copy the hashmap
                        index[name] = [v]
                        this.registerIndexHistory(name, index)
                        return [index, item]
                    })
                    // println("post: $k $hist")
                    // return x
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
        return [names, streams].transpose().collect((name, stream) -> {
            return new Tuple2(
                name,
                stream.flatMap((index, group) -> {
                    if (!(group instanceof List)) {
                        group = [group]
                    }
                    return group.collect((item) -> {
                        index = [:]+index // copy the hashmap
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
        def keys = indexes.inject([:].keySet(), (result, i) -> result+i.keySet()) // reduce
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

    private class SynchronizedHashGroup {
        private Map map
        private Map seen

        SynchronizedHashGroup() {
            this.map = [:]
            this.seen = [:]
        }

        // Returns true if (k, dedup_key) was newly inserted; false if it was
        // already present. When dedup_key is null, no dedup is performed and
        // every register call inserts.
        public synchronized boolean register(k, v, dedup_key) {
            if (dedup_key != null) {
                def seen_set = this.seen.get(k, new HashSet())
                if (seen_set.contains(dedup_key)) {
                    return false
                }
                seen_set.add(dedup_key)
                this.seen[k] = seen_set
            }
            this.map[k] = this.map.get(k, [])+[v]
            return true
        }

        public synchronized def get(k) {
            def cur = this.map[k]
            return cur == null ? [] : new ArrayList(cur)
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

    // Find the first shared-ancestor key between two streams (used to key the
    // SIBLING dispatch join). Returns null if there is no shared ancestor.
    private String _firstSharedAncestor(String a, String b) {
        if (a == b) return null
        def anc_a = this._collectAncestors(a)
        if (anc_a.isEmpty()) return null
        def anc_b = this._collectAncestors(b)
        def common = anc_a.intersect(anc_b)
        if (common.isEmpty()) return null
        return common.iterator().next()
    }

    // Classify a non-by stream's lineage relationship to the by-stream. Each
    // class drives a distinct dispatch path inside group(). Construction-time
    // (one call per stream per group() invocation), not per-item.
    private String classify(String name, String by_name) {
        if (this.isParent(name, by_name)) return "PARENT_OF_BY"
        if (this.isParent(by_name, name)) return "DESCENDANT_OF_BY"
        if (this._firstSharedAncestor(name, by_name) != null) return "SIBLING"
        return "WILDCARD"
    }

    public def group(by, streams, targets, batch_size) {
        def parents = streams.collect((k, s) -> k) as Set
        for (t : targets) {
            def existing = this.child2parent.get(t, java.util.concurrent.ConcurrentHashMap.newKeySet())
            existing.addAll(parents)
            this.child2parent[t] = existing
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

        return _batch(batch_size, to_group
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
                def pending_items = [:]
                def seen_per_key = [:]
                return _stream.concat(this.one_null)
                .flatMap((item) -> {
                    if (item == null) {
                        // Upstream closed: flush one tuple per accumulated key.
                        return pending_items.collect((h, items) -> new Tuple2([h], items))
                    }
                    def (_index, _value) = item
                    def by_hashes = _index[by_name]
                    if (by_hashes == null) {
                        // Lineage violation: stream is declared as a
                        // descendant of by_name but the item's index
                        // doesn't carry by_name. Log and drop.
                        this._logDispatch(_name, "LINEAGE_VIOLATION", null, null)
                        return []
                    }
                    // Bag-insertion dedup guards against retry/replay
                    // duplication, matching the WILDCARD branch.
                    def item_hash = "$_value".md5()
                    by_hashes.each((h) -> {
                        def seen_for_key = seen_per_key.get(h, new HashSet())
                        if (seen_for_key.contains(item_hash)) return
                        seen_for_key.add(item_hash)
                        seen_per_key[h] = seen_for_key
                        def group = pending_items.get(h, [])
                        group.add(item)
                        pending_items[h] = group
                    })
                    return []
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
                // SIBLING branch: join on a shared ancestor's hash. Both sides
                // key on idx[anc_key] and combine(by:0). The final key emitted
                // is the by-item's own hash, so the downstream set-overlap
                // filter still matches by_parsed cleanly.
                //
                // Aggregated per ancestor hash for the same reason as
                // DESCENDANT_OF_BY above.
                def anc_key = this._firstSharedAncestor(name, by_name)
                def _name = name
                def pending_items = [:]
                def seen_per_key = [:]
                return _stream.concat(this.one_null)
                .flatMap((item) -> {
                    if (item == null) {
                        return pending_items.collect((h, items) -> new Tuple2([h], items))
                    }
                    def (_index, _value) = item
                    def anc_hashes = _index[anc_key]
                    if (anc_hashes == null) return []
                    def item_hash = "$_value".md5()
                    anc_hashes.each((h) -> {
                        def seen_for_key = seen_per_key.get(h, new HashSet())
                        if (seen_for_key.contains(item_hash)) return
                        seen_for_key.add(item_hash)
                        seen_per_key[h] = seen_for_key
                        def group = pending_items.get(h, [])
                        group.add(item)
                        pending_items[h] = group
                    })
                    return []
                })
                .combine(by_stream.flatMap((item) -> {
                    def (_index, _value) = item
                    def anc_hashes = _index[anc_key]
                    if (anc_hashes == null) return []
                    return anc_hashes.collect((h) -> new Tuple2([h], _index[by]))
                }), by: 0)
                .map((combined) -> {
                    def (_, items, key) = combined
                    return [new Tuple3(key, _name, items)]
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
            def common_index = this.combineIndexes(groups.collect(channel -> channel.collect(group -> group[0])).flatten())
            def values = groups.collect(channel -> channel.collect(group -> group[-1]))
            return [common_index, *values]
        }))
    }

    private def _collateBatch(batch) {
        def streams = batch.collect(item -> {
            def index = [:]+item[0] // copy to avoid mutating shared state
            def values = item[1..-1]
            index['FILES'] = values.collect(group -> group*.toString())
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
                indexes = indexes.collect(index -> {
                    index.remove('FILES')
                    return index
                })
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

    public def mix(streams) {
        def (name, _) = streams[0]
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
            return new Tuple2(JsonOutput.toJson(index), item)
        })
    }
}
