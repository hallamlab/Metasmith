import groovy.json.JsonOutput

class Orchestrator {
    private Map pending_tasks
    private Map index_history
    private Map child2parent
    private def one_null
    private List _dispatchLog

    Orchestrator(one_null) {
        this.pending_tasks = new java.util.concurrent.ConcurrentHashMap()
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

    private synchronized def registerPendingTarget(String target, Map index) {
        // println("  <<ADD $target // $index")
        def pending_targets = this.pending_tasks.get(target, java.util.concurrent.ConcurrentHashMap.newKeySet()) // this also sets if not exist
        pending_targets.add(index)
    }

    private synchronized def removePendingTarget(String target, Map index) {
        if (!(target in this.pending_tasks)) return
        def pending_targets = this.pending_tasks[target]
        if (pending_targets==null) return
        // println("  - rm $target // $index // $pending_targets")
        pending_targets.remove(index)
        // // use the linage in the index to figure out which inputs were
        // // used to produce the output (@index) and remove these from pending_tasks
        // pending_targets = pending_targets.collect((candidate_index) -> {
        //     for (c : candidate_index) {
        //         if (!(c.key in index) || !index[c.key].containsAll(c.value)) {
        //             return null
        //         }
        //     }
        //     return candidate_index
        // })
        // .findAll(x -> x!=null)
        if (pending_targets.size()==0) {
            this.pending_tasks.remove(target)
        } else {
            this.pending_tasks[target] = pending_targets
        }
    }

    private synchronized def registerIndexHistory(String name, Map index) {
        def hist = this.index_history.get(name, Collections.synchronizedList(new ArrayList())) // sets if $name not in index_history
        hist.add(index)
    }

    private synchronized def getExpectedSize(String grouping_by, String name, group_ks) {
        def pending_targets = this.pending_tasks[name]
        def size_valid = pending_targets==null? true : pending_targets.collect(i -> i[grouping_by]).every(ks -> ks.every(k -> !(k in group_ks)))
        def expected_size = !size_valid? -1 : this.index_history[name].collect(i -> i[grouping_by]).findAll(ks -> ks.any(k -> (k in group_ks))).size()
        // println("  * $grouping_by $name $group_ks // $size_valid/$expected_size // $pending_targets")
        return new Tuple2(size_valid, expected_size)
    }

    public List _post(streams, names, fullHash) {
        // streams is a list of each of the channels produced:
        // output:
        //      tuple val(index),path("*i") <- stream 1
        //      tuple val(index),path("*j") <- stream 2
        // the index is then copied for multiple files
        return [names, streams].transpose().collect((name, stream) -> {
            return new Tuple2(
                name,
                stream.flatMap((index, group) -> {
                    this.removePendingTarget(name, index)
                    if (!(group instanceof List)) {
                        group = [group]
                    }
                    return group.collect((item) -> { // map
                        def LIMIT = 14 // 0..14 is 15 characters and enables sign to be ignored
                        def hash = ""
                        if (fullHash) {
                            hash = "$item".md5()[0..LIMIT]
                        } else {
                            hash = "${item.name}".md5()[0..LIMIT]
                        }
                        def v = Long.parseLong(hash, 16)
                        // println("post: <$name> $v $hash $item")
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

    public List post(streams, names) {
        return this._post(_debatch(streams), names, false)
    }

    public List postIn(streams, names) {
        return this._post(streams, names, true)
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

        // Classify each non-by stream once, up front. The result drives both
        // the dispatch branch in `to_group.collect` AND the bag-mode the
        // outer `_batch` uses. DESCENDANT_OF_BY and SIBLING want per-by-key
        // bagging (each by-item gets its own bag of descendant items, sized
        // batch_size); PARENT_OF_BY and WILDCARD want the legacy
        // consecutive-collate behavior (N adjacent emissions in one batch).
        def stream_relations = [:]
        to_group.each { stream ->
            def (s_name, s_chan) = stream
            stream_relations[s_name] = this.classify(s_name, by_name)
        }
        def per_key_bag_mode = stream_relations.values().any { r ->
            r == "DESCENDANT_OF_BY" || r == "SIBLING"
        }

        def by_parsed = by_stream.map(item -> {
            def (index, value) = item
            def group_k = index[by_name]
            return [
                new Tuple3(group_k, by_name, [new Tuple2(index, value)])
            ]
        })

        return _batch(batch_size, by_name, per_key_bag_mode, to_group
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
                // DESCENDANT branch (NEW, incremental): S items carry the
                // by-hashes they descend from in idx[by_name]. Mirror of the
                // PARENT branch with the join key inverted. Each S flatMaps
                // into one tuple per by-hash; we then combine(by:0) against
                // by_stream's own hash. No shared mutable state needed.
                def _name = name
                return _stream.flatMap((item) -> {
                    def (_index, _value) = item
                    def by_hashes = _index[by_name]
                    if (by_hashes == null) {
                        // Lineage violation: stream is declared as a
                        // descendant of by_name but the item's index
                        // doesn't carry by_name. Log and drop.
                        this._logDispatch(_name, "LINEAGE_VIOLATION", null, null)
                        return []
                    }
                    return by_hashes.collect((h) -> new Tuple2([h], item))
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
                    def (_, item, key) = combined
                    return [new Tuple3(key, _name, [item])]
                })
            }

            if (relation == "SIBLING") {
                // SIBLING branch (NEW, incremental): join on a shared
                // ancestor's hash. Both sides flatMap on idx[anc_key] and
                // combine(by:0). The final key emitted is the by-item's own
                // hash, so the downstream set-overlap filter still matches
                // by_parsed cleanly.
                def anc_key = this._firstSharedAncestor(name, by_name)
                def _name = name
                return _stream.flatMap((item) -> {
                    def (_index, _value) = item
                    def anc_hashes = _index[anc_key]
                    if (anc_hashes == null) return []
                    return anc_hashes.collect((h) -> new Tuple2([h], item))
                })
                .combine(by_stream.flatMap((item) -> {
                    def (_index, _value) = item
                    def anc_hashes = _index[anc_key]
                    if (anc_hashes == null) return []
                    return anc_hashes.collect((h) -> new Tuple2([h], _index[by]))
                }), by: 0)
                .map((combined) -> {
                    def (_, item, key) = combined
                    return [new Tuple3(key, _name, [item])]
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
            for (target : targets) {
                this.registerPendingTarget(target, common_index)
            }
            return [common_index, *values]
        }))
    }

    private def _collateBatch(batch) {
        def streams = batch.collect(item -> {
            def index = [:]+item[0] // copy to avoid mutating the map stored in pending_tasks
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

    // Legacy 2-arg form preserved for direct callers (test harnesses,
    // external workflow code). Routes to consecutive-collate mode, which
    // matches the historical behavior before per-key bagging existed.
    public def _batch(size, channel) {
        return this._batch(size, null, false, channel)
    }

    public def _batch(size, by_name, per_key_bag_mode, channel) {
        if (per_key_bag_mode) {
            // Per-by-key bagging: a result whose common_index[by_name] == k
            // joins bag k. groupTuple emits each bag as soon as it fills to
            // `size`, and remainder:true flushes partial bags at close. This
            // is the right semantic for DESCENDANT_OF_BY / SIBLING joins —
            // each by-item drives one downstream task with a bag of its
            // matching descendant items.
            return channel
            .map(_result -> {
                def common_index = _result[0]
                def bag_key = common_index[by_name]
                return new Tuple2(bag_key, _result)
            })
            .groupTuple(by: 0, size: size, remainder: true)
            .map(entry -> {
                def (bag_key, batch) = entry
                return this._collateBatch(batch)
            })
        }
        // Consecutive collate (legacy): PARENT_OF_BY and WILDCARD shapes
        // where the user wants N adjacent emissions folded into one task.
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
