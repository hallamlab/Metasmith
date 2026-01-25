import groovy.json.JsonOutput

class Orchestrator {
    private Map pending_tasks
    private Map index_history
    private Map child2parent
    private def one_null

    Orchestrator(one_null) {
        this.pending_tasks = [:]
        this.index_history = [:]
        this.child2parent = [:]     // this is just a topological map (keys only), the indexes link actual instances
        this.one_null = one_null
    }

    private synchronized def registerPendingTarget(String target, Map index) {
        def pending_targets = this.pending_tasks.get(target, [] as Set) // this also sets if not exist
        pending_targets.add(index)
    }

    private synchronized def removePendingTarget(String target, Map index) {
        if (!(target in this.pending_tasks)) return
        def pending_targets = this.pending_tasks[target]
        if (pending_targets==null) return
        // println("  - rm $target // $index // $pending_targets")
        // use the linage in the index to figure out which inputs were
        // used to produce the output (@index) and remove these from pending_tasks
        pending_targets = pending_targets.collect((candidate) -> {
            for (c : candidate) {
                if (!(c.key in index) || !index[c.key].containsAll(c.value)) {
                    return null
                }
            }
            return candidate
        })
        .findAll(x -> x!=null)
        if (pending_targets.size()==0) {
            this.pending_tasks.remove(target)
        } else {
            this.pending_tasks[target] = pending_targets
        }
    }

    private synchronized def registerIndexHistory(String name, Map index) {
        def hist = this.index_history.get(name, []) // sets if $name not in index_history
        hist.add(index)
    }

    private synchronized def getExpectedSize(String grouping_by, String name, group_ks) {
        def pending_targets = this.pending_tasks[name]
        def size_valid = pending_targets==null? true : pending_targets.collect(i -> i[grouping_by]).every(ks -> ks.every(k -> !(k in group_ks)))
        // println("  * $grouping_by $name $group_ks // $pending_targets")
        def expected_size = !size_valid? -1 : this.index_history[name].collect(i -> i[grouping_by]).findAll(ks -> ks.any(k -> (k in group_ks))).size()
        return new Tuple2(size_valid, expected_size)
    }

    public def using(streams, targets) {
        def parents = streams.collect((k, s) -> k) as Set
        for (t : targets) {
            this.child2parent[t] = this.child2parent.get(t, [] as Set)+parents
        }
        return streams.collect((stream) -> {
            def (name, _stream) = stream
            return new Tuple2(name, _stream.map((item) -> {
                def index = item[0]
                for (target : targets) {
                    this.registerPendingTarget(target, index)
                }
                // println("using: $name to $targets ${index}")
                return item
            }))
        })
    }

    public List _post(streams, names, fullHash) {
        // def (stream, name) = [streams, names]
        // todo: use hashes of file names instead of completion order, since order is not deterministic
        // and non-deterministic processes cant be hashed by nexflow
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
        return this._post(streams, names, false)
    }

    public List postIn(streams, names) {
        return this._post(streams, names, true)
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

        SynchronizedHashGroup() {
            this.map = [:]
        }

        public synchronized void register(k, v) {
            this.map[k] = this.map.get(k, [])+[v]
        }

        public synchronized def get(k) {
            return this.map[k].clone()
        }
    }
    
    private boolean isParent(String parent, String child) {
        if (parent==child) return false
        if (!(child in this.child2parent)) return false
        def parents = this.child2parent[child]
        if (parent in parents) return true
        return parents.any(p -> this.isParent(parent, p)) 
    }

    public def group(by, streams) {
        def original_order = streams.collect(s -> s[0]).withIndex().collectEntries((item, i) -> [item, i])
        def by_channel = streams.find(s -> s[0]==by)
        def (by_name, by_stream) = by_channel
        // println("g $by_name")

        def to_group = streams.findAll(stream -> {
            def (name, _stream) = stream
            return name!=by_name
        })
        def by_parsed = by_stream.map(item -> {
            def (index, value) = item
            def group_k = index[by_name]
            return [
                new Tuple3(group_k, by_name, [new Tuple2(index, value)])
            ]
        })

        return to_group
        .collect((stream) -> {
            // if a given stream is a parent, we use the by_stream as an index
            // and emit parents as they complete with the corresponding item of the by_stream
            def (name, _stream) = stream
            def _is_parent = this.isParent(name, by_name)
            if (_is_parent) {
                return _stream.map((item) -> {
                    def (_index, _value) = item
                    def k = _index[name]
                    return new Tuple2(k, item)
                })
                .combine(by_stream.map((item) -> {
                    def (_index, _value) = item
                    def k = _index[name]
                    return new Tuple2(k, _index[by]) // instead of a value, we pass through the "by index"
                }), by: 0)
                .map((combined) -> {
                    def (_, item, key) = combined // first is key of parent, used to sync with by
                    // println("$by // $name  // $key // $item")
                    return [new Tuple3(key, name, [item])]
                })
            }
            // else not parent...

            // The following enables groups to be emitted immediately when ready.
            // As tasks are queued, they are added to a pending list via ${using()}
            // and promise a named output stream.
            // As tasks complete, a history of completed items (by index) is stored
            // via ${post()}.
            // When there are no more pending tasks that may create an item with 
            // the relavent groupby ($by) value, the size of each group can be calculated
            // using $index_history.
            // Here, we buffer each item in $pending_groups until the group size matches
            // the expected size calculated from $index_history.
            // $flatMap enables remainders to be emmitted at end
            def pending_groups = [:]
            return _stream.concat(this.one_null)
            .flatMap((item) -> {
                if (item==null) { // this is the final call. There is no item
                    // last chance, flush remaining groups
                    return pending_groups
                    .collect((key, value) -> {
                        return new Tuple3(key, name, value)
                    })
                } else {
                    // register this group
                    def (index, value) = item
                    def group_k = index[by_name]
                    def group = pending_groups.get(group_k, [])
                    group.add(new Tuple2(index, value))
                    pending_groups[group_k] = group

                    // check at most N for every item finished in this stream
                    // and emit if complete, letting it "catch up" by N-1
                    // without this limit, all items from pending_groups may be checked
                    // which is something like O(n^2) vs the size of this stream?
                    def N = 2
                    def to_check = pending_groups.keySet().findAll(k -> k!=group_k).take(N-1) + [group_k]
                    return to_check.collect(key -> {
                        def candidate_group = pending_groups[key]
                        def (size_valid, expected_size) = this.getExpectedSize(by_name, name, key)
                        if (expected_size>0 && candidate_group.size()>=expected_size) {
                            pending_groups.remove(key)
                            return new Tuple3(key, name, candidate_group)
                        } else {
                            return null
                        }
                    })
                    .findAll(x -> x!=null)
                }
            })
            .map(x -> [x]) // see combine() below
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
                return _result
                .collect(x -> x[0])
                .findAll(x -> x!=null)
                .unique().size()==1
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
        })
    }

    public def batch(channel, n) {
        // return proc(channel)
        return channel.collate(n).map(group -> {
            def streams = group.transpose()
            def indexes = streams[0]
            def values = streams[1..-1].collect(x -> x.flatten())
            return [indexes, *values]
        })
    }

    public def debatch(channels) {
        return channels.collect(output -> {
            output.flatMap(streams -> {
                def groups = streams.transpose()
                return groups
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
