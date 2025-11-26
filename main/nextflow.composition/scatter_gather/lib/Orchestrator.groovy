class Orchestrator {
    private Map pending_tasks
    private Map index_history
    private Map children
    private def one_null

    Orchestrator(one_null) {
        this.pending_tasks = [:]
        this.index_history = [:]
        this.children = [:]
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
        if (index in pending_targets) {
            pending_targets.remove(index)
        }
        if (pending_targets.size()==0) {
            this.pending_tasks.remove(target)
        }
    }

    private synchronized def registerIndexHistory(String name, Map index) {
        def hist = this.index_history.get(name, []) // sets if $name not in index_history
        hist.add(index)
    }

    private synchronized def getExpectedSize(String grouping_by, String name, group_ks) {
        def pending_targets = this.pending_tasks[name]
        def size_valid = pending_targets==null? true : pending_targets.collect(i -> i[grouping_by]).every(ks -> ks.every(k -> !(k in group_ks)))
        def expected_size = !size_valid? -1 : this.index_history[name].collect(i -> i[grouping_by]).findAll(ks -> ks.any(k -> (k in group_ks))).size()
        return new Tuple2(size_valid, expected_size)
    }

    private synchronized def registerChild(String parent, int p, String child, int c) {
        def children = this.children.get(new Tuple2(parent, p), [])
        children.add(new Tuple2(child, c)) 
    }

    public def using(streams, targets) {
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

    public List post(streams, names) {
        // def (stream, name) = [streams, names]
        return [names, streams].transpose().collect((name, stream) -> {
            def completed = 0
            return new Tuple2(name, stream.flatMap((index, group) -> {
                this.removePendingTarget(name, index)
                // println("post: <$name> ${index} $pending_targets")
                if (!(group instanceof List)) {
                    group = [group]
                }
                return group.collect((item) -> { // map
                    completed+=1
                    index = [:]+index // copy the hashmap
                    index[name] = [completed]
                    this.registerIndexHistory(name, index)
                    return [index, item]
                })
                // println("post: $k $hist")
                // return x
            }))
        })
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
    
    public def group(to_group, by, to_split) {
        // using stream $by as in index
        // $to_group are grouped
        // $to_split are duplicatd
        // for each item in $by
        assert by.size() == 1
        def (by_name, by_stream) = by[0]

        def finished_parents = new SynchronizedHashGroup()
        def parent_channels = to_split.collect(t -> t[0])
        // since $parents are produced before $by
        // the parents for each $by will be available
        def stream_parents = to_split
        .collect((stream) -> {
            def (name, _stream) = stream
            return _stream.map((item) -> {
                finished_parents.register(name, item)
                return null // parents are returned with each item of $by
            })
        })
        .inject(
            by_stream.map((item) -> {
                def (index, value) = item
                def group_k = index[by_name]
                def _parent_streams = parent_channels.collect((parent_name) -> {
                    def parent_items = finished_parents.get(parent_name)
                    .collect(pitem -> {
                        def (pi, pv) = pitem
                        def is_parent = index[parent_name].any(v -> v in pi[parent_name])
                        return is_parent? new Tuple2(pi, pv) : null
                    })
                    .findAll(x -> x!=null)
                    return new Tuple3(group_k, parent_name, parent_items)
                })
                return [
                    new Tuple3(group_k, by_name, [new Tuple2(index, value)]),
                    *_parent_streams
                ]
            }),
            (result, channel) -> {
                return result.mix(channel)
            }
        )
        // .view(v -> "^ $v")
        .filter(x -> x!=null)

        return to_group
        .collect((stream) -> {
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
            def (name, _stream) = stream
            return _stream.concat(this.one_null)
            .flatMap((item) -> {
                if (item==null) { // this is the final call. There is no item
                    // last chance, flush remaining groups
                    return pending_groups
                    .collect((key, value) -> {
                        return new Tuple3(key, name, value)
                    })
                } else {
                    def (index, value) = item
                    assert by_name in index
                    def group_k = index[by_name]
                    def group = pending_groups.get(group_k, [])
                    group.add(new Tuple2(index, value))
                    pending_groups[group_k] = group
                    def (size_valid, expected_size) = this.getExpectedSize(by_name, name, group_k)
                    // println("req: stream $name by $by $is_final_call $size_valid $expected_size") // debug 
                    if (size_valid) {
                        for (key : pending_groups.keySet()) {
                            def candidate_group = pending_groups[key]
                            if (expected_size>0 && candidate_group.size()>=expected_size) {
                                pending_groups.remove(key)
                                return [new Tuple3(key, name, candidate_group)]
                            }
                        }
                    }
                    return []
                }
            })
            .map(x -> [x]) // see combine() below
        })
        .inject(stream_parents, (result, channel) -> { // reduce (to channel)
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
                def xx = _result
                .collect(x -> x[0])
                return xx
                .findAll(x -> x!=null).unique().size()==1
            })

        })
        // .view(v -> by_name=='f'? "^ $v" : null)
        .map((_result) -> { // we are a channel now, so we can map()
            // each channel is [key, name, group]
            def groups = _result.collect(channel -> channel[-1]) 
            def names = _result.collect(channel -> channel[1])
            def indexes = groups.collect(channel -> channel.collect(group -> group[0]))
            def index_of_by = [names, indexes].transpose().findAll((n, i) -> n==by_name).collect((n, i) -> i).flatten()[0]

            // println(" ^ $by_name${children.size()} $names $indexes")
            def common_index = this.combineIndexes(groups.collect(channel -> channel.collect(group -> group[0])).flatten())
            def values = groups.collect(channel -> channel.collect(group -> group[-1]))
            return [common_index, *values]
        })
    }
}
