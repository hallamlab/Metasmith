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

    public def post(streams, names) {
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

    public def group(by, streams) {
        def get_parent = (pk, indexes) -> {
            def parent_values = indexes.collect((index) -> index[pk]).unique()
            return parent_values
        }

        def pending_groups = [:]
        return streams
        .collect((stream) -> { // map
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
            def (name, _stream) = stream
            if (_stream==null) { // stream was not a tuple
                throw new IllegalArgumentException("channel was not properly setup using post()")
            }
            return _stream.concat(this.one_null)
            .flatMap((item) -> {
                if (item==null) { // this is the final call. There is no item
                    // last chance, flush remaining groups
                    return pending_groups
                    .collect((key, value) -> {
                        def (_name, k) = key
                        return _name==name? new Tuple3(k, name, value) : null
                    })
                    .findAll(x -> x!=null)
                } else {
                    def (index, value) = item
                    def UNGROUPED = 0
                    def group_v = (by in index)? index[by] : [UNGROUPED]
                    def group_k = new Tuple2(name, group_v)
                    if (name==by) {
                        // since this is the channel dictating the groups, it is the index
                        // each item is thus unique and so can return immediately.
                        return [new Tuple3(group_v, name, [new Tuple2(index, value)])]
                        // def to_return = [new Tuple3(group_v, name, [new Tuple2(index, value)])]
                        // // We can scan the pending groups 
                        // def parent_channels = index.keySet()
                        // for (entry : pending_groups) {
                        //     def (parent_channel, _) = entry.key
                        //     if (!(parent_channel in parent_channels)) continue
                        //     def expected_parent = index[parent_channel]
                        //     for (parent : entry.value) {
                        //         def (parent_index, pv) = parent
                        //         if (expected_parent in parent_index) {
                        //             to_return.add(new Tuple3(group_v, parent_channel, [new Tuple2(paren_index, pv)]))
                        //         }
                        //     }
                        // }
                    }

                    def (size_valid, expected_size) = [false, -1]
                    def group = pending_groups.get(group_k, [])
                    group.add(new Tuple2(index, value))
                    pending_groups[group_k] = group
                    if (group_k!=0) {
                        (size_valid, expected_size) = this.getExpectedSize(by, name, group_v)
                    }

                    // println("req: stream $name by $by $is_final_call $size_valid $expected_size") // debug 
                    if (size_valid) {
                        for (key : pending_groups.keySet()) {
                            def (_name, k) = key
                            if (_name != name) continue
                            def candidate_group = pending_groups[key]
                            if (expected_size>0 && candidate_group.size()>=expected_size) {
                                pending_groups.remove(key)
                                return [new Tuple3(k, name, candidate_group)]
                            }
                        }
                    }
                    return []
                }
            })
            // .view(v -> "^ $name by $by ${v[0]}")
            .flatMap((keys, channel, values) -> {
                return keys.collect(k -> [new Tuple3(k, channel, values)])
            })
        })
        .inject((result, channel) -> { // reduce (to channel)
            // cant use ${combine(by: 0)} since when k not in index,
            // it should be treated as wildcard, not a specific value
            return result
            .combine(channel)
            .filter((_result) -> {
                // println("  $by ${_result.size()}")
                return _result
                .collect(x -> x[0])
                // .findAll(x -> x!=null).unique().size()==1
            })

        })
        // .view(v -> "x $v")
        .map((_result) -> { // we are a channel now, so we can map()
            // each channel is [key, name, group]
            def groups = _result.collect(channel -> channel[-1]) 
            def names = _result.collect(channel -> channel[1])
            def indexes = groups.collect(channel -> channel.collect(group -> group[0]))
            def index_of_by = [names, indexes].transpose().findAll((n, i) -> n==by).collect((n, i) -> i).flatten()[0]

            // if (by=='h') {
            //     println("  $names $index_of_by ")
            // }
            // Using the $by as an "anchor", channels that are children of $by
            // have been matched by the combine().filter() above.
            // Parents of $by have been collected as a single group.
            // Here, we see if any channel is a parent and if so,
            // we find the actual parent instance and return that instead of the whold group
            def parents = [names, indexes].transpose()
            .findAll((name, ind) -> name!=by) // only look at channels that are not $by 
            .collect((name, ind) -> { // find the parent instance, if exists
                return new Tuple2(name, index_of_by[name])
            })
            .findAll((name, v) -> v!=null)
            .inject([:], (parents_table, parent) -> { // aggregate parent instances into lookup table
                def (k, v) = parent
                parents_table[k] = v
                return parents_table
            })
            groups = [names, groups].transpose()
            .collect((name, channel) -> {
                def parent = parents[name]
                // return only the parent if available
                return channel.findAll((index, value) -> {
                    return parent==null || index[name]==parent
                })
            })
            def common_index = this.combineIndexes(groups.collect(channel -> channel.collect(group -> group[0])).flatten())
            def values = groups.collect(channel -> channel.collect(group -> group[-1]))
            return [common_index, *values]
        })
    }
}
