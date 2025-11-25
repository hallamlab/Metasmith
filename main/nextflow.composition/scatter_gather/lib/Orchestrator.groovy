class Orchestrator {
    Map pending_tasks
    Map index_history
    private def one_null

    Orchestrator(one_null) {
        this.pending_tasks = [:]
        this.index_history = [:]
        this.one_null = one_null
    }

    private synchronized def registerPendingTarget(String target, Map index) {
        def pending_targets = this.pending_tasks.get(target, []) // this also sets if not exist
        pending_targets.add(index)
    }

    private synchronized def removePendingTarget(String target, Map index) {
        def pending_targets = this.pending_tasks[target]
        if (pending_targets!=null && index in pending_targets) {
            pending_targets.remove(index)
        }
        if (target in this.pending_tasks && pending_targets?.size()==0) {
            this.pending_tasks.remove(target)
        }
    }

    private synchronized def registerIndexHistory(String name, Map index) {
        def hist = this.index_history.get(name, []) // sets if $name not in index_history
        hist.add(index)
    }

    private synchronized def getExpectedSize(String grouping_by, String name, int group_k) {
        def pending_targets = this.pending_tasks[name]
        def size_valid = pending_targets==null? true : pending_targets.collect((i) -> i[grouping_by]).every((k) -> k!=group_k)
        def expected_size = !size_valid? -1 : this.index_history[name].collect(i -> i[grouping_by]).findAll(v -> v==group_k).size()
        return new Tuple2(size_valid, expected_size)
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
                    index[name] = completed
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
            def candidates = indexes.collect(index -> index[key]).findAll(x -> x!=null).unique() // collect == map, findAll == filter
            if (candidates.size()==1) {
                combined_index[key] = candidates[0]
            } else if (candidates.size()>1) {
                combined_index[key] = 0
            }
        }
        return combined_index
    }

    public def group(by, streams) {
        def get_parent = (pk, indexes) -> {
            def parent_values = indexes.collect((index) -> index[pk]).unique()
            return parent_values
        }

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
            def pending_groups = [:]
            return _stream.concat(this.one_null)
            .flatMap((item) -> {
                def is_final_call = item==null
                def (size_valid, expected_size) = [false, -1]
                if (!is_final_call) { // otherwise item is null
                    def (index, value) = item
                    def group_k = index[by]
                    def group = pending_groups.get(group_k, [])
                    group.add(new Tuple2(index, value))

                    println("$name by $by $group_k i: $index")

                    if (group_k!=null) {
                        (size_valid, expected_size) = this.getExpectedSize(by, name, group_k)
                    }
                }
                // println("req: stream $name by $by $is_final_call $size_valid $expected_size") // debug 
                if (size_valid) {
                    for (k : pending_groups.keySet()) {
                        def candidate_group = pending_groups[k]
                        if (expected_size>0 && candidate_group.size()>=expected_size) {
                            pending_groups.remove(k)
                            return [new Tuple3(k, name, candidate_group)]
                        }
                    }
                } 
                if (is_final_call) {
                    // println("  reqf: stream $name by $by $pending_groups") // debug
                    return pending_groups.collect((key, value) -> new Tuple3(key, name, value))
                }
                return []
            })
            // .view(v -> "^ $by $name $v")
            .map(item -> [item])
        })
        .inject((result, channel) -> { // reduce (to channel)
            // cant use ${combine(by: 0)} since when k not in index,
            // it should be treated as wildcard, not a specific value
            // println(">$by ${result.count()} ${channel.count()}<")
            return result
            .combine(channel)
            .filter((_result) -> {
                return _result.collect(x -> x[0]).findAll(x -> x!=null).unique().size()==1
            })

        })
        // .view(v -> "x $v")
        .map((_result) -> { // we are a channel now, so we can map()
            // each channel is [key, name, group]
            def groups = _result.collect(channel -> channel[-1]) 
            def names = _result.collect(channel -> channel[1])
            def indexes = groups.collect(channel -> channel.collect(group -> group[0]))
            def index_of_by = [names, indexes].transpose().findAll((n, i) -> n==by).collect((n, i) -> i).flatten()[0]
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
