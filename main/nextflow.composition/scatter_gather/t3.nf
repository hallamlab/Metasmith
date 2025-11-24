def in(f) {
	return Channel.fromPath(f)
    .splitCsv(header: false)
    .map((row) -> {
        def i = [:] // this will be filld with ${post()}
        return tuple(i, file(row[0]))
    })
}

def _using(stream, targets, globals) {
    def (name, _stream) = stream
    return tuple(name, _stream.map((item) -> {
        def index = item[0]
        for (target : targets) {
            def pending_targets = globals.pending_tasks.get(target, [])
            pending_targets.add(index)
        }
        // println("using: $name to $targets ${index}")
        return item
    }))
}

def _post(stream, name, globals) {
    def completed = 0
    return tuple(name, stream.flatMap((index, group) -> {
        pending_targets = globals.pending_tasks[name]
        pending_targets?.remove(index)
        if (pending_targets?.size()==0) {
            globals.pending_tasks?.remove(name)
        }
        // println("post: <$name> ${index} $pending_targets")
        if (!(group instanceof List)) {
            group = [group]
        }
        def hist = globals.index_history.get(name, []) // sets if $name not in index_history
        return group.collect((item) -> { // map
            completed+=1
            index = [:]+index // copy the hashmap
            index[name] = completed
            hist.add(index)
            return [index, item]
        })
        // println("post: $k $hist")
        // return x
    }))
}

def combine_indexes(indexes) {
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

def _group(streams, by, globals) {
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
        return _stream.concat(Channel.fromList([null]))
        .flatMap((item) -> {
            def is_final_call = item==null
            def size_valid = false
            def expected_size = -1
            if (!is_final_call) {
                def (index, value) = item
                def group_k = index[by]
                def group = pending_groups.get(group_k, [])
                group.add(tuple(index, value))
                pending_targets = globals.pending_tasks[name]?.clone() // clones for thread safety
                size_valid = pending_targets==null? true : pending_targets.collect((i) -> i[by]).every((k) -> k!=group_k)
                expected_size = !size_valid? -1 : globals.index_history[name].clone().collect(i -> i[by]).findAll(v -> v==group_k).size()
            }
            // println("req: stream $name by $by $is_final_call $size_valid $expected_size") // debug 
            if (size_valid) {
                for (k : pending_groups.keySet()) {
                    def candidate_group = pending_groups[k]
                    if (expected_size>0 && candidate_group.size()>=expected_size) {
                        pending_groups.remove(k)
                        return [tuple(k, name, candidate_group)]
                    }
                }
            } else if (is_final_call) {
                // println("  reqf: stream $name by $by $pending_groups") // debug
                return pending_groups.collect((key, value) -> tuple(key, name, value))
            }
            return []
        })
        // .view(v -> "group: $name $v")
        .map(item -> [item])
    })
    .inject((result, channel) -> { // reduce (to channel)
        // cant use ${combine(by: 0)} since when k not in index,
        // it should be treated as wildcard, not a specific value
        return result
        .combine(channel)
        .filter((_result) -> {
            return _result.collect(x -> x[0]).findAll(x -> x!=null).unique().size()==1
        })
        // .view(v -> "^ $v")
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
            return tuple(name, index_of_by[name])
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
        def common_index = combine_indexes(groups.collect(channel -> channel.collect(group -> group[0])).flatten())
        def values = groups.collect(channel -> channel.collect(group -> group[-1]))
        // println("x $parents")
        return tuple(common_index, *values)
        return _result
    })
}

def xross(streams) {
    return streams
    .collect((stream) -> { // map
        def (name, _stream) = stream
        return _stream
        .map(item -> [item])

    })
    .inject((result, channel) -> { // reduce (to channel)
        return result
        .combine(channel)
    })
    .map((_result) -> {
        def indexes = _result.collect(item -> item[0])
        def values = _result.collect(item -> [item[-1]])
        return tuple(combine_indexes(indexes), *values)
    })
}

def batch(streams, n) {
    // todo
}

def strip_paths(item) {
    def values = item[1..-1]
    return tuple(item[0], *values.collect(x -> x.collect(p -> p.name)))
}
