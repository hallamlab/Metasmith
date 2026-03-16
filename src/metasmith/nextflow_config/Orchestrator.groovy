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
        // println("  <<ADD $target // $index")
        def pending_targets = this.pending_tasks.get(target, [] as Set) // this also sets if not exist
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
        def hist = this.index_history.get(name, []) // sets if $name not in index_history
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

    public def group(by, streams, targets, batch_size) {
        def parents = streams.collect((k, s) -> k) as Set
        for (t : targets) {
            this.child2parent[t] = this.child2parent.get(t, [] as Set)+parents
        }

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

        return _batch(batch_size, to_group
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

            // Buffer non-parent items by group key and emit only when the stream
            // closes. Emitting early from transient index_history snapshots can
            // split a single logical group into multiple partial groups.
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
                    return []
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

    public def _batch(size, channel) {
        // return proc(channel)
        return channel.collate(size).map(batch -> {
            def streams = batch.collect(item -> {
                def index = [:]+item[0] // copy to avoid mutating the map stored in pending_tasks
                def values = item[1..-1]
                index['FILES'] = values.collect(path -> path.name)
                return [index, *values]
            }).transpose()
            def indexes = streams[0]
            // careful, this unique() could remove real file collisions as well!
            // this is needed for cases where reference dbs are passed multiple times per batch
            def values = streams[1..-1].collect(stream -> stream.flatten().unique())
            return [indexes, *values]
        })
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
