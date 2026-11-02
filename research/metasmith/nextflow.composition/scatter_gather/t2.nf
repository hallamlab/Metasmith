process s1 {
	input:
		tuple val(index),path(a)
	output:
		tuple val(index),path("*b")
		tuple val(index),path("*c")
    script:
        """
        touch 1b
        touch 2b

        touch 1c
        touch 2c
        """
}

process t1 {
	input:
		tuple val(index),path(b)
	output:
		tuple val(index),path("*c")
    script:
        def dt = (index['b']-1)
        """
        sleep $dt
        touch 1c
        """
}

process t2 {
	input:
		tuple val(index),path(b)
	output:
		tuple val(index),path("*d")
    script:
        """
        touch 1d
        """
}

// process s2 {
//     input:
// 		tuple val(sample),path(b)
// 	output:
// 		tuple val("$sample"),path("*d")
//     """
//     touch ${sample}.1d
//     touch ${sample}.2d
//     """
// }

// process g2 {
// 	input:
// 		tuple HashMap(sample),path(d),path(b)
// 	output:
// 		tuple HashMap(sample),path("*y")
//     """
//     touch ${sample}.1y
//     """
// }

index = Channel.fromList(1..99).map(i -> i.toString())
def in = (f, k) -> {
	data = Channel.fromPath(f).splitCsv(header: false).map(row -> file(row[0]))
    return index.map(i -> {
        def index = [:] // empty hashmap
        index[k] = i
        return index
    }).merge(data)
    return index.merge(data)
}
def post = (x, k) -> {
    def completed = 0
    return x.flatMap((index, group) -> {
        if (!(group instanceof List)) {
            group = [group]
        }
        return group.collect(item -> {
            completed+=1
            index = [:]+index // copy the hashmap
            index[k] = completed
            return [index, item] 
        })

    })
}

def combine_indexes = (indexes) -> {
    def combined_index = [:]
    def keys = indexes.inject([:].keySet(), (result, i) -> result+i.keySet()) // reduce
    for (key : keys) {
        // if any is missing, use the remainder
        // if remainder different, skip
        // if remainder same, add
        def candidates = indexes.collect(index -> index[key]).findAll(x -> x!=null).unique() // collect == map, findAll == filter
        if (candidates.size()==1) {
            combined_index[key] = candidates[0]
        }
    }
    return combined_index
}


def join = (main, k, streams) -> {
    def reformat = (stream) -> {
        return stream
        .map(item -> {
            def index = item[0]
            def value = item[1]
            assert index[k] != null
            return [index[k], [index, value]]
        })
    }
    return streams
    .collect(reformat) // map
    .inject(reformat(main), (result, channel) -> { // reduce (to channel)
        return result.join(channel)
    })
    .map((_result) -> { // we are a channel now, so we can map()
        def items = _result[1..-1]
        return [
            combine_indexes(items.collect(x -> x[0])),
            *items.collect(x -> x[1])
        ]
    })
}

def group = (streams, k) -> {
    def reformat = (stream) -> {
        def format_item = (index, value) -> {
            if (k==null) {
                return [-1, [[index, value]]] // simulate effect of groupTuple
            } else {
                return [index[k], [index, value]]
            }
        }
        def format_stream = (_stream) -> {
            return (k==null? _stream : _stream.groupTuple())
            .map(item -> [item])
        }
        return format_stream(stream.map(item -> format_item(item[0], item[1])))
    }
    return streams
    .collect(reformat) // map
    .inject((result, channel) -> { // reduce (to channel)
        // cant use (by: 0) since when k not in index,
        // it should be treated as wildcard, not a specific value
        return result.combine(channel)
        .filter((_result) -> {
            return _result.collect(x -> x[0]).findAll(x -> x!=null).unique().size()==1
        })
    })
    .map((_result) -> { // we are a channel now, so we can map()
        def indexes = _result
        .collect(x -> x[1].collect(y -> y[0]))
        .inject((res, group) -> res + group)
        return [
            combine_indexes(indexes),
            *_result.collect(x -> x[1].collect(y -> y[1])),
        ]
    })
}

def cross = (streams) -> {
    return group(streams, null)
}

def gather = (streams) -> {
    return streams
    .collect(stream -> stream.map(x -> [x])) // map
    .inject((result, channel) -> { // reduce (to channel)
        return result.combine(channel)
    })
    .map((_result) -> { // we are a channel now, so we can map()
        return [
            combine_indexes(_result.collect(x -> x[0])),
            *_result.collect(x -> x[1]),
        ]
    })
}

workflow {
	a = in("../inputs.a1", "a")
    (b, c) = s1(a)
    b = post(b, 'b')
    c = post(c, 'c')
    
    d = t1(b)
    e = t2(b)
    d = post(d, 'd')
    e = post(e, 'e')

    // d.merge(e).view()
    // d.view()
    // group([b, d, e], 'b').view()
    ee = e.map(x -> [x[0]['b'], x[1]])
    dd = d.map(x -> [x[0]['b'], x[1]])

    ee.join(dd).view()

    // d = s2(b)
    // d = u(d)
    // y = g2(d.combine())

    // a.view()
    // b.view()
    // cross([b, c]).view()
    // c.view()
    // group([a], 'a').view()
    // group([a, b], 'a').view()
    // group([a, b], 'b').view()
    // group([b, c], 'b').view()
    // group([b, c, d], 'b').view()
    // cross([a, c, d]).view()
    // gather([a, c]).view()
    // c.combine(d).view()
    // pre(c).view()
    // d.view()
}

// def j(ch_a, ch_b) {
//     // https://www.nextflow.io/docs/latest/reference/operator.html#cross
//     // cross only emits those with matching keys
//     return ch_b.cross(ch_a).map({ [it[0][0], *it[1][1..-1], *it[0][1..-1]] })
// }

// process no_op {
// 	input:
// 		// tuple val(sample),path(_01),path(_02),path(_03)
// 		tuple val(sample),path(_01)

// 	output:
// 		tuple val("$sample"),path("*.out.noop")

//     """
//     echo "$sample,$_01" >${sample}.out.noop
//     """
// }

// process gather {
// 	input:
// 		tuple val(sample),path(_01),path(_02),path(_03)
// 	output:
// 		tuple val("$sample"),path("*.out.gather")
//     """
//     touch ${sample}__${_01.name}__${_02.name}__${_03.name}.out.gather
//     """
// }
