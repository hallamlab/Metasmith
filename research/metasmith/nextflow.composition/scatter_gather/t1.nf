process s1 {
	input:
		tuple val(index),path(a)
	output:
		tuple val(index),path("*b")
		tuple val(index),path("*c")
    script:
        def sample = index['a']
        """
        touch ${sample}.1b
        touch ${sample}.2b
        
        touch ${sample}.1c
        touch ${sample}.2c
        """
}

process g1 {
	input:
		tuple val(ia),path(a)
		tuple val(ic),path(c)
	output:
		tuple val(index),path("*x")
    script:
        def sample = index['x']
        """
        touch ${sample}.1x
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

index = Channel.fromList(1..99).map({ i->i.toString()})
def In(f, k) {
	data = Channel.fromPath(f).splitCsv(header: false).map({row -> file(row[0])})
    return index.map({ i->
        def index = [:] // empty hashmap
        index[k] = i
        return index
    }).merge(data)
    return index.merge(data)
}
def u(x, k) { // post process
    def completed = 0
    return x.flatMap({ index, group -> 
        return group.collect({ item ->
            completed+=1
            index = [:]+index // copy the hashmap
            index[k] = completed
            return [index, item] 
        })
    })
}
def j(a, b, k) {
    aa = a.map( t->[[t[0].get(k, 0), t[0], *t[1..-1]]] )
    bb = b.map( t->[[t[0].get(k, 0), t[0], *t[1..-1]]] )
    return aa.combine(bb)
    .filter( _a,_b ->
        return _a[0]==_a[0]
    )
    .map( _a,_b ->
        def index = [:]+_a[1]+_b[1]
        return [index, *]
    )
    // return aa.combine(bb, by: 0).map( t->
    //     def index = t[1]
    //     return [t[0], *t[1..-1]]
    // )
}

workflow {
	a = In("../inputs.a1", "a")
    (b, c) = s1(a)
    b = u(b, 'b'); c = u(c, 'c')
    _g1 = j(a, c, 'a')

    // d = s2(b)
    // d = u(d)
    // y = g2(d.combine())

    // a.view()
    // x.view()
    _g1.view()


    // c.view()
    // v(a)
    // v(c)
    // v(x)
    // a.view()
    // c.view()
    // x.view()

    // (b, c) = scatter(a)

    // xf = no_op(a_in)
    // x1 = u(x1)
    // x2 = u(x2)
    
    // y = ij(ij(ij(x1, xf), x2), xf)
    // y = j(j(x1, x2), xf)

    // t = Channel.fromList(1.10)

    // y.first().view()
    // y.first().view()
    // y = j(x1, x2)
    // y.map({ t->[t[0], *t[1..-1].name] }).view()

    // target = gather(y)
    // target.map({ t->[t[0], *t[1..-1].name] }).view()
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
