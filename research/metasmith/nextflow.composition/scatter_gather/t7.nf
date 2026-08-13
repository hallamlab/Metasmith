import groovy.json.JsonSlurper
def in(f, l) {
    def rows = Channel.fromPath(f).splitCsv(header: false)
    if (f in l) {
        rows = Channel.fromList(l[f]).merge(rows)
    }
    return rows.map((row) -> {
        if (row.size()>1) {
            def (ri, rx) = row
            return tuple(ri, file(rx))
        } else {
            def i = [:]
            return tuple(i, file(row[0]))
        }
    })
}

process s1 {
	input:
        tuple val(index),path(a)
	output:
        tuple val(index),path("*b")
        tuple val(index),path("*c")
    script:
        // def n = a.name[-1].toInteger()
        // def dt = (n-1)
        // def x = n*2 - 1
        // def y = n*2
        // sleep $dt
        """
        touch ${a.name}.b
        touch ${a.name}.1c
        touch ${a.name}.2c
        """
}

process s2 {
	input:
        tuple val(index),path(b)
	output:
        tuple val(index),path("*d")
    script:
        // def k = index['b']
        // def dt = (index['b'][0]-1)
        // [ $dt -eq 0 ] && [ ${task.attempt} -eq 1 ] && exit 1
        // [ $k -eq 1 ] && exit 1
        // echo $dt
        """
        touch ${b.name}.d
        """
}

process s3 {
	input:
        tuple val(index),path(c)
	output:
        tuple val(index),path("*e")
    script:
        // def k = index['b']
        // def dt = (index['b'][0]-1)
        // [ $dt -eq 0 ] && [ ${task.attempt} -eq 1 ] && exit 1
        // [ $k -eq 1 ] && exit 1
        // echo $dt
        """
        touch ${c.name}.e
        """
}

workflow t5 {
    main:
    o = new Orchestrator(Channel.fromList([null])) // cant create channels in groovy
    l = new JsonSlurper().parseText(file("../l5.json").text)
    o.child2parent["a"] = (["p"] as Set)
    // (p) = o.postIn([in("../inputs.p", l)], ["p"])
    (a) = o.postIn([in("../inputs.a", l)], ["a"])

    // x = ['a':[1], 'b':[2]]
    // for (e : x) {
    //     println("$e.key $e.value")
    // }

    // a[1].view()

    k = ['b', 'c']
    (b, c) = o.post([*s1(o.group('a', [a], k))], k)

    // b[1].view()

    k = ['d']
    (d) = o.post([*s2(o.group('b', [b], k))], k)

    k = ['e']
    (e) = o.post([*s3(o.group('c', [c], k))], k)


    // (c) = o.post([*s2(o.group('p', o.using([b], k)))], k)
    d[1].view((i, v) -> "D>> ${v.name}")
    e[1].view((i, v) -> "E>> ${v.name}")
    x = a

    // k = ['b', 'c']
    // //   // this spreads the "multiChannelOutput" class into a list
    // //   // [*process()]
    // (b, c) = o.post([*s1(o.group('a', o.using([a], k)))], k)
      
    // // //   // b[1].view()
    // // //   // batch(2, group('b', using([b], ['b']))).view()
    // // //   b1(batch(2, group('b', using([b], ['b']))))
    // // // b1(b[1].collate(2))


    // // // logistics processes
    // // // batch outputs normal

    // k = ['f']
    // (f) = o.post([*s2(o.group('b', o.using([b], k)))], k)

    // k = ['h']
    // (h) = o.post([*p1(o.group('f', o.using([f], k)))], k)
    // // h[1].view()

    // k = ['g']
    // gx = o.group('f', o.using([b, f, c], k))
    // // gx.view(v -> "  . $v")
    // (g) = o.post([*g1(gx)], k)
    // // x = o.group('f', o.using([b, f, c], k))
    // // (g) = o.post([*o.batch(g1, x, 3)], k)
    // // g[1].view()

    // k = ['x']
    // // x = o.post([*o.debatch(b1(o.batch(o.group('g', o.using([g], k)), 3)))], k)
    // // x.view()
    // (y) = o.post(o.debatch([*b1(o.batch(o.group('g', o.using([g], k)), 3))]), k)
    // // y[1].view()

    // x = o.post([*o.batch(b1, o.group('g', o.using([g], k)), 3)], k)
    // x.view()
    // o.batch(b1, o.group('g', o.using([g], k)), 3).view()

    // o.xross(o.using([c, f], ['x'])).view()
    // o.unify(o.using([h, f], ['x'])).view()


    // b = post(b, 'b')
    // c = post(c, 'c')
    // println("${x.getClass()}")
    // b[1].view()
    // println("$c")
    // (b, c) = s1(pass(using(a, ['b', 'c'])))
    

    // // f = s2(pass(using(b, ['f'])))
    // // f.view()

    // // d.merge(e).view()
    // // d.view()

    // // group([f, b, c, a], 'a').view(v -> ">>> final: ${strip_paths(v)}")

    
    // g.view()
    // cross([a, b]).view(v -> ">>> final: $v")
    // cross([a, b]).view(v -> ">>> final: ${strip_paths(v)}")

    emit:
    // g = g[1]
    // y = y[1]
    x = x[1]
}


workflow {
    t5()
}