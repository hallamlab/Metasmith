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
    script:
        // def k = index['a']
        def n = a.name[-1].toInteger()
        def dt = (n-1)/10
        def x = n*2 - 1
        def y = n*2
        // def dt = (1-index['a'][0])
        // touch ${a.name}.zb
        """
        echo $dt
        sleep $dt
        touch ${a.name}.${x}b
        touch ${a.name}.${y}b
        """
}

process s2 {
	input:
        tuple val(index),path(b)
	output:
        tuple val(index),path("*c")
    script:
        def n = b.name[-2].toInteger()
        def dt = (n-1)
        // def dt = (3-index['b'][0])
        // [ $dt -eq 0 ] && [ ${task.attempt} -eq 1 ] && exit 1
        // [ $k -eq 1 ] && exit 1
        // echo $dt
        // touch ${b.name}.xc
        // touch ${b.name}.yc
        """
        echo $dt
        sleep $dt
        touch xc
        touch yc
        touch zc
        """
}

process s3 {
	input:
        tuple val(index),path(a),path(b),path(c)
	output:
        tuple val(index),path("*d")
    script:
        // def n = c.size()+1

        // def k = index['b']
        // def dt = (index['b'][0]-1)
        // [ $dt -eq 0 ] && [ ${task.attempt} -eq 1 ] && exit 1
        // [ $k -eq 1 ] && exit 1
        // echo $dt
        """
        touch ${b.name}.d
        """
}

process s3p {
	input:
        tuple val(index),path(a),path(b),path(c),path(p)
	output:
        tuple val(index),path("*d")
    script:
        // def n = c.size()+1

        // def k = index['b']
        // def dt = (index['b'][0]-1)
        // [ $dt -eq 0 ] && [ ${task.attempt} -eq 1 ] && exit 1
        // [ $k -eq 1 ] && exit 1
        // echo $dt
        """
        touch ${b.name}.d
        """
}
import groovy.json.JsonSlurper

workflow t5 {
    main:
    // println("${1 == 1}")
    // println("${[1] == [1]}")
    // a = ["55ca6345da803f23fff0146df229700a"]
    // x = "fff0146df229700a"
    // y = "55ca6345da803f23"
    // b = ["$y$x", '1']
    // c = "a"
    // x = [["a"], ["$c"], ["b"]]
    // println("${x.unique()}")

    x = ['b':[3, 2, 1], 'a':[1, 2, 3], 'c':[2, 1, 3]].sort().collectEntries((k, v) -> [k, v.sort()])
    // println("1234567"[0..3])
    println("${new Random().nextInt(3)+1}")

    // p = "/msm_home/runs/jNnaM8ZN/_metasmith/task/data/A7ZRjTx9d2kZ/SRR21655586.json"
    // ph = p.md5()[0..14]
    // println("${ph}")
    // println("${Long.parseLong(ph, 16)}")

    // // hash = "ffffffffffffffff"
    // hash = "fffffffffffffff"
    // // hash = "1"
    // hex = Long.toHexString(Long.parseLong(hash, 16))
    // // hex = Long.toHexString(Long.parseUnsignedLong(hash))

    // l = new JsonSlurper().parseText(file("../long.json").text)
    // max_long = l["val"]
    // println("${max_long}")

    // sleep(15)

    // o = new Orchestrator(Channel.fromList([null])) // cant create channels in groovy
    // l = new JsonSlurper().parseText(file("../l6.json").text)
    // // o.child2parent["a"] = (["p"] as Set)
    // (p) = o.postIn([in("../inputs.p", l)], ["p"])
    // (a) = o.postIn([in("../inputs.a2", l)], ["a"])

    // // o.group('a', [a]).view(v -> ">>> final: ${strip_paths(v)}")


    // k = ['b']
    // (b) = o.post([*s1(o.group('a', o.using([a], k)))], k)

    // k = ['c']
    // (c) = o.post([*s2(o.group('b', o.using([b], k)))], k)

    // k = ['d']
    // // (d) = o.post([*s3(o.group('b', o.using([a, b, c], k)))], k)
    // (d) = o.post([*s3p(o.group('b', o.using([a, b, c, p], k)))], k)
    // d[1].view((i, v) -> ">>> $i // ${v.name}")
    // x = a


    
    // // g.view()
    // // cross([a, b]).view(v -> ">>> final: $v")
    // // cross([a, b]).view(v -> ">>> final: ${strip_paths(v)}")

    // emit:
    // g = g[1]
    // y = y[1]
    // x = x[1]
}


workflow {
    t5()
}