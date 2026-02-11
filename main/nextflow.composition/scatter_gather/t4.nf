include {in} from './utils'
// include { in; _using; _post; _group; xross; batch; combine_indexes } from './t3'
// include { strip_paths } from './t3'

// class Globals {
//     static pending_tasks = [:];
//     static index_history = [:]; 
// } 

// def using = (stream, targets) -> _using(stream, targets, Globals)
// def post = (streams, names) -> _post(streams, names, Globals)
// def group = (by, streams) -> _group(by, streams, Globals)

process s1 {
	input:
        tuple val(index),path(a)
	output:
        tuple val(index),path("*b")
        tuple val(index),path("*c")
    script:
        // def dt = (index['a']-1)
        // sleep $dt
        """
        touch 1-${a.name}.1b
        touch 1-${a.name}.2b
        touch 1-${a.name}.1c
        """
}

process b1 {
    input:
        tuple val(index),path(a),path(b)
    output:
        tuple val(index),path("*i")
        tuple val(index),path("*j")
    script:
        def n = index.size()+1
        // def ib = b.collect(x -> "\"${x.name}\"").join(' ')
        def ia = a.collect(x -> "\"${x.name}\"").join(' ')
        // sleep $n
        // echo "\$inputs" >inputs
        // IFS=',' read -ra g <<< "$b"
        """
        echo "$index" >index
        items=($ia)
        i=0
        for x in "\${items[@]}"; do
            (( i=i+1 ))
            if (( i == 2 )); then
                continue
            fi
            echo "\$x" > \${i}-\$x.1i
            echo "\$x" > \${i}-\$x.2i
            echo "\$x" > \${i}-\$x.j
        done
        """
}

process s2 {
	input:
        tuple val(index),path(a),path(c),path(i),path(j)
	output:
        tuple val(index),path("*f")
    script:
        """
        touch 1-${a.name}--${c.name}--${j.name}1f
        """
}

workflow t4 {
    main:

    END = Channel.fromList([null]) // cant create channels in groovy
    o = new Orchestrator(END)
    (a) = o.postIn([in("../inputs.a")], ["a"])

    k = ['b', 'c']
    //   // this spreads the "multiChannelOutput" class into a list
    //   // [*process()]
    (b, c) = o.post([*s1(o.group('a', [a], k, 1))], k)
    

    k = ['i', 'j']
    // x = o.post([*o.debatch(b1(o.batch(o.group('g', [g], k)), 3)))], k)
    // x.view()
    (i, j) = o.post([*b1(o.group('a', [a, b], k, 3))], k)

    k = ['f']
    //   // this spreads the "multiChannelOutput" class into a list
    //   // [*process()]
    (f) = o.post([*s2(o.group('a', [a, c, i, j], k, 1))], k)


    x = f

    f[1].view(v -> ">>> ${v.name}")
    // //   // b[1].view()
    // //   // batch(2, group('b', using([b], ['b']))).view()
    // //   b1(batch(2, group('b', using([b], ['b']))))
    // // b1(b[1].collate(2))

    // o.xross([c, f], ['x'])).view()
    // o.unify([h, f], ['x'])).view()


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
    x = x[1]
    // g = g[1]
    // y = y[1]
}

workflow {
    t4()
}
