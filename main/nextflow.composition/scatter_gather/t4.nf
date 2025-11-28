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
        touch ${a.name}1b
        touch ${a.name}2b

        touch ${a.name}1c
        touch ${a.name}2c
        """
}

process s2 {
	input:
        tuple val(index),path(b)
	output:
        tuple val(index),path("*f")
    script:
        // def k = index['b']
        def dt = (index['b'][0]-1)
        // [ $dt -eq 0 ] && [ ${task.attempt} -eq 1 ] && exit 1
        // [ $k -eq 1 ] && exit 1
        """
        echo $dt
        sleep $dt
        touch ${b.name}1f
        touch ${b.name}2f
        """
}

process p1 {
    input:
        tuple val(index),path(f)
	output:
        tuple val(index),path("*h")
    script:
        """
        touch ${f.name}1h
        """
}

process g1 {
    input:
        tuple val(index),path(b),path(f),path(c)
	output:
        tuple val(index),path("*g")
    script:
        // echo ${a}>>1g
        // echo ${c}>>1g
        """
        touch ${f.name}1g
        """
}

process b1 {
  input:
        tuple val(index),path(g)
    output:
        tuple val(index),path("*i")
    script:
        def n = index.size()+1
        // sleep $n
        """
        IFS=',' read -ra g <<< "$g"
        echo "$index" >x
        echo "$g" >>x
        for arri in {1..$n}; do
            echo \$arri>>\${arri}i
        done
        """
}

workflow {
    END = Channel.fromList([null]) // cant create channels in groovy
    o = new Orchestrator(END)
    // in("../inputs.a1")
    (a) = o.post([in("../inputs.a1")], ["a"])

    k = ['b', 'c']
    //   // this spreads the "multiChannelOutput" class into a list
    //   // [*process()]
    (b, c) = o.post([*s1(o.group('a', o.using([a], k)))], k)
      
    // //   // b[1].view()
    // //   // batch(2, group('b', using([b], ['b']))).view()
    // //   b1(batch(2, group('b', using([b], ['b']))))
    // // b1(b[1].collate(2))


    // // logistics processes
    // // batch outputs normal

    k = ['f']
    (f) = o.post([*s2(o.group('b', o.using([b], k)))], k)

    k = ['h']
    (h) = o.post([*p1(o.group('f', o.using([f], k)))], k)
    // h[1].view()

    k = ['g']
    gx = o.group('f', o.using([b, f, c], k))
    // gx.view(v -> "  . $v")
    (g) = o.post([*g1(gx)], k)
    // x = o.group('f', o.using([b, f, c], k))
    // (g) = o.post([*o.batch(g1, x, 3)], k)
    // g[1].view()

    k = ['x']
    // x = o.post([*o.debatch(b1(o.batch(o.group('g', o.using([g], k)), 3)))], k)
    // x.view()
    (y) = o.post(o.debatch([*b1(o.batch(o.group('g', o.using([g], k)), 3))]), k)
    // y[1].view()

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
}