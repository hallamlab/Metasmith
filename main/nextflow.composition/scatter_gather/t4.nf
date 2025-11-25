include { in; _using; _post; _group; xross; batch; combine_indexes } from './t3'
include { strip_paths } from './t3'

class Globals {
    static pending_tasks = [:];
    static index_history = [:]; 
} 

def using = (stream, targets) -> _using(stream, targets, Globals)
def post = (streams, names) -> _post(streams, names, Globals)
def group = (by, streams) -> _group(by, streams, Globals)

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
        def dt = (index['b']-1)
        // [ $dt -eq 0 ] && [ ${task.attempt} -eq 1 ] && exit 1
        // [ $k -eq 1 ] && exit 1
        """
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
		tuple val(index),path(f),path(b),path(c)
	output:
		tuple val(index),path("*g")
    script:
        // echo ${a}>>1g
        """
        echo ${b}>>1g
        echo ${c}>>1g
        echo ${f}>>1g
        """
}

process b1 {
    input:
		tuple val(indexes),val(struct),path(files)
    output:
		tuple val(indexes),path("*i")
    script:
        """
        echo ${struct}>>1i
        """
}

workflow {
	// a = post(in("../inputs.a1"), "a")
    (a) = post([in("../inputs.a1")], ["a"])

    k = ['b', 'c']
    // this spreads the "multiChannelOutput" class into a list
    // [*process()]
    (b, c) = post([*s1(group('a', using([a], k)))], k)

    
    // b[1].view()
    // input:
    // indexes, group sturcture, flattened files for input
    b1(batch(2, group('b', using([b], ['b'])))).view()
    // b1(b[1].collate(2))


    // logistics processes
    // batch outputs normal






    // k = ['f']
    // (f) = post([*s2(group('b', using([b], k)))], k)

    // k = ['h']
    // (h) = post([*p1(group('f', using([f], k)))], k)

    // k = ['g']
    // (g) = post([*g1(group('f', using([f, h, c], k)))], k)
    // // g[1].view()

    // x = xross([a, b])


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