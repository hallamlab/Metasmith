include { in; _using; _post; _group; xross; batch; combine_indexes } from './t3'
include { strip_paths } from './t3'

class Globals {
    static pending_tasks = [:];
    static index_history = [:]; 
} 

def using = (stream, targets) -> _using(stream, targets, Globals)
def post = (stream, name) -> _post(stream, name, Globals)
def group = (streams, by) -> _group(streams, by, Globals)

process s1 {
	input:
		tuple val(index),path(a)
	output:
		tuple val(index),path("*b")
		tuple val(index),path("*c")
    script:
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
        def k = index['b']
        def dt = (index['b']-1)/2
        // [ $dt -eq 0 ] && [ ${task.attempt} -eq 1 ] && exit 1
        // [ $k -eq 1 ] && exit 1
        """
        sleep $dt
        touch ${b.name}1f
        touch ${b.name}2f
        """
}

workflow {
	a = post(in("../inputs.a1"), "a")
    (b, c) = s1(group([using(a, ['b', 'c'])], 'a'))
    // (b, c) = s1(pass(using(a, ['b', 'c'])))
    b = post(b, 'b')
    c = post(c, 'c')
    
    f = s2(group([using(b, ['f'])], 'b'))
    // f = s2(pass(using(b, ['f'])))
    f = post(f, 'f')
    // f.view()

    // d.merge(e).view()
    // d.view()

    group([f, b, a, c], 'f').view(v -> ">>> final: ${strip_paths(v)}")
    // cross([a, b]).view(v -> ">>> final: $v")
    // cross([a, b]).view(v -> ">>> final: ${strip_paths(v)}")
}