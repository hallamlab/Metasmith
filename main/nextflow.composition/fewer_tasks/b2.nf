params.home = '/home/tony/workspace/tools/Metasmith/main/local_mock/cache/local_home'
params.workspace = "${params.home}/runs/dfCva1vO"
params.bootstrap = '''
CONTAINER=/msm_home
DIRECT=/home/tony/workspace/tools/Metasmith/main/local_mock/cache/local_home
function bootstrap {
	if [ -e $CONTAINER ]; then
		$CONTAINER/lib/msm_bootstrap $@
	elif [ -e $DIRECT ]; then
		$DIRECT/lib/msm_bootstrap $@
	else
		echo "critical error: could not find metasmith bootstrap script"
	fi
}
'''
index = Channel.fromList(1..10)
def In(f) {
	data = Channel.fromPath(f).splitCsv(header: false).map({row -> file(row[0])})
	return index.merge(data)
}


process b01p01__interleave_pe_reads_seqtk {
	publishDir "$params.output/b01p01_interleave_pe_reads_seqtk", mode: "copy", pattern: "interleaved_reads.fq.gz", saveAs: {f -> String.format("%05d_%s", sample, f)}

	input:
		tuple val(sample),path(_01),path(_02),path(_03)

	output:
		tuple val("$sample"),path("interleaved_reads.fq.gz")

	"""
	${params.bootstrap}
	echo "$task.cpus/$task.memory" >.command.metadata
	b01="/home/tony/workspace/projects/Cyanoverse/main/assembly_denovo/cache/SRR14675403"
	echo "sample $sample, step 1"
	echo "--bind \$b01:\$b01" >>.command.metadata
	bootstrap ${params.workspace} "$sample/1"
	[ -e .command.success ] && exit 0 || exit 1
	"""
}

workflow b01 {
	_DakjL3X5 = In("inputs/DakjL3X5")
	_fMTYDPew = In("inputs/fMTYDPew")
	_mZmdJ9Uw = In("inputs/mZmdJ9Uw")
	_pwsmsbXp = b01p01__interleave_pe_reads_seqtk(_fMTYDPew.join(_mZmdJ9Uw).join(_DakjL3X5))
}

workflow {
    b01()
}
