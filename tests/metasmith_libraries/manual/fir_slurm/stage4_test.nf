// Stage 4 smoke test: validate Nextflow -> SLURM with GPU MIG slice on fir.
//
// Two processes:
//   cpu_demo: writes a marker (routes to CPU partition via cpu_small label)
//   gpu_demo: nvidia-smi + apptainer --nv torch.cuda check (gpu_small -> 1g.10gb)
//
// Verifies end-to-end: Nextflow submits both as SLURM jobs from the login
// node, each lands on the right partition with the right resources.

nextflow.enable.dsl = 2

process cpu_demo {
    label 'cpu_small'
    output:
    path 'cpu_marker.txt'
    script:
    """
    {
      hostname
      date
      echo "partition: \${SLURM_JOB_PARTITION:-unset}"
      echo "cpus: \${SLURM_CPUS_PER_TASK:-unset}"
    } > cpu_marker.txt
    cat cpu_marker.txt
    """
}

process gpu_demo {
    label 'gpu_small'
    input:
    path cpu_marker
    output:
    path 'gpu_marker.txt'
    script:
    """
    {
      echo "== node =="
      hostname
      date
      echo "partition: \${SLURM_JOB_PARTITION:-unset}"
      echo "gpus: \${SLURM_JOB_GPUS:-unset} / \${SLURM_GPUS:-unset}"
      echo "== nvidia-smi =="
      nvidia-smi -L
      echo "== apptainer --nv torch.cuda =="
      apptainer exec --nv \\
        /scratch/phyberos/dl_testing_claude/scaffold/pytorch_240_cu124.sif \\
        python -c 'import torch; print("cuda:", torch.cuda.is_available(), "dev:", torch.cuda.get_device_name(0) if torch.cuda.is_available() else "none")'
      echo "== upstream cpu_marker contents =="
      cat ${cpu_marker}
    } | tee gpu_marker.txt
    """
}

workflow {
    cpu = cpu_demo()
    gpu_demo(cpu)
}
