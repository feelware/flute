#!/bin/bash

# Arguments:
# $1 = job_id
# $2 = video_path (in MinIO)
# $3 = lut_path (in MinIO)
# $4 = num_mpi_processes (number of processes to use)
# $5 = enable_intra_node_parallelism (1 = enabled, 0 = disabled)

machines

time mpirun --allow-run-as-root --mca btl_tcp_if_include eth0 --mca oob_tcp_if_include eth0 --mca routed direct -np $4 --machinefile /root/machinefile /root/project/process_video $1 $2 $3 $5 > /var/log/mpi_jobs/$1.log 2>&1
