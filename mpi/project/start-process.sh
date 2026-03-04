#!/bin/bash

machines

mpirun --allow-run-as-root --mca btl_tcp_if_include eth0 --mca oob_tcp_if_include eth0 --mca routed direct -np 6 --machinefile /root/machinefile /root/project/process_video $1 $2 $3 $4 > /var/log/mpi_jobs/$1.log 2>&1