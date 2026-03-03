#!/bin/bash

machines

mpirun --allow-run-as-root -np 6 --machinefile /root/machinefile /usr/local/bin/process_video $1 $2 $3 $4 > /var/log/mpi_jobs/$1.log 2>&1
