"""
This script runs repeated jobs that read shuffled data.
"""

import subprocess

import utils

import time

start = time.time()

num_machines = 1
cores_per_machine = 8
items_per_partition_values = [8000000]
base_num_map_tasks = num_machines * cores_per_machine
base_num_reduce_tasks = num_machines * cores_per_machine
num_tasks_multiplier_values = [1, 2, 4, 8, 16, 32]
longs_per_value = 6
num_shuffles = 6
sortByKey = False
cacheRdd = False
slaves = [slave_line.strip("\n") for slave_line in open("/root/spark/conf/slaves").readlines()]
print "Running experiment assuming slaves %s" % slaves

for items_per_partition in items_per_partition_values:
  for num_tasks_multiplier in num_tasks_multiplier_values:
    num_reduce_tasks = num_tasks_multiplier * base_num_reduce_tasks
    num_map_tasks = num_tasks_multiplier * base_num_map_tasks
    print "*************Running experiment with {} shuffle values".format(
      items_per_partition)
    parameters = [num_map_tasks, num_reduce_tasks, items_per_partition, longs_per_value, num_shuffles, sortByKey, cacheRdd]
    stringified_parameters = ["{}".format(p) for p in parameters]
    command = "/root/spark/bin/run-example monotasks.ShuffleJob {}".format(
      " ".join(stringified_parameters))
    print command
    subprocess.check_call(command, shell=True)

    utils.copy_and_zip_all_logs(stringified_parameters, slaves)
	
end = time.time()

completion = (end - start)

print "Matrix script took" + str(completion)
