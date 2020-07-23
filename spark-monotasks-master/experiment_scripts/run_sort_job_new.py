"""
This script runs repeated jobs that each sort the same amount of data, using
different numbers of values for each key.
"""

import subprocess

import utils

target_total_data_gb = 600
# HDFS blocks are actually 128MB; round down here so that none of the output monotasks
# end up writing data to two different blocks, which we don't handle correctly.
hdfs_blocks_per_gb = 1024 / 105

subordinates = [subordinate_line.strip("\n") for subordinate_line in open("/root/spark/conf/subordinates").readlines()]
print "Running experiment assuming subordinates {}".format(subordinates)

num_machines = len(subordinates)
values_per_key_values = [10]
num_tasks = target_total_data_gb * hdfs_blocks_per_gb
# Just do one trial for now! When experiment is properly configured, do many trials.
num_shuffles = 3
cores_per_worker_values = [8]
cache_input_output_data = "false"

for cores_per_worker in cores_per_worker_values:
  for values_per_key in values_per_key_values:
    total_num_items = target_total_data_gb / (4.9 + values_per_key * 1.92) * (64 * 4000000)
    items_per_task =  int(total_num_items / num_tasks)
    data_filename = "randomData_{}_{}GB_105target".format(values_per_key, target_total_data_gb)
    use_existing_data_files = utils.check_if_hdfs_file_exists(data_filename)
    # The cores_per_worker parameter won't be used by the experiment; it's just included here for
    # convenience in how the log files are named.
    parameters = [num_tasks, num_tasks, items_per_task, values_per_key, num_shuffles,
      data_filename, use_existing_data_files, cache_input_output_data, cores_per_worker]
    stringified_parameters = ["{}".format(p) for p in parameters]
    command = ("/root/spark/bin/run-example monotasks.SortJob " +
               " ".join(stringified_parameters))
    print command
    subprocess.check_call(command, shell=True)

    utils.copy_and_zip_all_logs(stringified_parameters, subordinates)

    # Clear the buffer cache, to sidestep issue with machines dying.
    subprocess.check_call("/root/ephemeral-hdfs/sbin/subordinates.sh /root/spark-ec2/clear-cache.sh", shell=True)

    # Delete any sorted data.
    subprocess.check_call("/root/ephemeral-hdfs/bin/hadoop dfs -rm -r ./*sorted*", shell=True)

  # Future numbers of cores_per_worker don't need to re-generate the data files, and can instead just use the existing ones.
  use_existing_data_files = True
