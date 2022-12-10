import argparse
import os
import json

MASTER_HOST = "2.2.2.101"
MASTER_PORT = 55555

NUM_TUPLE_PER_BLOCK = 335544

INPUT_DIR = "./input"
OUTPUT_DIR = "./output"


def make_input_dir(dir_cnt):
    os.system(f"mkdir {INPUT_DIR}")

    for i in range(dir_cnt):
        os.system(f"mkdir {INPUT_DIR}/{i}")


def make_output_dir():
    os.system(f"mkdir {OUTPUT_DIR}")


def make_blocks(input_dir, block_cnt):
    global offset
    for i in range(block_cnt):
        block_filename = f"{input_dir}/partition.{i}"

        cmd = f"gensort/gensort -b{offset} "
        cmd += f"{NUM_TUPLE_PER_BLOCK} "
        cmd += block_filename
        os.system(cmd)

        offset += NUM_TUPLE_PER_BLOCK


def load_config(filename):
    with open(filename, 'r') as f:
        config = json.load(f)
        return config


def parse_config(config, worker_idx):

    block_cnt_list = None
    offset = 0

    for worker in config["workers"]:
        if worker["index"] == worker_idx:
            block_cnt_list = worker["blocks"]
        elif worker["index"] < worker_idx:
            offset += sum(worker["blocks"])

    offset = offset * NUM_TUPLE_PER_BLOCK

    assert(block_cnt_list is not None)

    return block_cnt_list, offset


def make_command(num_input_dirs):
    input_dirs = ""
    for idx in range(num_input_dirs):
        input_dirs += f"./input/{idx} "
    output_dir = "./output"

    cmd = f"/usr/bin/java -jar worker.jar {MASTER_HOST}:{MASTER_PORT} "
    cmd += f"-I {input_dirs} "
    cmd += f"-O {output_dir} "
    return cmd


def read_tuple(f):
    return f.read(100)


def get_key(t):
    return t[:10]


def get_keys(filename):
    keys = []
    with open(filename, 'rb') as f:
        while True:
            key = get_key(read_tuple(f))
            if key == b'':
                break

            assert(len(key) == 10)
            key = int.from_bytes(key, byteorder='big')
            keys.append(key)

    return keys


def is_sorted(keys):
    for prev, next in zip(keys[:-1], keys[1:]):
        if prev > next:
            return False
    return True


def get_filenames(dir):
    file_names = []
    for path in os.listdir(dir):
        if os.path.isfile(os.path.join(dir, path)):
            file_names.append(os.path.join(dir, path))
    file_names.sort()
    return file_names


def merge_dist_info(dist_info_list):
    all_sorted = True
    for dist in dist_info_list:
        if not dist["sorted"]:
            all_sorted = False

    key_overlapped = False
    key_list = []
    for idx, dist in enumerate(dist_info_list):
        key_list.append((dist["min_key"], idx))
        key_list.append((dist["max_key"], idx))

    key_list.sort()
    assert(len(key_list) % 2 == 0)

    for i in range(0, len(key_list), 2):
        # If sorted, consecutive key must be from same block.
        if key_list[i][1] != key_list[i+1][1]:
            key_overlapped = True

    min_key = min([dist["min_key"] for dist in dist_info_list])
    max_key = max([dist["max_key"] for dist in dist_info_list])

    return {
        "sorted": not key_overlapped and all_sorted,
        "min_key": min_key,
        "max_key": max_key,
        }


def dist_info_all_file(output_dir):
    file_names = get_filenames(output_dir)
    dist_infos = []

    prev_max_key = 0
    key_overlapped = False

    for file in file_names:
        keys = get_keys(file)
        dist_info = {
            "sorted" : is_sorted(keys),
            "min_key" : min(keys),
            "max_key" : max(keys),
        }
        dist_infos.append(dist_info)

    return merge_dist_info(dist_infos)

def get_gensort():
    os.system('bash get_gensort.sh')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Worker Tester",
        description="Woker script executor for testing",
    )

    parser.add_argument("worker_idx", type=int, help="Worker index of this machine")
    parser.add_argument("config_file", type=str, help="configuration file name")

    args = parser.parse_args()
    worker_idx = args.worker_idx
    config_filename = args.config_file

    config = load_config(config_filename)
    block_cnt_list, offset = parse_config(config, worker_idx)
    num_input_dirs = len(block_cnt_list)

    # set up directory structure
    # use index as input dir name
    get_gensort()
    make_input_dir(num_input_dirs)
    make_output_dir()

    # make blocks
    for idx, block_cnt in enumerate(block_cnt_list):
        make_blocks(f"{INPUT_DIR}/{idx}", block_cnt)

    cmd = make_command(num_input_dirs)
    os.system(cmd)

    dist_info = dist_info_all_file(OUTPUT_DIR)
    print(dist_info)

    os.system(f"rm -rf {INPUT_DIR}")
    os.system(f"rm -rf {OUTPUT_DIR}")
