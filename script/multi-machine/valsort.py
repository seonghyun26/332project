
import argparse
import os

from keyrange import get_keys, sort_check


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
            "sorted" : sort_check(keys),
            "min_key" : min(keys),
            "max_key" : max(keys),
        }
        dist_infos.append(dist_info)

    return merge_dist_info(dist_infos)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog = "keyrange", description = "Print a key range (max, min)")
    parser.add_argument('dir', type=str)
    args = parser.parse_args()

    print(dist_info_all_file(args.dir))
