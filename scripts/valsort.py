
import argparse
import os

from keyrange import get_keys, sort_check

parser = argparse.ArgumentParser(prog = "keyrange", description = "Print a key range (max, min)")
parser.add_argument('dir', type=str)
args = parser.parse_args()


if __name__ == "__main__":
    dir = args.dir

    file_names = []

    for path in os.listdir(dir):
        if os.path.isfile(os.path.join(dir, path)):
            file_names.append(os.path.join(dir, path))

    file_names.sort()
    sorted = {}

    prev_max_key = 0
    all_sorted = True

    for file in file_names:
        keys = get_keys(file)
        sorted[file] = sort_check(keys)

        min_key = min(keys)
        max_key = max(keys)

        if prev_max_key > min_key:
            all_sorted = False

        prev_max_key = max_key

    all_file_sorted = True
    not_sorted_file = None
    for file in sorted:
        if not sorted[file]:
            all_file_sorted = False
            not_sorted_file = file
            break

    if not all_sorted:
        print("Not sorted: Key range of some block are overlapped")
    elif not all_file_sorted:
        assert(not_sorted_file is not None)
        print(f"Not sorted: {not_sorted_file} are not sorted")
    else:
        print("All files are sorted")

