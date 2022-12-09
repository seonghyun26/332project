
import argparse

parser = argparse.ArgumentParser(prog = "keyrange", description = "Print a key range (max, min)")
parser.add_argument('filename', type=str)
args = parser.parse_args()


def get_filename():
    return args.filename


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


def sort_check(keys):
    for prev, next in zip(keys[:-1], keys[1:]):
        if prev > next:
            return False
    return True


if __name__ == "__main__":
    filename = get_filename()

    keys = get_keys(filename)

    keys.sort()

    print(f"sort check: {sort_check(keys)}")
    print(f"max key: {hex(keys[-1])}")
    print(f"min key: {hex(keys[0])}")