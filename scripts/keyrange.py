
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

if __name__ == "__main__":
    filename = get_filename()
    max_key = int.from_bytes(b'\x00' * 10, byteorder='big')
    min_key = int.from_bytes(b'\xff' * 10, byteorder='big')

    with open(filename, 'rb') as f:
        while True:
            key = get_key(read_tuple(f))
            if key == b'':
                break

            assert(len(key) == 10)

            key = int.from_bytes(key, byteorder='big')

            max_key = max(key, max_key)
            min_key = min(key, min_key)

    print(f"max key: {hex(max_key)}")
    print(f"min key: {hex(min_key)}")