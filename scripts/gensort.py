import os
import subprocess
import tempfile
from typing import Tuple, Iterable

KEY_LENGTH = 10
VALUE_LENGTH = 90

def _gen(bytestream: bytes):
    l = KEY_LENGTH + VALUE_LENGTH
    for offset in range(0, len(bytestream), l):
        yield bytestream[offset : offset + l]

def _bytes_to_tuples(bytes_: bytes) -> Iterable[Tuple[bytes, bytes]]:
    return map(lambda kv: (kv[:KEY_LENGTH], kv[KEY_LENGTH:]), _gen(bytes_))

def generate_tuples_to_file(num_tuples: int, filename: str):
    subprocess.run(['./gensort/gensort', '-s', str(num_tuples), filename])

def generate_tuples(num_tuples: int) -> Iterable[Tuple[bytes, bytes]]:
    filename = tempfile.mktemp()
    generate_tuples_to_file(num_tuples, filename)
    with open(filename, 'rb') as f:
        tuples = _bytes_to_tuples(f.read())
    os.remove(filename)
    return tuples

def load_tuples(filename: str) -> Iterable[Tuple[bytes, bytes]]:
    with open(filename, 'rb') as f:
        return _bytes_to_tuples(f.read())

def sort_in_file(filename: str):
    tuples = load_tuples(filename)
    with open(filename, 'wb') as f:
        for k, v in sorted(tuples):
            f.write(k + v)

def validate_file(filename: str) -> Tuple[bool, str]:
    p = subprocess.run(['./gensort/valsort', filename], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return p.returncode == 0, p.stdout.decode()
