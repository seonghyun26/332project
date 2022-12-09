import shutil
import subprocess
import tempfile
import os
from itertools import chain, product
from multiprocessing import Process
from time import sleep
from typing import List, NoReturn

import gensort

NUM_WORKERS = 3
NUM_INPUT_DIRECTORIES = 3
NUM_BLOCKS = 5
NUM_TUPLES = 100

MASTER_JAR_PATH = 'master/target/scala-2.12'
WORKER_JAR_PATH = 'worker/target/scala-2.12'
JAVA_HOME = os.getenv('JAVA_HOME')

MASTER_PORT = 44123
# todo: set this limit to a higher value when the sorting algorithm is implemented
TIMEOUT = 20

def create_input_directories(num_workers: int, tempdir: str) -> List[List[str]]:
    list_of_directories = [
        [f'{tempdir}/worker-{w}/input-{i}' for i in range(NUM_INPUT_DIRECTORIES)]
        for w in range(num_workers)
    ]
    for dir in chain.from_iterable(list_of_directories):
        os.makedirs(dir)
    return list_of_directories

def create_output_directories(tempdir: str) -> List[str]:
    output_directories = [f'{tempdir}/worker-{w}/output' for w in range(NUM_WORKERS)]
    for dir in output_directories:
        os.makedirs(dir)
    return output_directories

def generate_input_tuples(input_directories: List[str]):
    for input_directory, block_number in product(input_directories, range(NUM_BLOCKS)):
        gensort.generate_tuples_to_file(NUM_TUPLES, f'{input_directory}/block-{block_number}')

def run_master():
    returncode = subprocess.run([f'{JAVA_HOME}/bin/java', '-jar', f'{MASTER_JAR_PATH}/master.jar', str(NUM_WORKERS), str(MASTER_PORT)]).returncode
    exit(returncode)

def run_worker(input_paths: List[str], output_path: str):
    returncode = subprocess.run([
        f'{JAVA_HOME}/bin/java', '-jar', f'{WORKER_JAR_PATH}/worker.jar', f'127.0.0.1:{MASTER_PORT}',
        '-I', *input_paths, '-O', output_path
    ]).returncode
    exit(returncode)

def do_sort(tempdir: str) -> bool:
    list_of_input_directories = create_input_directories(NUM_WORKERS, tempdir)
    output_directories = create_output_directories(tempdir)
    generate_input_tuples(chain.from_iterable(list_of_input_directories))
    # start test
    master = Process(target=run_master)
    master.start()
    sleep(3)
    workers = [
        Process(target=run_worker, args=[list_of_input_directories[worker_index], output_directories[worker_index]])
        for worker_index in range(NUM_WORKERS)
    ]
    for worker in workers:
        worker.start()
    # wait for test to finish
    master.join()
    for worker in workers:
        worker.join()
    return master.exitcode == 0 and all(worker.exitcode == 0 for worker in workers)

def validate(tempdir: str) -> bool:
    # todo: validate sorted data
    return True

def clean_up(tempdir: str):
    shutil.rmtree(tempdir)

def failure(msg: str, tempdir: str) -> NoReturn:
    print(msg)
    clean_up(tempdir)
    exit(1)

if __name__ == '__main__':
    tempdir = tempfile.mkdtemp()
    try:
        if not do_sort(tempdir):
            failure('the master or one of the workers did not exit successfully', tempdir)
        if not validate(tempdir):
            failure('validation on sorted data failed', tempdir)
    except Exception as e:
        failure(f'an exception occurred during test: {e}', tempdir)
    print('Test passed')
    clean_up(tempdir)