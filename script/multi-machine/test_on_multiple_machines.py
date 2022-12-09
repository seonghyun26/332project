import os
from multiprocessing import Process, Queue

from testcase import Testcase
from ssh import createSSHClient, exec_command_blocking
from worker import merge_dist_info
from setup import setup_machines
from machine_info import MASTER_IP_ADDRESS, MASTER_PORT, WORKER_IP_ADDRESS, WORKER_PORTS

TESTCASE_DIRECTORY = 'script/multi-machine/testcase'

class TestcaseRunner:
    def __init__(self, testcase: Testcase):
        self.testcase = testcase

    def _run_master(self, num_workers: int):
        ssh = createSSHClient(MASTER_IP_ADDRESS, MASTER_PORT)
        exec_command_blocking(ssh, f'/usr/bin/java -jar /home/cyan/master.jar {num_workers}')

    def _run_worker(self, worker_index: int, to_runner: Queue):
        ssh = createSSHClient(WORKER_IP_ADDRESS, WORKER_PORTS[worker_index])
        stdout, stderr = exec_command_blocking(ssh, f'python3 /home/cyan/worker.py {worker_index} testcase/{self.testcase.config_file_name}')
        print('=========[stdout]=========')
        print(stdout)
        print('=========[stderr]=========')
        print(stderr)

    def run(self):
        num_workers = self.testcase.num_workers
        from_workers = [Queue() for _ in range(num_workers)]
        print('Running master...')
        master_process = Process(target=self._run_master, args=[num_workers])
        print('Running workers...')
        worker_processes = [Process(target=self._run_worker, args=[worker['index'], from_workers[i]]) for i, worker in enumerate(self.testcase.workers)]
        master_process.start()
        for worker in worker_processes:
            worker.start()
        master_process.join()
        for worker in worker_processes:
            worker.join()

if __name__ == '__main__':
    print('Setting up machines... (this may take about a minute)')
    setup_machines()
    print('Setup complete.')
    filenames = os.listdir(TESTCASE_DIRECTORY)
    for i, config_file_name in enumerate(filenames):
        print(f'Running testcase {config_file_name} ({i} / {len(filenames)})')
        testcase = Testcase(f'{TESTCASE_DIRECTORY}/{config_file_name}')
        TestcaseRunner(testcase).run()
