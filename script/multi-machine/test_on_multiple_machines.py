import os
from multiprocessing import Process, Queue
import re
import json
from time import sleep

from testcase import Testcase
from ssh import createSSHClient, exec_command_blocking
from valsort import merge_dist_info
from setup import setup_machines
from machine_info import MASTER_IP_ADDRESS, MASTER_PORT, WORKER_IP_ADDRESS, WORKER_PORTS

TESTCASE_DIRECTORY = 'script/multi-machine/testcase'

class TestcaseRunner:
    def __init__(self, testcase: Testcase):
        self.testcase = testcase

    def _run_master(self, num_workers: int):
        try:
            ssh = createSSHClient(MASTER_IP_ADDRESS, MASTER_PORT)
            exec_command_blocking(ssh, f'/usr/bin/java -jar /home/cyan/master.jar {num_workers}')
        except KeyboardInterrupt:
            cmd = "kill -s kill `ps x | grep master.jar | grep -v grep | awk '{print $1}'`"
            exec_command_blocking(ssh, cmd)
            ssh.close()

    def _run_worker(self, worker_index: int, to_runner: Queue):
        try:
            ssh = createSSHClient(WORKER_IP_ADDRESS, WORKER_PORTS[worker_index])
            stdout, stderr = exec_command_blocking(ssh, f'python3 /home/cyan/worker.py {worker_index} testcase/{self.testcase.config_file_name}')
            print('=========[stdout]=========')
            print(stdout)
            print('=========[stderr]=========')
            print(stderr)
            try:
                result = json.loads(re.search(r'\{.+\}', stdout).group(0).replace('\'', '"'))
            except json.decoder.JSONDecodeError:
                result = None
            to_runner.put(result)
        except KeyboardInterrupt:
            cmd = "kill -s kill `ps x | egrep 'worker.py|worker.jar' | grep -v grep | awk '{print $1}'`"
            exec_command_blocking(ssh, cmd)
            ssh.close()

    def run(self) -> bool:
        num_workers = self.testcase.num_workers
        from_workers = [Queue() for _ in range(num_workers)]
        master_process = Process(target=self._run_master, args=[num_workers])
        worker_processes = [Process(target=self._run_worker, args=[worker['index'], from_workers[i]]) for i, worker in enumerate(self.testcase.workers)]
        try:
            print('Running master...')
            master_process.start()
            print('Waiting for master... (3 secs)')
            sleep(3)
            print('Running workers...')
            for worker in worker_processes:
                worker.start()
            results = [from_workers[i].get() for i in range(num_workers)]
            final_result = merge_dist_info(results)
            print('=========[final result]=========')
            print(final_result)
            master_process.join()
            for worker in worker_processes:
                worker.join()
            return False
        except KeyboardInterrupt:
            master_process.join()
            for worker in worker_processes:
                worker.join()
            return True

if __name__ == '__main__':
    do_setup = input('Setup machines? (y/n): ')
    if do_setup == 'y':
        print('Setting up machines...')
        setup_machines()
        print('Setup complete')
    else:
        print('Skipping setup...')
    filenames = os.listdir(TESTCASE_DIRECTORY)
    for i, config_file_name in enumerate(filenames):
        print(f'Running testcase {config_file_name} ({i + 1} / {len(filenames)})')
        testcase = Testcase(f'{TESTCASE_DIRECTORY}/{config_file_name}')
        interrupted = TestcaseRunner(testcase).run()
        if interrupted:
            print('Interrupted by user')
            break
