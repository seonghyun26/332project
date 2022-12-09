from paramiko import SSHClient
import os
from multiprocessing import Process, Queue

from testcase import Testcase
from ssh import createSSHClient
from worker import merge_dist_info
from setup import setup_machines
from machine_info import MASTER_IP_ADDRESS, MASTER_PORT, WORKER_IP_ADDRESS, WORKER_PORTS

MASTER_SCRIPT = 'setup.py'
WORKER_SCRIPT = 'worker.py'
TESTCASE_DIRECTORY = 'script/testcase'

class TestcaseRunner:
    def __init__(self, testcase: Testcase):
        self.testcase = testcase

    @staticmethod
    def _run_script_via_ssh(ssh: SSHClient, script: str):
        executor = 'python3' if script.endswith('.py') else 'bash'
        _ = ssh.exec_command(f'{executor} {script}')

    def _run_master(self):
        ssh = createSSHClient(MASTER_IP_ADDRESS, MASTER_PORT)
        self._run_script_via_ssh(ssh, MASTER_SCRIPT)

    def _run_worker(self, worker_index: int):
        ssh = createSSHClient(WORKER_IP_ADDRESS, WORKER_PORTS[worker_index])
        self._run_script_via_ssh(ssh, WORKER_SCRIPT)

    def run(self):
        from_master = Queue()
        from_workers = [Queue() for _ in range(self.testcase.num_workers)]
        master_process = Process(target=self._run_master, args=[from_master])
        worker_processes = [Process(target=self._run_worker, args=[from_workers[i]]) for i in range(self.testcase.num_workers)]
        master_process.start()
        for worker in worker_processes:
            worker.start()
        master_process.join()
        for worker in worker_processes:
            worker.join()

if __name__ == '__main__':
    setup_machines()
    for config_file_name in os.listdir(TESTCASE_DIRECTORY):
        testcase = Testcase(f'{TESTCASE_DIRECTORY}/{config_file_name}')
        TestcaseRunner(testcase).run()
