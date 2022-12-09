from scp import SCPClient
import tempfile

from ssh import createSSHClient
from machine_info import MASTER_IP_ADDRESS, MASTER_PORT, WORKER_IP_ADDRESS, WORKER_PORTS


def getFile(client, remote_path, local_path):
    scp = SCPClient(client.get_transport())
    scp.get(remote_path, local_path)


def putFile(client, file, remote_path):
    scp = SCPClient(client.get_transport())
    scp.put(file, remote_path)


if __name__ == "__main__":
    client = createSSHClient(MASTER_IP_ADDRESS, MASTER_PORT)
    # Package the project and get the jar files
    with tempfile.TemporaryDirectory() as tempdir:
        getFile(client, '/home/cyan/332project/worker/target/scala-2.12/worker.jar', f'{tempdir}/worker.jar')
        getFile(client, '/home/cyan/332project/master/target/scala-2.12/master.jar', f'{tempdir}/master.jar')
        client.close()
        for port in WORKER_PORTS:
            client = createSSHClient(WORKER_IP_ADDRESS, port)
            putFile(client, f'{tempdir}/worker.jar', '/home/cyan/worker.jar')
            putFile(client, f'{tempdir}/master.jar', '/home/cyan/master.jar')
            putFile(client, '/home/cyan/332project/worker.py', '/home/cyan/worker.py')
            client.close()
