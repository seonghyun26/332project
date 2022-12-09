import paramiko
from scp import SCPClient, SCPException


def createSSHClient(server, port, user, password):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, port, user, password)
    return client


def getFile(client, remote_path, local_path):
    scp = SCPClient(client.get_transport())
    scp.get(remote_path, local_path)


def putFile(client, file, remote_path):
    scp = SCPClient(client.get_transport())
    scp.put(file, remote_path)


if __name__ == "__main__":
    client = createSSHClient('141.223.16.227', '2201', 'cyan', 'cyane4#2002e140b29fc*482a')

    _, stdout, _ = client.exec_command('cd 332project; sbt "master/assembly"')
    exit_status = stdout.channel.recv_exit_status()
    if exit_status != 0:
        raise Exception("Bulid failed")

    _, stdout, _ = client.exec_command('cd 332project; sbt "worker/assembly"')
    exit_status = stdout.channel.recv_exit_status()
    if exit_status != 0:
        raise Exception("Bulid failed")

    getFile(client, '/home/cyan/332project/worker/target/scala-2.12/worker.jar', '/home/cyan/worker.jar')
    getFile(client, '/home/cyan/332project/master/target/scala-2.12/master.jar', '/home/cyan/master.jar')

