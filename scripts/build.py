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

    getFile(client, '/home/cyan/332project/worker/target/scala-2.12/worker.jar', '/home/cyan/worker.jar')
    getFile(client, '/home/cyan/332project/master/target/scala-2.12/master.jar', '/home/cyan/master.jar')

    getFile(client, '/home/cyan/332project/scripts/worker.py', '/home/cyan/worker.py')

    client.close()

    machines = [
        {
            'IP': '141.223.16.227',
            'port': str(i)
        }
        for i in range(2203, 2212, 1)
    ]

    print(machines)

    for machine in machines:
        client = createSSHClient(machine['IP'], machine['port'], 'cyan', 'cyane4#2002e140b29fc*482a')

        putFile(client, '/home/cyan/worker.jar', '/home/cyan/worker.jar')
        putFile(client, '/home/cyan/master.jar', '/home/cyan/master.jar')
        putFile(client, '/home/cyan/worker.py', '/home/cyan/worker.py')

        client.close()