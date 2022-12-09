import paramiko
from scp import SCPClient, SCPException


def createSSHClient(server, port, user, password):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, port, user, password)
    return client


ssh = createSSHClient('141.223.16.227', '2201', 'cyan', 'cyane4#2002e140b29fc*482a')
scp = SCPClient(ssh.get_transport())
scp.put('/home/cyan/dongyeop3813/a.txt', '/home/cyan/scp_test')
