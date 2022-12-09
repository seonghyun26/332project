import paramiko
from paramiko import SSHClient

USER = 'cyan'
KEYFILE = '/home/cyan/.ssh/id_rsa'

def createSSHClient(server, port):
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(server, port, username=USER, key_filename=KEYFILE)
    return ssh
