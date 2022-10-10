from __future__ import print_function
from base64 import decodebytes
from datetime import datetime
import os
import paramiko
from scp import SCPClient
import socket
from ssh2.session import Session


class FastSshCopy():
    def __init__(self, host_addr, host_port, username, password="", private_key_file="", passphrase=""):
        self.username = username
        self.password = password
        self.private_key_file = private_key_file
        self.passphrase = passphrase
        self.host_addr = host_addr
        self.host_port = host_port
        self.session = Session()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def connect(self):
        self.sock.connect((self.host_addr, self.host_port))
        self.session.handshake(self.sock)
        if self.password != "":
            self.session.userauth_password(self.username, self.password)
        elif self.private_key_file != "":
            self.session.userauth_publickey_fromfile(username=self.username, privatekey=self.private_key_file, passphrase=self.passphrase)
        else:
            self.session.agent_auth(self.username)

    def send(self, local_path, remote_path):
        fileinfo = os.stat(local_path)
        chan = self.session.scp_send64(remote_path, fileinfo.st_mode & 0o777, fileinfo.st_size,
                            fileinfo.st_mtime, fileinfo.st_atime)
        print("Starting SCP of local file %s to remote %s:%s" % (local_path, self.host_addr, remote_path))
        now = datetime.now()
        with open(local_path, 'rb') as local_fh:
            for data in local_fh:
                chan.write(data)
        taken = datetime.now() - now
        rate = (fileinfo.st_size / (1024000.0)) / taken.total_seconds()
        print("Finished writing remote file in %s, transfer rate %s MB/s" % (taken, rate))


class SshCopy():
    def __init__(self, host_addr, host_key_data, host_port, username, password="", private_key_file="", passphrase=""):
        self.host_key = paramiko.RSAKey(data=decodebytes(host_key_data))
        self.username = username
        self.password = password
        self.private_key_file = private_key_file
        self.passphrase = passphrase
        self.client = paramiko.SSHClient()
        self.host_addr = host_addr
        self.host_port = host_port
        self.host_id = "[%s]:%d" % (host_addr, host_port)
        self.client.get_host_keys().add(self.host_id, 'ssh-rsa', self.host_key)

    def connect(self):
        if self.password != "":
            self.client.connect(hostname=self.host_addr, port=str(self.host_port), username=self.username, password=self.password)
        elif self.private_key_file != "":
            self.client.connect(hostname=self.host_addr, port=str(self.host_port), username=self.username, key_filename=self.private_key_file, passphrase=self.passphrase)

    def put(self, local_path, remote_path):
        # SCPClient takes a paramiko transport as its only argument
        scp = SCPClient(self.client.get_transport())
        scp.put(local_path, remote_path)
        scp.close()

    def get(self, local_path, remote_path):
        # SCPClient takes a paramiko transport as its only argument
        scp = SCPClient(self.client.get_transport())
        scp.get(remote_path, local_path)
        scp.close()
