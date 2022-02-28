from contextlib import contextmanager
import traceback
from pipe import Pipe
import paramiko
import getpass


@Pipe
def job(data, q="debug", n=1, t=5, A='datascience', venv=None, script=None, code=None, password=False):
    print("THETA DATA",data)
    
    _ssh = paramiko.SSHClient()
    _ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    if password:
        pw = getpass.getpass("Password: ")
        _ssh.connect(hostname="cooley.alcf.anl.gov", username="dgovoni", password=pw)
    else:
        _ssh.connect(hostname="cooley.alcf.anl.gov", username="dgovoni") # sshkey?

    command = "hostname"
    _, stdout, _ = _ssh.exec_command(command)

    for line in stdout.read().splitlines():
        print("SSH: theta: stdout:", line)

    return "thetagpu" + data

class run(object):

    def __exit__(self, _type, value, _traceback):
        
        stack = traceback.extract_stack()
        file, end = self._get_origin_info(stack,'__exit__')
        self.end = end
        print("FILE",self.file,self.start,self.end)
        with open(file) as code:
            lines = code.readlines()
            print("THETA CODE:",lines[self.start:self.end])

    def __enter__(self):
        stack = traceback.extract_stack()
        file, start = self._get_origin_info(stack,'__enter__')
        self.start = start
        self.file = file

    def _get_origin_info(self, stack, label):
        origin = None
        for i, x in enumerate(stack[::-1]):
            if x[2] == label:
                origin = stack[::-1][i + 1]
                break

        return origin[0], origin[1]

