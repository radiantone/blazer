from contextlib import contextmanager
import traceback
from pipe import Pipe
import paramiko
import getpass
from paramiko_expect import SSHClientInteraction


@Pipe
def job(data, q="debug", n=1, t=5, A='datascience', venv=None, script=None, code=None, password=False):
    import datetime
    import time

    print("THETA DATA",data)

    _ssh = paramiko.SSHClient()
    _ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    if password:
        pw = getpass.getpass("Password: ")
        _ssh.connect(hostname="theta.alcf.anl.gov", username="dgovoni", password=pw)
    else:
        _ssh.connect(hostname="theta.alcf.anl.gov", username="dgovoni") # sshkey?
    root_prompt = '.*$ '

    
    if script:
        command = f"ssh thetagpusn1 qsub -t {t} -n {n} -q {q} {script}"
    else:
        command = f"ssh thetagpusn1 qsub -t {t} -n {n} -q {q} {venv} {code}"

    _, stdout, _ = _ssh.exec_command(command)

    job_id = None
    for line in stdout.read().splitlines():
        parts = line.split()
        if len(parts) == 1:
            job_id = int(parts[0])

    start = datetime.datetime.now()
    not_finished = True
    assert job_id is not None
    while not_finished:
        command = f"ssh thetagpusn1 qstat -u dgovoni | grep {job_id}"
        _, stdout, _ = _ssh.exec_command(command)
        for line in stdout.read().splitlines():
            print("LINE",line)
            if str(line).find("exiting") > -1:
                not_finished = False
        
        now = datetime.datetime.now()
        if now - start > datetime.timedelta(minutes=t):
            not_finished = False
        time.sleep(1)

    _ssh.close()
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

