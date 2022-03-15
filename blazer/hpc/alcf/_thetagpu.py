import getpass
import logging
import traceback

import paramiko

from .then import Then


@Then
def job(
    user="nobody",
    q="debug",
    n=1,
    t=5,
    A="datascience",
    venv=None,
    script=None,
    code=None,
    password=False,
):
    class Job:
        def __init__(self):
            logging.debug("[THETA]: Job init")
            self._ssh = paramiko.SSHClient()
            self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        def login(self):
            logging.debug("[THETA]: Logging in from Job")
            pw = getpass.getpass("Theta MAF Password: ")
            self._ssh.connect(hostname="theta.alcf.anl.gov", username=user, password=pw)

            return self

        def __call__(self, *args, **kwargs):
            import datetime
            import time

            data = "stubbed"
            logging.info("[THETA]: Job call %s %s", args, kwargs)
            logging.debug("[THETA]: DATA %s %s %s", data, args, kwargs)

            if script:
                command = f"ssh thetagpusn1 qsub -t {t} -n {n} -q {q} {script}"
            else:
                command = f"ssh thetagpusn1 qsub -t {t} -n {n} -q {q} {venv} {code}"

            logging.debug("[THETA]: Executing command %s", command)
            _, stdout, _ = self._ssh.exec_command(command)
            logging.info("[THETA]: executed command %s", command)

            job_id = None
            for line in stdout.read().splitlines():
                parts = line.split()
                if len(parts) == 1:
                    job_id = int(parts[0])

            start = datetime.datetime.now()
            not_finished = True
            logging.info("[THETA]: job_id %s", job_id)
            assert job_id is not None
            while not_finished:
                command = f"ssh thetagpusn1 qstat -u dgovoni | grep {job_id}"
                _, stdout, _ = self._ssh.exec_command(command)
                for line in stdout.read().splitlines():
                    if str(line).find("exiting") > -1:
                        not_finished = False
                        logging.info(f"[THETA]: Job {job_id} has now completed.")

                now = datetime.datetime.now()
                if now - start > datetime.timedelta(minutes=t):
                    not_finished = False
                time.sleep(1)

            return "thetagpu" + str(data)

    return Job()


class run(object):
    def __exit__(self, _type, value, _traceback):

        stack = traceback.extract_stack()
        file, end = self._get_origin_info(stack, "__exit__")
        self.end = end
        logging.debug("FILE %s", self.file, self.start, self.end)
        with open(file) as code:
            lines = code.readlines()
            logging.debug("THETA CODE: %s", lines[self.start : self.end])

    def __enter__(self):
        stack = traceback.extract_stack()
        file, start = self._get_origin_info(stack, "__enter__")
        self.start = start
        self.file = file

    def _get_origin_info(self, stack, label):
        origin = None
        for i, x in enumerate(stack[::-1]):
            if x[2] == label:
                origin = stack[::-1][i + 1]
                break

        return origin[0], origin[1]
