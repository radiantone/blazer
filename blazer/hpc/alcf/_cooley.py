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
            logging.debug("[COOLEY]: Job init")
            self._ssh = paramiko.SSHClient()
            self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        def login(self):
            logging.debug("[COOLEY]: Logging in from Job")
            pw = getpass.getpass("Cooley MAF Password: ")
            self._ssh.connect(
                hostname="cooley.alcf.anl.gov", username=user, password=pw
            )

            return self

        def __call__(self, *args, **kwargs):
            logging.info("[COOLEY]: Job call %s %s", args, kwargs)
            import datetime
            import time

            data = "stubbed"

            if script:
                command = f"qsub -t {t} -n {n} -q {q} {script}"
            else:
                command = f"qsub -t {t} -n {n} -q {q} {venv} {code}"

            logging.info("[COOLEY]: executing command %s", command)
            _, stdout, _ = self._ssh.exec_command(command)
            logging.info("[COOLEY]: executed command %s", command)

            job_id = None
            for line in stdout.read().splitlines():
                logging.info("[COOLEY][qsub]: %s", line)
                parts = line.split()
                if len(parts) == 1:
                    job_id = int(parts[0])

            logging.info("[COOLEY]: job_id %s", job_id)
            start = datetime.datetime.now()
            not_finished = True
            is_exiting = False
            while not_finished:
                command = f"qstat -u {user} | grep {job_id}"
                _, stdout, _ = self._ssh.exec_command(command)
                lines = stdout.read().splitlines()

                if len(lines) == 0 and is_exiting:
                    logging.info(f"[COOLEY]: Job {job_id} has now completed.")
                    not_finished = False

                for line in lines:
                    if str(line).find("exiting") > -1:
                        if q == "debug":
                            logging.info(
                                "[COOLEY]: Waiting for job to exit debug queue."
                            )
                            is_exiting = True
                        else:
                            not_finished = False

                    # Determine if job succeeded or failed.
                    # If failed, throw exception
                    if False:
                        raise Exception()

                now = datetime.datetime.now()
                if now - start > datetime.timedelta(minutes=t):
                    not_finished = False
                time.sleep(1)

            logging.debug("[COOLEY]: exiting...")

            return "cooley" + str(data)

    logging.debug("Returning job from cooley")
    return Job()


class run(object):
    def __exit__(self, _type, value, _traceback):

        stack = traceback.extract_stack()
        file, end = self._get_origin_info(stack, "__exit__")
        self.end = end
        logging.debug("FILE", self.file, self.start, self.end)
        with open(file) as code:
            lines = code.readlines()
            logging.debug("COOLEY CODE: %s", lines[self.start : self.end])

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
