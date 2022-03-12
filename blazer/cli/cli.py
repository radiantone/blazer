import sys
import logging
logging.basicConfig(level=logging.INFO,
                    format='%(filename)s: '    
                            '%(levelname)s: '
                            '%(funcName)s(): '
                            '%(lineno)d:\t'
                            '%(message)s')


logger = logging.getLogger(__name__)
import click

@click.group(invoke_without_command=True)
@click.option("--debug", is_flag=True, default=False, help="Debug switch")
@click.pass_context
def cli(context, debug):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    if debug:
        logging.basicConfig(
            format="%(asctime)s : %(name)s %(levelname)s : %(message)s", level=logging.DEBUG
        )
    else:
        logging.basicConfig(
            format="%(asctime)s : %(name)s %(levelname)s : %(message)s", level=logging.INFO
        )

    if len(sys.argv) == 1:
        click.echo(context.get_help())

import tempfile,os
def readcmd(cmd):
    ftmp = tempfile.NamedTemporaryFile(suffix='.out', prefix='tmp', delete=False)
    fpath = ftmp.name
    if os.name=="nt":
        fpath = fpath.replace("/","\\") # forwin
    ftmp.close()
    os.system(cmd + " > " + fpath)
    data = ""
    with open(fpath, 'r') as file:
        data = file.read()
        file.close()
    os.remove(fpath)
    return data

@cli.command(name="run", help="Run a shell command")
@click.option("-s", "--shell", is_flag=True, default=False, help="Run command in shell")
@click.option("-m", "--mpi", is_flag=True, default=False, help="Enable MPI")
@click.option("-a", "--args", is_flag=True, default=False, help="Add jobid and uuid as args")
@click.option("-n","--numjobs", default=1, help="Number of times to run")
@click.option("-c","--command", default="echo hello", help="Command to run in \"'s")
@click.pass_context
def run(context, shell, mpi, args, numjobs, command):
    from uuid import uuid4
    import blazer
    from blazer.hpc.mpi import rank, stream, host
    

    def getjobs():
        for i in range(0,numjobs):
            uuid = str(uuid4())
            
            if args:
                _command = f"{command} {i} {uuid}"
            else:
                _command = command
            yield {'command':_command, 'jobid':i, 'uuid':uuid}
        
    def run_cmd(cmd):
        import os
        import subprocess

        if not cmd:
            return 0

        logging.debug("run_cmd: %s",cmd)
        if rank:
            logging.debug("Running job Host[%s] Rank[%s] %s: jobid [%s] uuid [%s]",host, rank,cmd['command'],cmd['jobid'],cmd['uuid'])
        else:
            logging.debug("Running job %s: jobid [%s] uuid [%s]",cmd['command'],cmd['jobid'],cmd['uuid'])

        #result = subprocess.run(cmd['command'], shell=shell, stdout=subprocess.PIPE)

        #return result.stdout.decode('utf-8').strip()
        return readcmd(cmd['command'])
    if mpi:
        with blazer.begin():
            results = stream(getjobs(), run_cmd) 
            for result in results:
                blazer.print("RESULT:",result)
    else:
        for cmd in getjobs():
            print("RESULT:",run_cmd(cmd))




