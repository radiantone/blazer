from blazer.hpc.alcf import cooley, thetagpu

job1 = cooley.job(n=2, q="debug", A="datascience", venv="/home/dgovoni/miniconda4/bin/python", password=True,
                  code="/home/dgovoni/git/blazer/blazer/examples/example1.py")
job2 = thetagpu.job(n=1, q="single-gpu", A="datascience", venv="/home/dgovoni/git/blazer/venv/bin/python", password=True,
                  code="/home/dgovoni/git/blazer/blazer/examples/example7.py")

print("data" | job1 | job2)