from blazer.hpc.alcf import cooley, thetagpu

job1 = cooley.job(n=2, q="debug", A="datascience", password=True, script="/home/dgovoni/git/blazer/testcooley.sh")            
job2 = thetagpu.job(n=1, q="single-gpu", A="datascience", password=True, script="/home/dgovoni/git/blazer/testthetagpu.sh")
job3 = thetagpu.job(n=1, q="single-gpu", A="datascience", password=True, script="/home/dgovoni/git/blazer/testthetagpu.sh")

result = list("data" | job1 | job2 | job3)
print(result)

'''     
job2 = thetagpu.job(n=1, q="single-gpu", A="datascience", venv="/home/dgovoni/git/blazer/venv/bin/python", password=True,
                  code="/home/dgovoni/git/blazer/blazer/examples/example7.py")
'''