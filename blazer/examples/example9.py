from blazer.hpc.alcf import cooley, thetagpu
from blazer.hpc.local import parallel
from blazer.hpc.local import partial as p
from blazer.hpc.local import pipeline

# Log into each cluster using MAF password from MobilePASS
cooleyjob = cooley.job(
    user="dgovoni",
    n=1,
    q="debug",
    A="datascience",
    password=True,
    script="/home/dgovoni/git/blazer/testcooley.sh",
).login()
thetajob = thetagpu.job(
    user="dgovoni",
    n=1,
    q="single-gpu",
    A="datascience",
    password=True,
    script="/home/dgovoni/git/blazer/testthetagpu.sh",
).login()


def hello(data, *args):
    data = "Hello " + str(data)
    print(data)
    return data


# Sequential workflow
pipeline([p(hello, "a pipeline task!"), p(cooleyjob, "some pipeline data1")])

# Parallel workflow with local tasks too
result = parallel(
    [
        p(thetajob, "some data2"),
        p(hello, "a local task!"),
        p(hello, "another local task!"),
        p(cooleyjob, "some data1"),
    ]
)

# Or a pipeline cross-cluster using chaining
cooleyjob("some data").then(hello).then(thetajob).then(hello)

print("Done")
