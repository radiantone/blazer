for i in {1,2,3,4,5,6,7,8,10,11};
do
echo "Running example$i"
time mpirun -n 4 venv/bin/python blazer/examples/example$i.py
echo "-----------------------------------------------------------------"
done
./bin/rungpu.sh