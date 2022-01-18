build:
	mpicc main.c -o testapd2

run:
	mpirun -np 6 testapd2 file.in

clean:
	rm testapd2