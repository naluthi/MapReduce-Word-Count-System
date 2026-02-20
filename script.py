import logging
from logging import info
import threading  # library functions for multi threading
import time  # library functions for time formats
import random  # library functions for generating random numbers

# The followings are global variables
writeable = True  # Allow Producer to start first
bufferCell = 0
t = time.time()
lock = threading.Lock()

def hotpoFunc(id, s, t):
	myId = id
	logging.info("range from %d to %d", s, t)
	n=s
	for e in range(s, t):
		n = e
		while (n > 1):
			if (n % 2 == 0): # if n is even
				n = n / 2 # Integer division
			else:
				n = 3 * n + 1
		e = e+1
	logging.info("Thread %s completed", myId)

def Producer(id):
	global writeable
	global bufferCell
	global t
	my_id = id
	info("Producer Thread %s: starts", my_id)
	i = 0
	while (t > time.time() - 10):  # Run producer for 10 seconds
		i = i + 1
		while (writeable == False):
			delay = random.randint(1, 3)
			info("Producer delays: %d seconds", delay)
			time.sleep(delay)
		lock.acquire()
		if (writeable == True):
			info("Producer %s: generates Item= %d", my_id, i)
			bufferCell = i
			writeable = False
		lock.release()

def Consumer(id):
	global writeable
	global bufferCell
	logging.info("Consumer Thread %s: starting", id)
	while x.is_alive():
		while (writeable == True) and x.is_alive():
			delay = random.randint(1, 5)
			info("Consumer %s delays %d: seconds", id, delay)
			time.sleep(delay)
		lock.acquire()
		if (writeable == False):
			info("Consumer %s: receives item=%d", id, bufferCell)
			writeable = True
		lock.release()


if __name__ == "__main__":
	format = "%(asctime)s: %(message)s"  # Define time & message format
	logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
	logging.info("Main thread starts before creating child threads")
	size = 2000000  # 2,000,000
	T = int(input("Please enter the number of threads that you want: "))
	section = int(size / T)  # Divide the data into T subsets
	print("section size is: ", section)
	for i in range(0, T):
		x = threading.Thread(target=hotpoFunc, args=(i, i * section + 1, (i + 1) * section))
		x.start()
		logging.info("Threads %d starts", i)
logging.info("Main thread: terminates")
