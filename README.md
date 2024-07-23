The files will be calculated based on the nth rows. So if there are 2 files then k size from file 1 + k size from file 1 will be outputted on to line 1 of the output file. If there are 6 files, then there will be 6 k buffer sizes added together and outputted to each line. This way ensures the same answers no matter the lock or checkpoint.

I am doing this on CSIL so I do not have access to the root folder, so I could not test with the absolute path, but it should work, since the files are just being read the same way.

I kept the order of the arguments the same as in the assignment document so: buffer size, number of threads, metadata path, lock config, checkpointing, and then output file path.

I also have a few errors such as if strings instead of numbers are entered in the argument or if files are not a multiple of threads.

My code will also take global checkpoint = 0 and lock config = 1 as defaults if a number greater than what is expected is enetered as an arguments.

I am printing out calculating and done just to show the program is working and when it finishes.

I zipped the metadata file with a folder to show how I called the function and this is the same example as the one in the assignment document. The command I used for example was: ./myChannels 2 2 metadata.txt 1 0 output.txt