all: myChannels

myChannels: myChannels.c
	gcc -o myChannels myChannels.c -pthread -lm

clean:
	rm -f myChannels *.o