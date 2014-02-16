CC=gcc

all: avro_test

avro_test: ../lib/jsmn/libjsmn.a ../json.o avro_test.o
	$(CC) $(CFLAGS) $^ -lavro -o $@

%.o: %.c
	$(CC) -c $(CFLAGS) $^ -o $@

clean: clean-build clean-test

clean-build:
	rm -f *.o *.a avro_test

clean-test:
	rm -f *.out *.schema *.db
