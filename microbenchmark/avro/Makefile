CC=gcc

all: avro_test

avro_test: ../lib/jsmn/libjsmn.a ../json.o avro_test.o
	$(CC) $(CFLAGS) $^ -lavro -o $@

avro_test.o: avro_test.c nobench_schema.h
	$(CC) -c $(CFLAGS) $< -o $@

# %.o: %.c
# 	$(CC) -c $(CFLAGS) $^ -o $@

test: clean-test
	./avro_test ~/Downloads/nobench/nb_16000.out deser.out project.out mproject.out

clean: clean-build clean-test

clean-build:
	rm -f *.o *.a avro_test

clean-test:
	rm -f *.out *.schema *.db
