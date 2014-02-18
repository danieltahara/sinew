CC=g++

all: protobuf_test

protobuf_test: lib/jsmn/libjsmn.a json.o nobench.pb.o protobuf_test.o
	$(CC) $(CFLAGS) $^ -lprotobuf -o $@

protobuf_test.o: protobuf_test.cc nobench.pb.h
	$(CC) -c $(CFLAGS) $< -o $@

%.o: %.cc
	$(CC) -c $(CFLAGS) $< -o $@

test: clean-test
	./protobuf_test ~/Downloads/nobench/nb.out deser.out project.out

clean: clean-build clean-test

clean-build:
	rm -f *.o *.a protobuf_test

clean-test:
	rm -f *.out *.schema *.db
