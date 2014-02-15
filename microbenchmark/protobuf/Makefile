all: protobuf_test

protobuf_test: ../json.o protobuf_test.o nobench.pb.o ../lib/jsmn/libjsmn.a

%.o: %.c
	$(CC) -c $(CFLAGS) $< -o $@

clean:
	rm -f *.o *.a
