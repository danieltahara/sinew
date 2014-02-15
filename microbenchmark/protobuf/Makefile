all: protobuf_test

sinew_test: ../json.o protobuf_test.o nobench.pb.o

%.o: %.c
	$(CC) -c $(CFLAGS) $< -o $@

clean:
	rm -f *.o *.a
