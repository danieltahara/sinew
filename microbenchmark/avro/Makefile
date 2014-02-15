all: avro_test

avro_test: avro_test.o nobench_schema.h ../json.o ../lib/jsmn/libjsmn.a
	$(CC) -c $(CFLAGS) $< -o $@ -Llibavro

%.o: %.c
	$(CC) -c $(CFLAGS) $< -o $@

clean:
	rm -f *.o *.a
