all: avro_test

avro_test.o: avro_test.c nobench_schema.h ../json.o
	$(CC) -c $(CFLAGS) $< -o $@ -Llibavro

%.o: %.c
	$(CC) -c $(CFLAGS) $< -o $@

clean:
	rm -f *.o *.a
