all: sinew_test

sinew_test: ../json.o sinew_test.o document.o schema.o hash_table.o ../lib/jsmn/libjsmn.a

%.o: %.c
	$(CC) -c $(CFLAGS) $< -o $@

clean:
	rm -f *.o *.a
