all: sinew_test

sinew_test: ../json.o sinew_test.o document.o schema.o hash_table.o ../lib/jsmn/libjsmn.a

%.o: %.c
	$(CC) -c $(CFLAGS) $< -o $@

clean: clean-build clean-test

clean-build:
	rm -f *.o *.a sinew_test

clean-test:
	rm -f *.out *.schema *.db
