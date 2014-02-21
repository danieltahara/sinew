all: sinew_test

sinew_test: ../json.o sinew_test.o document.o schema.o hash_table.o ../lib/jsmn/libjsmn.a

%.o: %.c
	$(CC) -c $(CFLAGS) $^ -o $@

test: clean-test
	./sinew_test ~/Downloads/nobench/nb_16000.out deser.out project.out mproject.out

clean: clean-build clean-test

clean-build:
	rm -f *.o *.a sinew_test

clean-test:
	rm -f *.out *.schema *.db
