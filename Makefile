.PHONY: tests

tests:
	make -C tests

test-flight-sql:
	make -C tests test-flight-sql
