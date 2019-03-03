.PHONY: run
run:
	env BIGTABLE_EMULATOR_HOST=localhost:9035 go run main.go

.PHONY: start-bigtable
start-bigtable:
	docker run --rm -it -p 9035:9035 --name bigtable shopify/bigtable-emulator -cf dev.events.x
