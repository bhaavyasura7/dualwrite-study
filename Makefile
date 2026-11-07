.PHONY: up down reset ps logs smoke smoke-order smoke-checker smoke-crash test lint accept run run-all run-must run-perf pause merge verify backup demo

up: ## start the base stack and wait until every service is healthy
	docker compose up -d --wait --wait-timeout 300

down: ## stop the stack, keeping data
	docker compose down

reset: ## stop the stack and delete all data
	docker compose --profile brokers3 --profile app down -v --remove-orphans
	docker volume rm -f dualwrite_kafka-b3-1 dualwrite_kafka-b3-2 dualwrite_kafka-b3-3

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=100

smoke: ## verify the Milestone 1 acceptance criteria against the running stack
	./deploy/smoke.sh

smoke-order: ## verify the Milestone 2 acceptance criteria (P1 end to end)
	./deploy/smoke-order.sh

smoke-checker: ## verify the Milestone 3 acceptance criteria (checker on real cycles)
	./deploy/smoke-checker.sh

smoke-crash: ## verify the Milestone 4 acceptance criteria (real crashes, exact damage)
	./deploy/smoke-crash.sh

test:
	go vet ./...
	go test ./... -count=1

lint:
	golangci-lint run ./...

accept: test lint smoke smoke-order smoke-checker smoke-crash ## every acceptance check; each must pass
	@echo "acceptance passed"

run: ## run selected vectors: make run VECTORS=A-1 ARGS="-patterns P1"
	go run ./cmd/runner -vectors "$(VECTORS)" $(ARGS)

run-all: ## run the catalogue; SHARD=i/n runs this laptop's share
	go run ./cmd/runner $(if $(SHARD),-shard $(SHARD)) $(ARGS)

merge: ## combine results/results.*.csv from all laptops into results.csv
	go run ./cmd/runner -merge

verify: ## check a run: make verify RUN=results/runs/<commit>
	cd $(RUN) && shasum -a 256 -c --quiet SHA256SUMS
	go run ./cmd/runner -priority must -exclude-groups perf -verify $(RUN)/results.csv

backup: ## push results/ to the private results repo
	./deploy/backup.sh

demo: up ## one crash cycle end to end; writes results/demo.csv
	go run ./cmd/runner -cycles A-1_P1_r01 -allow-dirty -results results/demo.csv
	@python3 -c "import csv; [print({k: r[k] for k in ('cycle_id', 'status', 'events_total', 'lost', 'ghost', 'duplicates')}) for r in csv.DictReader(open('results/demo.csv'))]"

run-must: ## final must-run correctness vectors; resumable, stop with make pause
	caffeinate -i go run ./cmd/runner -priority must -exclude-groups perf $(ARGS)

run-perf: ## performance vectors (P-1) on their own; the Mac must be fully idle
	caffeinate -i go run ./cmd/runner -vectors P-1 $(ARGS)

pause: ## stop the running run after its current cycle; rerun to resume
	touch results/STOP
	@echo "the run will stop after its current cycle"
