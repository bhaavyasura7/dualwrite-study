.PHONY: up down reset ps logs smoke smoke-order smoke-checker smoke-crash test

up: ## start the base stack and wait until every service is healthy
	docker compose up -d --wait --wait-timeout 300

down: ## stop the stack, keeping data
	docker compose down

reset: ## stop the stack and delete all data
	docker compose down -v

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
	go test ./...
