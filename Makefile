.PHONY: build test test-cover test-unit test-integration bench bench-cluster clean run docker-build docker-up docker-down docker-test docker-logs

# Build variables
BINARY_NAME=aegis
BUILD_DIR=./bin
CMD_DIR=./cmd/aegis

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Build the binary
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME) $(CMD_DIR)
	@echo "Build complete: $(BUILD_DIR)/$(BINARY_NAME)"

# Run all tests
test:
	@echo "Running all tests..."
	$(GOTEST) -v -timeout 300s ./...

# Run unit tests only (fast)
test-unit:
	@echo "Running unit tests..."
	$(GOTEST) -v -short ./pkg/...

# Run integration tests (multi-node)
test-integration:
	@echo "Running integration tests (5-10 node clusters)..."
	$(GOTEST) -v -timeout 300s ./tests/integration/...

# Run tests with coverage
test-cover:
	@echo "Running tests with coverage..."
	$(GOTEST) -v -coverprofile=coverage.out ./pkg/...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Run all benchmarks
bench:
	@echo "Running benchmarks..."
	$(GOTEST) -bench=. -benchmem ./pkg/...

# Run cluster benchmarks (5-10 nodes)
bench-cluster:
	@echo "Running cluster benchmarks..."
	$(GOTEST) -bench=. -benchmem -timeout 300s ./tests/integration/...

# Clean build artifacts
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html

# Docker: Build images
docker-build:
	@echo "Building Docker images..."
	docker compose build

# Docker: Start 5-node cluster
docker-up:
	@echo "Starting 5-node Docker cluster..."
	docker compose up -d
	@echo "Waiting for cluster to start..."
	@sleep 10
	docker compose ps

# Docker: Start 10-node cluster
docker-up-10:
	@echo "Starting 10-node Docker cluster..."
	docker compose -f docker compose.10nodes.yml up -d
	@echo "Waiting for cluster to start..."
	@sleep 15
	docker compose -f docker compose.10nodes.yml ps

# Docker: Stop cluster
docker-down:
	@echo "Stopping Docker cluster..."
	docker compose down -v
	docker compose -f docker compose.10nodes.yml down -v 2>/dev/null || true

# Docker: Run tests against Docker cluster (from host)
docker-test:
	@echo "Running Go tests against Docker cluster..."
	go test -v ./tests/docker/...

# Docker: Run tests from inside Docker network (recommended)
docker-test-internal:
	@echo "Running tests from inside Docker network..."
	docker compose run --rm aegis-test

# Docker: Run benchmark from inside Docker network
docker-bench:
	@echo "Running benchmark inside Docker network..."
	docker compose run --rm aegis-test \
		--seeds=aegis-node1:7700,aegis-node2:7700,aegis-node3:7700,aegis-node4:7700,aegis-node5:7700 \
		--test=benchmark \
		--ops=10000 \
		--workers=20

# Docker: Run comprehensive cluster tests
docker-test-full:
	@echo "Running comprehensive Docker cluster tests..."
	@$(MAKE) docker-down
	@$(MAKE) docker-build
	@$(MAKE) docker-up
	@sleep 20
	@echo "Checking cluster health..."
	@docker compose ps
	@echo ""
	@$(MAKE) docker-test-internal
	@echo ""
	@echo "Checking cluster logs for errors..."
	@docker compose logs --tail=50 | grep -i "error\|panic\|fatal" || echo "No errors found"

# Docker: Show cluster logs
docker-logs:
	docker compose logs -f

# Docker: Show cluster status
docker-status:
	@echo "=== Cluster Status ==="
	@docker compose ps
	@echo ""
	@echo "=== Node Health ==="
	@for port in 7700 7710 7720 7730 7740; do \
		response=$$(curl -s http://localhost:$$port/health 2>/dev/null); \
		echo "Port $$port: $$response"; \
	done
	@echo ""
	@echo "=== Cluster Members (from node1) ==="
	@curl -s http://localhost:7700/cluster/members 2>/dev/null | head -10 || echo "Could not get members"

# Docker: Run client test (legacy)
docker-client-test:
	@echo "Running Go client tests against Docker cluster..."
	go test -v ./tests/docker/... -run TestDockerCluster

# Run a single node
run: build
	$(BUILD_DIR)/$(BINARY_NAME)

# Run node 1 of a cluster
run-node1: build
	$(BUILD_DIR)/$(BINARY_NAME) --id node1 --client-addr :7000 --gossip-addr :7002

# Run node 2 of a cluster
run-node2: build
	$(BUILD_DIR)/$(BINARY_NAME) --id node2 --client-addr :7010 --gossip-addr :7012 --seeds localhost:7002

# Run node 3 of a cluster
run-node3: build
	$(BUILD_DIR)/$(BINARY_NAME) --id node3 --client-addr :7020 --gossip-addr :7022 --seeds localhost:7002

# Download dependencies
deps:
	$(GOMOD) download
	$(GOMOD) tidy

# Format code
fmt:
	@echo "Formatting code..."
	$(GOCMD) fmt ./...

# Run linter (requires golangci-lint)
lint:
	@echo "Running linter..."
	golangci-lint run

# Build for multiple platforms
build-all: clean
	@echo "Building for multiple platforms..."
	@mkdir -p $(BUILD_DIR)
	GOOS=linux GOARCH=amd64 $(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64 $(CMD_DIR)
	GOOS=linux GOARCH=arm64 $(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-arm64 $(CMD_DIR)
	GOOS=darwin GOARCH=amd64 $(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-amd64 $(CMD_DIR)
	GOOS=darwin GOARCH=arm64 $(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-arm64 $(CMD_DIR)
	GOOS=windows GOARCH=amd64 $(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME)-windows-amd64.exe $(CMD_DIR)
	@echo "Build complete for all platforms"

# Show help
help:
	@echo "AegisKV - Distributed Key-Value Cache"
	@echo ""
	@echo "Usage:"
	@echo "  make build            Build the binary"
	@echo "  make test             Run all tests"
	@echo "  make test-unit        Run unit tests only (fast)"
	@echo "  make test-integration Run integration tests (multi-node)"
	@echo "  make test-cover       Run tests with coverage report"
	@echo "  make bench            Run unit benchmarks"
	@echo "  make bench-cluster    Run cluster benchmarks (5-10 nodes)"
	@echo "  make clean            Clean build artifacts"
	@echo "  make run              Build and run a single node"
	@echo "  make run-node1        Run as node1 in a cluster"
	@echo "  make run-node2        Run as node2 in a cluster"
	@echo "  make run-node3        Run as node3 in a cluster"
	@echo "  make deps             Download dependencies"
	@echo "  make fmt              Format code"
	@echo "  make lint             Run linter"
	@echo "  make build-all        Build for multiple platforms"
	@echo ""
	@echo "Docker Commands:"
	@echo "  make docker-build     Build Docker images"
	@echo "  make docker-up        Start 5-node Docker cluster"
	@echo "  make docker-up-10     Start 10-node Docker cluster"
	@echo "  make docker-down      Stop Docker cluster"
	@echo "  make docker-test      Run Go tests against Docker cluster (from host)"
	@echo "  make docker-test-internal  Run tests from inside Docker network"
	@echo "  make docker-test-full Run comprehensive cluster tests"
	@echo "  make docker-bench     Run benchmark inside Docker network"
	@echo "  make docker-status    Show cluster status and health"
	@echo "  make docker-logs      Show cluster logs"
	@echo ""
	@echo "  make help             Show this help"
