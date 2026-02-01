.PHONY: build test test-cover bench clean run

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

# Run tests
test:
	@echo "Running tests..."
	$(GOTEST) -v ./...

# Run tests with coverage
test-cover:
	@echo "Running tests with coverage..."
	$(GOTEST) -v -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	$(GOTEST) -bench=. -benchmem ./...

# Clean build artifacts
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html

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
	@echo "  make build       Build the binary"
	@echo "  make test        Run tests"
	@echo "  make test-cover  Run tests with coverage report"
	@echo "  make bench       Run benchmarks"
	@echo "  make clean       Clean build artifacts"
	@echo "  make run         Build and run a single node"
	@echo "  make run-node1   Run as node1 in a cluster"
	@echo "  make run-node2   Run as node2 in a cluster"
	@echo "  make run-node3   Run as node3 in a cluster"
	@echo "  make deps        Download dependencies"
	@echo "  make fmt         Format code"
	@echo "  make lint        Run linter"
	@echo "  make build-all   Build for multiple platforms"
	@echo "  make help        Show this help"
