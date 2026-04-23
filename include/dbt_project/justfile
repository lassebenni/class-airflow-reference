DBT := "dbt"

# Show available recipes
default:
    @just --list

# Verify dbt can connect to Azure PostgreSQL
debug:
    {{DBT}} debug --profiles-dir .

# Install dbt packages from packages.yml (only needed from v4 onward)
deps:
    {{DBT}} deps

# Build all models from sources through fct_trips, with tests interleaved
build:
    {{DBT}} build --select +fct_trips --profiles-dir .

# Build models only (no tests) — useful while iterating locally
run:
    {{DBT}} run --select +fct_trips --profiles-dir .

# Run every test in the project
test:
    {{DBT}} test --profiles-dir .

# Drop and rebuild everything from a clean target/
clean-build:
    {{DBT}} clean
    just build
