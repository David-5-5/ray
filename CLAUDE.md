# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Ray is a unified framework for scaling AI and Python applications. It consists of a core distributed runtime and a set of AI libraries for ML compute.

Key abstractions:
- **Tasks**: Stateless functions executed in the cluster
- **Actors**: Stateful worker processes created in the cluster
- **Objects**: Immutable values accessible across the cluster

Higher-level AI libraries:
- Data: Scalable Datasets for ML
- Train: Distributed Training
- Tune: Scalable Hyperparameter Tuning
- RLlib: Scalable Reinforcement Learning
- Serve: Scalable and Programmable Serving
- Workflows: Durable workflows

## Architecture

### Core System Components

Ray has a layered architecture:

1. **C++ Core (`src/ray/`)**:
   - **Raylet**: The node manager that runs on each machine. Responsible for task scheduling, worker management, and object management.
     - `src/ray/raylet/` - Main raylet implementation
     - Scheduling logic, worker pool management, IPC
   - **Core Worker**: Per-process execution engine for tasks and actors
     - `src/ray/core_worker/` - Core worker implementation
     - Task submission, actor management, object references
   - **GCS (Global Control Store)**: Centralized state management
     - `src/ray/gcs/` - Redis-backed global state storage
     - Actor/task/job table management, pub/sub
   - **Object Manager**: Distributed object store
     - `src/ray/object_manager/` - Plasma object store integration
     - Object transfer between nodes

2. **Python API (`python/ray/`)**:
   - `python/ray/actor.py` - Actor API and management
   - `python/ray/remote_function.py` - Task API (`@ray.remote`)
   - `python/ray/_raylet.pyx` - Cython bindings to the C++ core
   - `python/ray/runtime_context.py` - Runtime context for jobs/tasks/actors

3. **Cross-Language Support**:
   - Java bindings in `java/`
   - C++ API bindings in `cpp/`

### Key Data Flow

1. User calls `ray.init()` - Connects to or starts a Ray cluster
2. `@ray.remote` decorator marks functions/classes for remote execution
3. Task/actor submission goes through Core Worker → Raylet
4. Raylet schedules work on workers based on resources and locality
5. Objects stored in Plasma object store, accessible via reference counting

## Directory Structure

- `src/ray/`: C++ core distributed runtime (raylet, object manager, GCS, scheduling, RPC, etc.)
- `python/ray/`: Python API and core components
- `cpp/`: C++ API bindings
- `java/`: Java API bindings
- `rllib/`: Reinforcement learning library
- `python/ray/data`: Ray Datasets for scalable data processing
- `python/ray/train`: Distributed training library
- `python/ray/tune`: Hyperparameter tuning
- `python/ray/serve`: Model serving
- `python/ray/autoscaler`: Cluster autoscaling
- `python/ray/dashboard`: Ray dashboard for monitoring and debugging
- `ci/`: CI scripts and utilities
- `doc/`: Documentation (Sphinx/reStructuredText)

## Build Commands

### Install development version
```bash
# Install Ray in development mode
pip install -e "python[all]" -c python/requirements_compiled.txt

# Skip C++ compilation (faster build for Python-only changes)
RAY_DISABLE_EXTRA_CPP=1 pip install -e "python[all]"
```

### Build with Bazel
```bash
# Full build using bazel
bazel build //src/ray/...

# Build Python wheel
cd python && ./build-wheel-manylinux2014.sh  # Linux
cd python && ./build-wheel-macos.sh          # macOS
```

## Lint and Format

### Run all lint checks
```bash
./ci/lint/lint.sh pre_commit
```

### Format code
```bash
# Run pre-commit on all files
pre-commit run --all-files

# Format all scripts
./ci/lint/format.sh --all-scripts
```

### Individual lint checks
```bash
# Check C++ format
./ci/lint/check-git-clang-format-output.sh

# Check Bazel format
./ci/lint/check-bazel-buildifier.sh

# Check docstyle
./ci/lint/check-docstyle.sh

# Check API annotations
./ci/lint/lint.sh api_annotations

# Check for banned words
./ci/lint/lint.sh banned_words

# pydoclint (docstring linting)
# Uses google style with baseline file at ci/lint/pydoclint-baseline.txt
```

## Test Commands

### Run all tests in a file
```bash
pytest python/ray/tests/test_<file_name>.py
```

### Run a single test
```bash
pytest python/ray/tests/test_<file_name>.py::test_<test_name>
```

### Run tests with verbose output
```bash
pytest -v python/ray/tests/test_<file_name>.py
```

### Run C++ tests
```bash
bazel test //src/ray/...
```

### Run specific CI checks locally
```bash
./ci/ci.sh  # Replicate CI checks locally
```

## Testing Patterns

### Common Fixtures
Ray tests use pytest fixtures defined in:
- `python/ray/tests/conftest.py` - Main test fixtures
- `python/ray/_private/test_utils.py` - Test utilities (wait_for_condition, etc.)
- `python/ray/cluster_utils.py` - Cluster/AutoscalingCluster classes

### Key Test Utilities
```python
from ray._private.test_utils import (
    wait_for_condition,  # Poll until condition is met
    find_free_port,      # Find available network port
)
from ray.cluster_utils import Cluster, AutoscalingCluster

# Pattern: Use wait_for_condition for async operations
wait_for_condition(lambda: some_state == expected, timeout=10)
```

### Test Structure
- Unit tests: `python/ray/tests/unit/` - Fast, no Ray cluster needed
- Integration tests: `python/ray/tests/` - Require `ray.init()`
- Library-specific tests: `python/ray/data/tests/`, `python/ray/serve/tests/`, etc.

## Development Workflow

1. Ray uses **pre-commit** for code quality checks. Install pre-commit hooks with:
   ```bash
   pre-commit install
   ```

2. Linting tools used:
   - **ruff**: Python linting and import sorting (with `--fix` support)
   - **black**: Python code formatting
   - **clang-format**: C/C++ formatting
   - **buildifier**: Bazel BUILD file formatting
   - **cpplint**: C++ style checks
   - **mypy**: Static type checking for select files
   - **pydoclint**: Google-style docstring validation

3. Key environment variables for development:
   - `RAY_DISABLE_EXTRA_CPP=1` - Disable C++ compilation (speeds up Python development)
   - `RAY_BUILD_CORE=0` - Skip building core
   - `SKIP_BAZEL_BUILD=1` - Skip Bazel build step
   - `RAY_NUM_REDIS_GET_RETRIES=2` - Faster test retries

4. Debugging:
   - Use `RAY_BACKEND_LOG_LEVEL=debug` for verbose logging
   - Dashboard available by default at port 8265
   - Log files stored in `/tmp/ray/session_*/logs/`

## Documentation Style Guide

For files in `doc/`, follow these conventions (from `.cursor/rules/ray-docs-style.mdc`):

- **Voice**: Active voice, use contractions (don't, can't, won't)
- **Headings**: Sentence case, imperative mood for procedures
- **Code examples**: Complete sentence lead-ins, not just "Example:"
- **Word choice**: "such as" not "like", "ID" not "id", "through" not "via"
- **Component names**: Ray Serve, Ray Data, Ray Train, Ray Core (capitalized)
- **Sphinx directives**: Use `:::{note}`, `:::{warning}`, `:doc:`, `:ref:`

## Common Pitfalls

1. **Ray Shutdown**: Always call `ray.shutdown()` after tests to clean up
2. **Object Lifetimes**: Don't hold references to Ray objects outside scope
3. **Resource Specification**: Use `num_cpus`, `num_gpus` in `@ray.remote` decorators
4. **Serialization**: Objects passed to tasks must be serializable (cloudpickle)
5. **Async Tasks**: Use `wait_for_condition` for async operation verification

## Important Files

- `python/ray/__init__.py` - Main Ray API exports
- `python/ray/_raylet.pyx` - Core Cython bindings
- `src/ray/raylet/scheduling/cluster_resource_scheduler.cc` - Task scheduling
- `src/ray/gcs/gcs_server/gcs_actor_manager.cc` - Actor management
