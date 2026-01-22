# Load Tests (P2-041)

Load testing infrastructure for AgentSpace Backend API using Locust.

## Quick Start

### Install Dependencies

```bash
cd backend
uv pip install locust
```

### Run Load Tests

**With Web UI (recommended for exploration):**

```bash
cd backend
locust -f tests/load/locustfile.py --host=http://localhost:8000
```

Then open http://localhost:8089 in your browser.

**Headless mode (for CI/automation):**

```bash
locust -f tests/load/locustfile.py --host=http://localhost:8000 \
    --headless -u 50 -r 10 -t 60s
```

Parameters:
- `-u 50`: 50 concurrent users
- `-r 10`: Spawn 10 users per second
- `-t 60s`: Run for 60 seconds

## User Types

The load tests simulate different user behaviors:

| User Type | Weight | Description |
|-----------|--------|-------------|
| DashboardUser | 10 | Most common - dashboard and browsing |
| AgentManagerUser | 3 | Hierarchy and downline operations |
| PayoutsUser | 2 | Commission tracking |
| SMSUser | 2 | SMS communication features |
| SearchUser | 2 | Search operations |
| ReferenceDataUser | 1 | Carriers and products |

## Configuration

Set these environment variables:

```bash
export TEST_AUTH_TOKEN="your-jwt-token"
export TEST_AGENCY_ID="your-agency-uuid"
```

## Running Specific Tests

Use tags to run specific test categories:

```bash
# Only dashboard tests
locust -f tests/load/locustfile.py --tags dashboard --host=http://localhost:8000

# Only P1 priority tests
locust -f tests/load/locustfile.py --tags p1 --host=http://localhost:8000

# Spike testing (stress test)
locust -f tests/load/locustfile.py --tags spike --host=http://localhost:8000 \
    --headless -u 100 -r 50 -t 30s
```

## Performance Targets

| Metric | Target | Critical |
|--------|--------|----------|
| P50 Response Time | < 200ms | < 500ms |
| P95 Response Time | < 500ms | < 1000ms |
| P99 Response Time | < 1000ms | < 2000ms |
| Error Rate | < 0.1% | < 1% |
| Throughput | > 100 RPS | > 50 RPS |

## CI Integration

Add to your CI pipeline:

```yaml
- name: Run Load Tests
  run: |
    cd backend
    locust -f tests/load/locustfile.py \
      --host=http://localhost:8000 \
      --headless \
      --users 50 \
      --spawn-rate 10 \
      --run-time 60s \
      --csv=loadtest-results \
      --only-summary
```

## Analyzing Results

Locust generates CSV files with `--csv` flag:
- `loadtest-results_stats.csv`: Request statistics
- `loadtest-results_stats_history.csv`: Time series data
- `loadtest-results_failures.csv`: Failed requests
- `loadtest-results_exceptions.csv`: Exceptions
