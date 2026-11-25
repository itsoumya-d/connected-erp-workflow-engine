# connected-erp-workflow-engine

Connected ERP workflow orchestration and agentic task dispatch engine: models multi-step enterprise business processes, idempotent step execution, exponential backoff retries with dead-letter queueing, and tamper-evident audit state transitions.

## Architecture & Guarantees

- **Multi-Step ERP Process Execution**: Sequences multi-system enterprise workflows (e.g. order validation, inventory reservation, ledger entry, invoice generation) across distributed microservices.
- **Strict Idempotency**: Guarantees exactly-once step execution via idempotency keys to prevent duplicate transaction postings during network retries.
- **Resilience & Dead-Letter Handling**: Configurable retry policies with exponential backoff and automated dead-letter routing upon exhaustion.
- **Audit Traceability**: Records state transitions with SHA-256 state hashes and timing metadata for complete audit compliance.

## Run Tests Locally

```bash
python3 -m unittest discover -s tests -p "test_*.py" -v
```

## CI/CD

Automated GitHub Actions workflow runs unit tests and linting on every push and pull request.
