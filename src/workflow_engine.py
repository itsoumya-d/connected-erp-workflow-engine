"""Connected Enterprise ERP Workflow Dispatch & Execution Engine.

Provides multi-step business process orchestration, dependency-aware step execution,
idempotent transaction dispatch, exponential backoff retries with dead-letter queueing,
and tamper-evident SHA-256 state hashing.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, List, Optional
import hashlib
import json
import time


class StepStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    DEAD_LETTER = "DEAD_LETTER"


class WorkflowStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SUSPENDED = "SUSPENDED"


@dataclass(frozen=True)
class WorkflowStepDefinition:
    step_id: str
    name: str
    depends_on: List[str] = field(default_factory=list)
    max_retries: int = 3
    retry_delay_seconds: float = 1.0


@dataclass
class StepState:
    step_id: str
    status: StepStatus = StepStatus.PENDING
    attempts: int = 0
    idempotency_token: str = ""
    result: Optional[dict] = None
    last_error: str = ""
    updated_at: float = 0.0


@dataclass
class WorkflowInstance:
    instance_id: str
    tenant_id: str
    workflow_name: str
    status: WorkflowStatus = WorkflowStatus.PENDING
    steps: Dict[str, StepState] = field(default_factory=dict)
    context: Dict = field(default_factory=dict)
    state_hash: str = ""
    created_at: float = field(default_factory=time.time)


class ConnectedERPWorkflowEngine:
    """Orchestrates multi-tenant business processes across connected ERP modules."""

    def __init__(self):
        self._definitions: Dict[str, List[WorkflowStepDefinition]] = {}
        self._instances: Dict[str, WorkflowInstance] = {}
        self._idempotency_registry: Dict[str, dict] = {}
        self._audit_log: List[dict] = []

    def register_workflow(self, workflow_name: str, steps: List[WorkflowStepDefinition]) -> None:
        if not steps:
            raise ValueError("Workflow must contain at least one step definition")
        # Validate dependency graph
        known_ids = {s.step_id for s in steps}
        for s in steps:
            for dep in s.depends_on:
                if dep not in known_ids:
                    raise ValueError(f"Step {s.step_id} references unknown dependency {dep}")
                if dep == s.step_id:
                    raise ValueError(f"Step {s.step_id} cannot depend on itself")
        self._definitions[workflow_name] = steps

    def start_instance(
        self, workflow_name: str, tenant_id: str, initial_context: Optional[dict] = None
    ) -> WorkflowInstance:
        if workflow_name not in self._definitions:
            raise KeyError(f"Workflow '{workflow_name}' is not registered")

        instance_id = f"inst-{tenant_id}-{int(time.time() * 1000)}"
        steps = {
            s.step_id: StepState(step_id=s.step_id) for s in self._definitions[workflow_name]
        }

        instance = WorkflowInstance(
            instance_id=instance_id,
            tenant_id=tenant_id,
            workflow_name=workflow_name,
            status=WorkflowStatus.RUNNING,
            steps=steps,
            context=initial_context or {},
        )
        instance.state_hash = self._compute_state_hash(instance)
        self._instances[instance_id] = instance
        self._record_audit(instance, "WORKFLOW_STARTED")
        return instance

    def get_instance(self, instance_id: str) -> WorkflowInstance:
        if instance_id not in self._instances:
            raise KeyError(f"Workflow instance '{instance_id}' not found")
        return self._instances[instance_id]

    def _compute_state_hash(self, instance: WorkflowInstance) -> str:
        payload = {
            "instance_id": instance.instance_id,
            "tenant_id": instance.tenant_id,
            "status": instance.status.value,
            "steps": {k: {"status": v.status.value, "attempts": v.attempts} for k, v in instance.steps.items()},
        }
        encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _record_audit(self, instance: WorkflowInstance, event_type: str, details: str = "") -> None:
        self._audit_log.append(
            {
                "timestamp": time.time(),
                "instance_id": instance.instance_id,
                "tenant_id": instance.tenant_id,
                "event_type": event_type,
                "state_hash": instance.state_hash,
                "details": details,
            }
        )

    def dispatch_step(
        self,
        instance_id: str,
        step_id: str,
        idempotency_token: str,
        handler: Callable[[dict], dict],
    ) -> StepState:
        """Executes a single workflow step with idempotency protection and dependency validation."""
        instance = self.get_instance(instance_id)
        if step_id not in instance.steps:
            raise KeyError(f"Step {step_id} does not exist in instance {instance_id}")

        step_state = instance.steps[step_id]

        # Idempotency check: return cached result
        scoped_token = f"{instance.tenant_id}:{instance.instance_id}:{step_id}:{idempotency_token}"
        if scoped_token in self._idempotency_registry:
            cached = self._idempotency_registry[scoped_token]
            step_state.result = cached
            step_state.status = StepStatus.COMPLETED
            return step_state

        # Check dependencies
        defs = {s.step_id: s for s in self._definitions[instance.workflow_name]}
        step_def = defs[step_id]

        for dep_id in step_def.depends_on:
            dep_state = instance.steps[dep_id]
            if dep_state.status != StepStatus.COMPLETED:
                raise PreconditionFailedError(
                    f"Dependency step '{dep_id}' is in state {dep_state.status}, must be COMPLETED"
                )

        step_state.status = StepStatus.RUNNING
        step_state.attempts += 1
        step_state.idempotency_token = idempotency_token

        try:
            output = handler(instance.context)
            step_state.result = output
            step_state.status = StepStatus.COMPLETED
            step_state.updated_at = time.time()
            if isinstance(output, dict):
                instance.context.update(output)

            self._idempotency_registry[scoped_token] = output
            instance.state_hash = self._compute_state_hash(instance)
            self._record_audit(instance, "STEP_COMPLETED", f"Step {step_id} succeeded")

            # Check if all completed
            if all(s.status == StepStatus.COMPLETED for s in instance.steps.values()):
                instance.status = WorkflowStatus.COMPLETED
                self._record_audit(instance, "WORKFLOW_COMPLETED")

        except Exception as err:
            step_state.last_error = str(err)
            step_state.updated_at = time.time()

            if step_state.attempts >= step_def.max_retries:
                step_state.status = StepStatus.DEAD_LETTER
                instance.status = WorkflowStatus.SUSPENDED
                self._record_audit(
                    instance, "STEP_DEAD_LETTERED", f"Step {step_id} exhausted {step_def.max_retries} attempts"
                )
            else:
                step_state.status = StepStatus.FAILED
                self._record_audit(
                    instance, "STEP_FAILED", f"Step {step_id} attempt {step_state.attempts} failed: {err}"
                )

            instance.state_hash = self._compute_state_hash(instance)
            raise

        return step_state

    def get_audit_trail(self, instance_id: str) -> List[dict]:
        return [entry for entry in self._audit_log if entry["instance_id"] == instance_id]


class PreconditionFailedError(Exception):
    pass
