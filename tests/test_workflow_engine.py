import unittest
from src.workflow_engine import (
    ConnectedERPWorkflowEngine,
    PreconditionFailedError,
    StepStatus,
    WorkflowStatus,
    WorkflowStepDefinition,
)


class TestConnectedERPWorkflowEngine(unittest.TestCase):
    def setUp(self):
        self.engine = ConnectedERPWorkflowEngine()
        self.steps = [
            WorkflowStepDefinition("validate_order", "Validate Order", depends_on=[]),
            WorkflowStepDefinition("reserve_inventory", "Reserve Inventory", depends_on=["validate_order"]),
            WorkflowStepDefinition(
                "post_ledger", "Post to General Ledger", depends_on=["reserve_inventory"], max_retries=2
            ),
        ]
        self.engine.register_workflow("order_to_cash", self.steps)

    def test_linear_workflow_execution(self):
        inst = self.engine.start_instance(
            "order_to_cash", "tenant-acme", {"order_id": "ORD-99", "amount": 1500.0}
        )
        self.assertEqual(inst.status, WorkflowStatus.RUNNING)

        # Step 1
        s1 = self.engine.dispatch_step(
            inst.instance_id,
            "validate_order",
            "token-val-01",
            lambda ctx: {"order_valid": True, "credit_approved": True},
        )
        self.assertEqual(s1.status, StepStatus.COMPLETED)
        self.assertTrue(inst.context["order_valid"])

        # Step 2
        s2 = self.engine.dispatch_step(
            inst.instance_id,
            "reserve_inventory",
            "token-inv-01",
            lambda ctx: {"reservation_id": "RES-8821"},
        )
        self.assertEqual(s2.status, StepStatus.COMPLETED)

        # Step 3
        s3 = self.engine.dispatch_step(
            inst.instance_id,
            "post_ledger",
            "token-led-01",
            lambda ctx: {"journal_entry_id": "JRN-5541"},
        )
        self.assertEqual(s3.status, StepStatus.COMPLETED)
        self.assertEqual(inst.status, WorkflowStatus.COMPLETED)

    def test_dependency_precondition_rejection(self):
        inst = self.engine.start_instance("order_to_cash", "tenant-acme")

        # Try to run reserve_inventory without running validate_order
        with self.assertRaises(PreconditionFailedError):
            self.engine.dispatch_step(
                inst.instance_id,
                "reserve_inventory",
                "token-inv-fail",
                lambda ctx: {"reservation_id": "FAIL"},
            )

    def test_idempotent_step_dispatch(self):
        inst = self.engine.start_instance("order_to_cash", "tenant-acme")
        call_count = [0]

        def handler(ctx):
            call_count[0] += 1
            return {"tax_computed": 120.0}

        # First call
        res1 = self.engine.dispatch_step(inst.instance_id, "validate_order", "idem-token-101", handler)
        self.assertEqual(call_count[0], 1)
        self.assertEqual(res1.result["tax_computed"], 120.0)

        # Retry with same idempotency token
        res2 = self.engine.dispatch_step(inst.instance_id, "validate_order", "idem-token-101", handler)
        self.assertEqual(call_count[0], 1)  # Handler was not executed again
        self.assertEqual(res2.result["tax_computed"], 120.0)

    def test_retry_and_dead_letter_exhaustion(self):
        inst = self.engine.start_instance("order_to_cash", "tenant-acme")

        # Complete step 1 and 2
        self.engine.dispatch_step(inst.instance_id, "validate_order", "t1", lambda c: {})
        self.engine.dispatch_step(inst.instance_id, "reserve_inventory", "t2", lambda c: {})

        # Attempt 1 on post_ledger fails
        with self.assertRaises(RuntimeError):
            self.engine.dispatch_step(
                inst.instance_id,
                "post_ledger",
                "t3-attempt-1",
                lambda c: (_ for _ in ()).throw(RuntimeError("DB Connection Timeout")),
            )
        step = inst.steps["post_ledger"]
        self.assertEqual(step.status, StepStatus.FAILED)
        self.assertEqual(step.attempts, 1)

        # Attempt 2 fails (exhausts max_retries = 2)
        with self.assertRaises(RuntimeError):
            self.engine.dispatch_step(
                inst.instance_id,
                "post_ledger",
                "t3-attempt-2",
                lambda c: (_ for _ in ()).throw(RuntimeError("DB Connection Timeout")),
            )
        self.assertEqual(step.status, StepStatus.DEAD_LETTER)
        self.assertEqual(inst.status, WorkflowStatus.SUSPENDED)

    def test_audit_trail_logging(self):
        inst = self.engine.start_instance("order_to_cash", "tenant-acme")
        self.engine.dispatch_step(inst.instance_id, "validate_order", "t1", lambda c: {})

        audit = self.engine.get_audit_trail(inst.instance_id)
        events = [a["event_type"] for a in audit]
        self.assertIn("WORKFLOW_STARTED", events)
        self.assertIn("STEP_COMPLETED", events)
        for entry in audit:
            self.assertTrue(len(entry["state_hash"]) == 64)


if __name__ == "__main__":
    unittest.main()
