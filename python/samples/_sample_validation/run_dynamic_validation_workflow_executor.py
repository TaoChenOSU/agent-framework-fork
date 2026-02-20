# Copyright (c) Microsoft. All rights reserved.

from typing import Sequence

from _sample_validation.models import (
    ExecutionResult,
    RunResult,
    RunStatus,
    WorkflowCreationResult,
)
from agent_framework import Executor, WorkflowContext, handler
from agent_framework_github_copilot import GitHubCopilotAgent


async def stop_agents(agents: Sequence[GitHubCopilotAgent]) -> None:
    """Stop all GitHub Copilot agents used by the nested workflow."""
    for agent in agents:
        try:
            await agent.stop()
        except Exception:
            continue


class RunDynamicValidationWorkflowExecutor(Executor):
    """Executor that runs the nested workflow created in the previous step."""

    def __init__(self) -> None:
        super().__init__(id="run_dynamic_workflow")

    @handler
    async def run(self, creation: WorkflowCreationResult, ctx: WorkflowContext[ExecutionResult]) -> None:
        """Run the nested workflow and emit execution results."""
        if creation.workflow is None:
            await ctx.send_message(ExecutionResult(results=[]))
            return

        print("\nRunning nested concurrent workflow...")
        prompt = (
            "Validate your assigned sample using your instructions. Return only the JSON object in the required schema."
        )

        try:
            events = await creation.workflow.run(prompt)
            outputs = events.get_outputs()
            if outputs and isinstance(outputs[0], ExecutionResult):
                await ctx.send_message(outputs[0])
            else:
                fallback_results = [
                    RunResult(
                        sample=sample,
                        status=RunStatus.ERROR,
                        output="",
                        error="Nested workflow did not return an ExecutionResult.",
                    )
                    for sample in creation.samples
                ]
                await ctx.send_message(ExecutionResult(results=fallback_results))
        finally:
            await stop_agents(creation.agents)
