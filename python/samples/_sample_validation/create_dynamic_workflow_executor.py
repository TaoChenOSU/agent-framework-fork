# Copyright (c) Microsoft. All rights reserved.


import logging

from _sample_validation.discovery import DiscoveryResult
from _sample_validation.models import (
    ExecutionResult,
    RunResult,
    RunStatus,
    SampleInfo,
    ValidationConfig,
    WorkflowCreationResult,
)
from agent_framework import (
    AgentExecutorRequest,
    AgentExecutorResponse,
    AgentResponse,
    Executor,
    Message,
    WorkflowContext,
    handler,
)
from agent_framework.github import GitHubCopilotAgent
from agent_framework.orchestrations import ConcurrentBuilder
from copilot import PermissionRequest, PermissionRequestResult
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class AgentResponseFormat(BaseModel):
    status: str
    output: str
    error: str


def agent_prompt(sample: SampleInfo) -> str:
    """Build per-sample instructions for a GitHub Copilot validator agent."""
    return (
        "You are validating exactly one Python sample.\n"
        f"Sample path: {sample.relative_path}\n"
        "Analyze the code and execute it. Determine if it runs successfully, fails, or times out.\n"
        "The sample can be interactive. If it is interactive, response to the sample when prompted "
        "based on your analysis of the code. You do not need to consult human on what to respond\n"
        "Return ONLY valid JSON with this schema:\n"
        "{\n"
        '  "status": "success|failure|timeout|error",\n'
        '  "output": "short summary of the result and what you did if the sample was interactive",\n'
        '  "error": "error details or empty string"\n'
        "}\n\n"
    )


def parse_agent_json(text: str) -> AgentResponseFormat:
    """Parse JSON object from an agent response."""
    stripped = text.strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        return AgentResponseFormat.model_validate_json(stripped)

    start = stripped.find("{")
    end = stripped.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("No JSON object found in response")

    return AgentResponseFormat.model_validate_json(stripped[start : end + 1])


def status_from_text(value: str) -> RunStatus:
    """Convert a string value to RunStatus with safe fallback."""
    normalized = value.strip().lower()
    for status in RunStatus:
        if status.value == normalized:
            return status
    return RunStatus.ERROR


def prompt_permission(request: PermissionRequest, context: dict[str, str]) -> PermissionRequestResult:
    """Permission handler that always approves."""
    kind = request.get("kind", "unknown")
    logger.debug(f"[Permission Request: {kind}] ({context})Automatically approved for sample validation.")
    return PermissionRequestResult(kind="approved")


class CustomAgentExecutor(Executor):
    """Executor that runs a GitHub Copilot agent and returns its response.

    We need the custome executor to wrap the agent call in a try/except to ensure that any exceptions are caught and
    returned as error responses, otherwise an exception in one agent could crash the entire workflow.
    """

    def __init__(self, agent: GitHubCopilotAgent):
        super().__init__(id=agent.id)
        self.agent = agent

    @handler
    async def handle_request(self, request: AgentExecutorRequest, ctx: WorkflowContext[AgentExecutorResponse]) -> None:
        """Execute the agent with the given request and return its response."""
        try:
            response = await self.agent.run(request.messages)
            await ctx.send_message(AgentExecutorResponse(executor_id=self.id, agent_response=response))
        except Exception as ex:
            logger.error(f"Error executing agent {self.agent.id}: {ex}")
            error_response = AgentExecutorResponse(
                executor_id=self.id,
                agent_response=AgentResponse(
                    messages=Message(
                        role="assistant",
                        text=AgentResponseFormat(
                            status="error",
                            output="",
                            error=str(ex),
                        ).model_dump_json(),
                    )
                ),
            )
            await ctx.send_message(error_response)


class CreateConcurrentValidationWorkflowExecutor(Executor):
    """Executor that builds a nested concurrent workflow with one agent per sample."""

    def __init__(self, config: ValidationConfig):
        super().__init__(id="create_dynamic_workflow")
        self.config = config

    @handler
    async def create(
        self,
        discovery: DiscoveryResult,
        ctx: WorkflowContext[WorkflowCreationResult],
    ) -> None:
        """Create a nested concurrent workflow with N GitHub Copilot agents."""
        sample_count = len(discovery.samples)
        print(f"\nCreating nested concurrent workflow with {sample_count} parallel GitHub agents...")

        if sample_count == 0:
            await ctx.send_message(WorkflowCreationResult(samples=[], workflow=None, agents=[]))
            return

        agents: list[GitHubCopilotAgent] = []
        sample_by_agent_id: dict[str, SampleInfo] = {}

        for index, sample in enumerate(discovery.samples, start=1):
            agent_id = f"sample_validator_{index}"
            agent = GitHubCopilotAgent(
                id=agent_id,
                name=agent_id,
                instructions=agent_prompt(sample),
                default_options={"on_permission_request": prompt_permission, "timeout": 180},  # type: ignore
            )
            agents.append(agent)
            sample_by_agent_id[agent_id] = sample

        async def aggregate_results(results: list[AgentExecutorResponse]) -> ExecutionResult:
            run_results: list[RunResult] = []

            for result in results:
                executor_id = result.executor_id
                sample = sample_by_agent_id.get(executor_id)

                if sample is None:
                    continue

                try:
                    result_payload = parse_agent_json(result.agent_response.text)
                    run_results.append(
                        RunResult(
                            sample=sample,
                            status=status_from_text(result_payload.status),
                            output=result_payload.output,
                            error=result_payload.error,
                        )
                    )
                except Exception as ex:
                    run_results.append(
                        RunResult(
                            sample=sample,
                            status=RunStatus.ERROR,
                            output="",
                            error=(
                                f"Failed to parse agent output for {sample.relative_path}: {ex}. "
                                f"Raw: {result.agent_response.text}"  # type: ignore
                            ),
                        )
                    )

            unresolved = [
                sample
                for sample in discovery.samples
                if sample.relative_path not in {r.sample.relative_path for r in run_results}
            ]
            for sample in unresolved:
                run_results.append(
                    RunResult(
                        sample=sample,
                        status=RunStatus.ERROR,
                        output="",
                        error=f"No response from agent for sample {sample.relative_path}.",
                    )
                )

            return ExecutionResult(results=run_results)

        nested_workflow = (
            ConcurrentBuilder(participants=[CustomAgentExecutor(agent) for agent in agents])
            .with_aggregator(aggregate_results)
            .build()
        )

        await ctx.send_message(
            WorkflowCreationResult(
                samples=discovery.samples,
                workflow=nested_workflow,
                agents=agents,
            )
        )
