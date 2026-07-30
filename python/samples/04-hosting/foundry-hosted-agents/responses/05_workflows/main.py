# Copyright (c) Microsoft. All rights reserved.

import os

from agent_framework import (
    Agent,
    AgentExecutor,
    AgentExecutorResponse,
    Executor,
    WorkflowBuilder,
    WorkflowContext,
    handler,
    response_handler,
)
from agent_framework.foundry import FoundryChatClient, ResponsesHostServer
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv
from typing_extensions import Never

# Load environment variables from .env file
load_dotenv()


class ApprovalExecutor(Executor):
    """Requests human approval for the formatted slogan before completing the workflow.

    Following the same pattern as the guessing game sample, this executor pauses the workflow
    with `ctx.request_info` and resumes in its `@response_handler` once the human replies.
    """

    def __init__(self, id: str | None = None):
        super().__init__(id=id or "approval")

    @handler
    async def request_approval(
        self,
        response: AgentExecutorResponse,
        ctx: WorkflowContext,
    ) -> None:
        """Take the formatted slogan and ask the human to approve it."""
        slogan = response.agent_response.text
        prompt = (
            f"Please review the final slogan:\n\n{slogan}\n\n"
            "Reply 'approve' to accept it, or reply with feedback to reject it."
        )
        # Pause the workflow and surface an approval request to the caller.
        await ctx.request_info(request_data=prompt, response_type=str)

    @response_handler
    async def on_human_decision(
        self,
        request: str,
        decision: str,
        ctx: WorkflowContext[Never, str],
    ) -> None:
        """Complete the workflow based on the human's decision."""
        if decision.strip().lower() == "approve":
            await ctx.yield_output("Approved")
        else:
            await ctx.yield_output(f"Slogan rejected. Human feedback: {decision}")


def main():
    client = FoundryChatClient(
        project_endpoint=os.environ["FOUNDRY_PROJECT_ENDPOINT"],
        model=os.environ["AZURE_AI_MODEL_DEPLOYMENT_NAME"],
        credential=DefaultAzureCredential(),
    )

    writer_agent = Agent(
        client=client,
        instructions=("You are an excellent slogan writer. You create new slogans based on the given topic."),
        name="writer",
    )

    legal_agent = Agent(
        client=client,
        instructions=(
            "You are an excellent legal reviewer. "
            "Make necessary corrections to the slogan so that it is legally compliant."
        ),
        name="legal_reviewer",
    )

    format_agent = Agent(
        client=client,
        instructions=(
            "You are an excellent content formatter. "
            "You take the slogan and format it in a cool retro style when printing to a terminal."
        ),
        name="formatter",
    )

    # Set the context mode to `last_agent` so that each agent only sees the output of the
    # previous agent instead of the full conversation history
    writer_executor = AgentExecutor(writer_agent, context_mode="last_agent")
    legal_executor = AgentExecutor(legal_agent, context_mode="last_agent")
    format_executor = AgentExecutor(format_agent, context_mode="last_agent")
    approval_executor = ApprovalExecutor()

    workflow_agent = (
        WorkflowBuilder(
            start_executor=writer_executor,
            # Select only the approval result as Workflow Output.
            # Unselected executor payloads are hidden unless selected as Intermediate Output.
            output_from=[approval_executor],
        )
        .add_edge(writer_executor, legal_executor)
        .add_edge(legal_executor, format_executor)
        # After formatting, request human approval before the workflow completes.
        .add_edge(format_executor, approval_executor)
        .build()
        .as_agent()
    )

    server = ResponsesHostServer(workflow_agent)
    server.run()


if __name__ == "__main__":
    main()
