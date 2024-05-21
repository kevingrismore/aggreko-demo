from datetime import datetime

from prefect import flow, get_client
from prefect.artifacts import TableArtifact
from prefect.client.schemas.filters import (
    DeploymentFilter,
    DeploymentFilterName,
    FlowRunFilter,
    FlowRunFilterStartTime,
)
from prefect.client.schemas.objects import FlowRun
from prefect.settings import PREFECT_UI_URL


@flow
async def reporting(
    date: datetime = datetime.today(), deployments: list[str] = ["writer", "reader"]
):
    async with get_client() as client:
        flow_runs: list[FlowRun] = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                start_time=FlowRunFilterStartTime(
                    after_=date.replace(hour=0, minute=0, second=0),
                    before_=date.replace(hour=23, minute=59, second=59),
                )
            ),
            deployment_filter=DeploymentFilter(
                name=DeploymentFilterName(any_=deployments)
            ),
        )

        flow_run_list = []

        for flow_run in flow_runs:
            flow_run_list.append(
                {
                    "name": f"[{flow_run.name}]({PREFECT_UI_URL.value()}/flow-runs/flow-run/{flow_run.id})",
                    "flow_run_id": str(flow_run.id),
                    "flow_id": str(flow_run.flow_id),
                    "deployment_id": str(flow_run.deployment_id),
                    "state": flow_run.state.type,
                    "total_run_time": str(flow_run.total_run_time),
                    "start_time": str(flow_run.start_time),
                    "end_time": str(flow_run.end_time),
                }
            )

        await TableArtifact(
            key=f"flow-runs",
            description=f"Flow runs  - {date.strftime('%Y-%m-%d')}",
            table=flow_run_list,
        ).create()


if __name__ == "__main__":
    reporting.serve()
