from prefect import serve
from prefect.events import DeploymentEventTrigger

from flows.simple_blob_writer import simple_blob_writer
from flows.simple_blob_reader import simple_blob_reader

if __name__ == "__main__":
    serve(
        simple_blob_writer.to_deployment("writer"),
        simple_blob_reader.to_deployment(
            "reader",
            triggers=[
                DeploymentEventTrigger(
                    name="Storage Blob Created",
                    match={
                        "prefect.resource.id": "https://kevinstest.blob.core.windows.net/data/*"
                    },
                    expect=["Microsoft.Storage.BlobCreated"],
                    parameters={
                        "file_url": "{{ event.payload.data.url }}",
                        "storage_account_name": "kevinstest",
                    },
                )
            ],
        ),
    )
