from prefect import flow, get_run_logger
from prefect_azure.blob_storage import AzureBlobStorageContainer


@flow
def simple_blob_writer():
    blob_container: AzureBlobStorageContainer = AzureBlobStorageContainer.load("data")

    get_run_logger().info(f"Uploading file example.txt to {blob_container.container_name}")
    blob_container.upload_from_path("example.txt", "example.txt", overwrite=True)
