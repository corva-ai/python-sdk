import raygun4py.raygunprovider  # <.>
from corva import Api, TaskEvent, task

raygun_handler = raygun4py.raygunprovider.RaygunHandler('YOUR_RAYGUN_API_KEY')  # <.>


@task(handler=raygun_handler)  # <.>
def app(event: TaskEvent, api: Api) -> None:
    1 / 0  # <.>
