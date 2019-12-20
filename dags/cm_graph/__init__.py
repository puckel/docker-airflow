from .create_views import create_tasks as cv_create_tasks


def create_tasks(dag):
    create_view_tasks = cv_create_tasks(dag)
    d = {
        'create_view_tasks': create_view_tasks,
    }
    return d
