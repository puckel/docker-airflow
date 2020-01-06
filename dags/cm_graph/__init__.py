from .create_views import create_tasks as cv_create_tasks
from .refresh_tables import create_tasks as re_create_tasks


def create_refresh_tasks(dag):
    create_view_tasks = cv_create_tasks(dag)
    refresh_table_tasks = re_create_tasks(dag)
    d = {
        'create_view_tasks': create_view_tasks,
        'refresh_table_tasks': refresh_table_tasks,
    }

    create_view_tasks['cache_schoolleader_mentor'].set_upstream([
        refresh_table_tasks['refresh_mentor_task'],
        refresh_table_tasks['refresh_schoolleader_task']]
    )

    create_view_tasks['cache_parent'].set_upstream(
        refresh_table_tasks['refresh_parent_task'],
    )


    return d
