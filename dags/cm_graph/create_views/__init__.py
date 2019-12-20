from . import (
    create_cache_views, mentor_lifecycle,
    parent_lifecycle, school_leader_lifecycle,
    teacher_lifecycle
)

def create_tasks(dag):
    d = {
        'cache_teacher': create_cache_views.create_cache_teacher_view_task(dag),
        'cache_parent': create_cache_views.create_cache_parent_view_task(dag),
        'cache_schoolleader_mentor': create_cache_views.create_schoolleader_mentor_view_task(dag),
        'teacher_lifecycle': teacher_lifecycle.create_teacher_lifecycle_view_task(dag),
        'parent_lifecycle': parent_lifecycle.create_parent_lifecycle_view_task(dag),
        'schoolleader_lifecycle': school_leader_lifecycle.create_schoolleader_lifecycle_view_task(dag),
        'mentor_lifecycle': mentor_lifecycle.create_mentor_lifecycle_view_task(dag)
    }
    return d
