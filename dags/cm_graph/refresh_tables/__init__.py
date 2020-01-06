from . import (
    refresh_mentor, refresh_schoolleader,
    refresh_vanilla_teacher, refresh_parent,
    refresh_invite, refresh_school,
    refresh_parent_connection_request,
    refresh_all_teacher,
)


def create_tasks(dag):
    d = {
        'refresh_mentor_task': refresh_mentor.create_refresh_mentor_table_task(dag),
        'refresh_schoolleader_task': refresh_schoolleader.create_refresh_schoolleader_table_task(dag),
        'refresh_vanilla_teacher_task': refresh_vanilla_teacher.create_refresh_vanilla_teacher_table_task(dag),
        'refresh_parent_task': refresh_parent.create_refresh_parent_table_task(dag),
        'refresh_invite_task': refresh_invite.create_refresh_invite_table_task(dag),
        'refresh_school_task': refresh_school.create_refresh_school_table_task(dag),
        'refresh_parent_connection_request_task': refresh_parent_connection_request.create_refresh_parent_connection_request_table_task(dag),
        'refresh_all_teacher_task': refresh_all_teacher.refresh_all_teacher_table_task(dag),
    }

    d['refresh_schoolleader_task'].set_upstream(d['refresh_mentor_task'])
    d['refresh_vanilla_teacher_task'].set_upstream(d['refresh_mentor_task'])
    d['refresh_all_teacher_task'].set_upstream(d['refresh_vanilla_teacher_task'], d['refresh_mentor_task'], d['refresh_schoolleader_task'])
