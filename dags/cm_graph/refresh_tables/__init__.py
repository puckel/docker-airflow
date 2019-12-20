from . import (
    refresh_mentor, refresh_schoolleader,
    refresh_vanilla_teacher, refresh_parent,
    refresh_invite,
)


def create_tasks(dag):
    d = {
        'refresh_mentor_task': refresh_mentor.create_refresh_mentor_table_task(dag),
        'refresh_schoolleader_task': refresh_schoolleader.create_refresh_schoolleader_table_task(dag),
        'refresh_vanilla_teacher_task': refresh_vanilla_teacher.create_refresh_vanilla_teacher_table_task(dag),
        'refresh_parent_task': refresh_parent.create_refresh_parent_table_task(dag),
        'refresh_invite': refresh_invite.create_refresh_invite_table_task(dag),
    }

    d['refresh_schoolleader_task'].set_upstream(d['refresh_mentor_task'])
    d['refresh_vanilla_teacher_task'].set_upstream(d['refresh_mentor_task'])
