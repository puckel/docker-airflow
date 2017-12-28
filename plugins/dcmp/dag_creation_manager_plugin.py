# encoding: utf-8

__author__ = "yanghanbing"
__version__ = "0.2.1"

import logging
import json
import difflib
from functools import wraps
from collections import OrderedDict

import airflow
from airflow.plugins_manager import AirflowPlugin
from airflow.www.app import csrf
from airflow.utils.db import provide_session
from flask import Blueprint, Markup, request, jsonify, flash
from flask_admin import BaseView, expose
from flask_admin.babel import gettext
from pygments import highlight, lexers
from pygments.formatters import HtmlFormatter

from dcmp import settings as dcmp_settings
from dcmp.models import DcmpDag, DcmpDagConf
from dcmp.dag_converter import dag_converter
from dcmp.utils import LogStreamContext, search_conf_iter


def login_required(func):
# when airflow loads plugins, login is still None.
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        if airflow.login:
            return airflow.login.login_required(func)(*args, **kwargs)
        return func(*args, **kwargs)
    return func_wrapper


def get_current_user(raw=True):
    try:
        if raw:
            res = airflow.login.current_user.user
        else:
            res = airflow.login.current_user
    except Exception as e:
        res = None
    return res


def need_approver():
    if not dcmp_settings.AUTHENTICATE:
        return False
    if not dcmp_settings.DAG_CREATION_MANAGER_NEED_APPROVER:
        return False
    current_user = get_current_user(raw=False)
    if not current_user:
        return False
    if not hasattr(current_user, "is_approver"):
        return False
    return True


def can_access_approver():
    if not need_approver():
        return True
    current_user = get_current_user(raw=False)
    return not current_user.is_anonymous() and current_user.is_approver()


def pygment_html_render(s, lexer=lexers.TextLexer):
    return highlight(
        s,
        lexer(),
        HtmlFormatter(linenos=True),
    )


def render(obj, lexer):
    out = ""
    if isinstance(obj, basestring):
        out += pygment_html_render(obj, lexer)
    elif isinstance(obj, (tuple, list)):
        for i, s in enumerate(obj):
            out += "<div>List item #{}</div>".format(i)
            out += "<div>" + pygment_html_render(s, lexer) + "</div>"
    elif isinstance(obj, dict):
        for k, v in obj.items():
            out += '<div>Dict item "{}"</div>'.format(k)
            out += "<div>" + pygment_html_render(v, lexer) + "</div>"
    return out


def command_render(task_type, command):
    attr_renderer = {
        'bash': lambda x: render(x, lexers.BashLexer),
        'hql': lambda x: render(x, lexers.SqlLexer),
        'sql': lambda x: render(x, lexers.SqlLexer),
        'python': lambda x: render(x, lexers.PythonLexer),
        'short_circuit': lambda x: render(x, lexers.PythonLexer),
        'time_sensor': lambda x: render(x, lexers.PythonLexer),
        'timedelta_sensor': lambda x: render(x, lexers.PythonLexer),
    }
    if task_type in attr_renderer:
        res = attr_renderer[task_type](command)
    else:
        res = "<pre><code>%s</pre></code>" % command
    return res


class RequestArgsFilter(object):
    def __init__(self, model, request_args, arg_tuple):
        self.model = model
        self.arg_tuple = arg_tuple
        self.request_args = request_args
        self.filter_arg_dict = None
        self.filter_groups = None
        self.active_filters = None
        self.filters = None
        self.filters_dict = {}
        self.refresh_filter_arg_dict()
        self.refresh_filter_groups()
        self.refresh_filters()
    
    def refresh_filter_arg_dict(self):
        filter_arg_dict = OrderedDict(self.arg_tuple)
        index = 0
        for filter_arg, filter_params in filter_arg_dict.iteritems():
            for filter_operation in filter_params["operations"]:
                filter_params["index"] = index
                index += 1
        self.filter_arg_dict = filter_arg_dict

    def refresh_filter_groups(self):
        filter_groups = OrderedDict()
        for filter_arg, filter_params in self.filter_arg_dict.iteritems():
            for filter_operation in filter_params["operations"]:
                filter_groups[filter_arg] = [{
                    "index" : filter_params["index"],
                    "operation" : filter_operation,
                    "arg" : "%s_%s" % (filter_arg, filter_operation),
                    "type" : None,
                    "options" : None
                }]
        self.filter_groups = filter_groups

    def refresh_filters(self):
        active_filters = []
        filters = []
        for key in sorted(self.request_args.keys()):
            value = self.request_args.get(key)
            if not value:
                continue
            if not key.startswith("flt"):
                continue
            key = key.split("_", 1)[-1]
            if key.find("_") == -1:
                continue
            arg, operation = key.rsplit("_")
            if arg not in self.filter_arg_dict:
                continue
            if operation not in self.filter_arg_dict[arg]["operations"]:
                continue
            active_filters.append([self.filter_arg_dict[arg]["index"], arg, value])
            if not self.filter_arg_dict[arg].get("no_filters"):
                filters.append(getattr(getattr(self.model, arg.lower().replace(" ", "_")), operation)(value))
            self.filters_dict[arg] = {"operation": operation, "value": value}
        self.active_filters = active_filters
        self.filters = filters


class DagCreationManager(BaseView):
    CONSTANT_KWS = {
        "TASK_TYPES": dcmp_settings.TASK_TYPES,
        "DAG_CREATION_MANAGER_LINE_INTERPOLATE": dcmp_settings.DAG_CREATION_MANAGER_LINE_INTERPOLATE,
        "DAG_CREATION_MANAGER_QUEUE_POOL": dcmp_settings.DAG_CREATION_MANAGER_QUEUE_POOL,
        "DAG_CREATION_MANAGER_CATEGORYS": dcmp_settings.DAG_CREATION_MANAGER_CATEGORYS,
        "DAG_CREATION_MANAGER_TASK_CATEGORYS": dcmp_settings.DAG_CREATION_MANAGER_TASK_CATEGORYS,
    }
    
    DEFAULT_CONF = {
        "retries": 3,
        "retry_delay_minutes": 5,
        "start_date": "",
        "end_date": "",
        "email_on_failure": True,
        "email_on_retry": False,
        "depends_on_past": False,
        "concurrency": 16,
        "max_active_runs": 16,
        "add_start_task": False,
        "add_end_task": False,
        "skip_dag_not_latest": False,
        "skip_dag_on_prev_running": False,
        "email_on_skip_dag": False,
        "emails": "",
        "tasks": [],
    }

    @expose("/")
    @expose("/list")
    @login_required
    @provide_session
    def index(self, session=None):
        TASK_NAME = "Task Name"
        COMMAND = "Command"
        request_args_filter = RequestArgsFilter(DcmpDag, request.args, (
            ("Category", {"operations": ["contains"]}),
            (TASK_NAME, {"operations": ["contains"], "no_filters": True}),
            (COMMAND, {"operations": ["contains"], "no_filters": True}),
        ))
        confs = OrderedDict()
        dcmp_dags = session.query(DcmpDag).order_by(DcmpDag.dag_name).filter(*request_args_filter.filters)
        dcmp_dags_count = dcmp_dags.count()
        dcmp_dags = dcmp_dags[:]
        for dcmp_dag in dcmp_dags:
            dcmp_dag.conf = dcmp_dag.get_conf(session=session)

        if request_args_filter.filters_dict.get(TASK_NAME):
            task_name_value = request_args_filter.filters_dict.get(TASK_NAME)["value"]
            def filter_dcmp_dags_by_task_name(dcmp_dag):
                for task in dcmp_dag.conf["tasks"]:
                    if task_name_value in task["task_name"]:
                        return True
                return False
            dcmp_dags = filter(filter_dcmp_dags_by_task_name, dcmp_dags)

        if request_args_filter.filters_dict.get(COMMAND):
            command_value = request_args_filter.filters_dict.get(COMMAND)["value"]
            def filter_dcmp_dags_by_command(dcmp_dag):
                for task in dcmp_dag.conf["tasks"]:
                    if command_value in task["command"]:
                        return True
                return False
            dcmp_dags = filter(filter_dcmp_dags_by_command, dcmp_dags)

        search = request.args.get("search", "")
        if search:
            searched_dcmp_dags = []
            for dcmp_dag in dcmp_dags:
                dcmp_dag.search_results = []
                for result_task_name, result_key, result_line in search_conf_iter(search, dcmp_dag.conf):
                    dcmp_dag.search_results.append({
                        "key": result_key,
                        "full_key": "%s__%s" % (result_task_name, result_key),
                        "line": result_line,
                        "html_line": ('<span class="nb">[%s]</span> ' % result_key if result_key else "") + result_line.replace(search, '<span class="highlighted">%s</span>' % search),
                    })
                if dcmp_dag.search_results:
                    searched_dcmp_dags.append(dcmp_dag)
            dcmp_dags = searched_dcmp_dags

        return self.render("dcmp/index.html",
            can_access_approver=can_access_approver(),
            dcmp_dags=dcmp_dags,
            dcmp_dags_count=dcmp_dags_count,
            filter_groups=request_args_filter.filter_groups,
            active_filters=request_args_filter.active_filters,
            search=search, )

    def _edit(self, template, session=None):
        conf = None
        dcmp_dag = None
        dcmp_dag_confs = []
        user = get_current_user()
        readonly = bool(request.args.get("readonly", False))
        dag_name = request.args.get("dag_name")
        if dag_name:
            dcmp_dag = session.query(DcmpDag).filter(
                DcmpDag.dag_name == dag_name,
            ).first()
            if dcmp_dag:
                dcmp_dag_confs = session.query(DcmpDagConf).filter(
                    DcmpDagConf.dag_id == dcmp_dag.id,
                ).order_by(DcmpDagConf.version.desc())
                dcmp_dag_confs = dcmp_dag_confs[:]
                for i, dcmp_dag_conf in enumerate(dcmp_dag_confs):
                    if dcmp_dag_conf.version == dcmp_dag.version:
                        dcmp_dag_confs[0], dcmp_dag_confs[i] = dcmp_dag_confs[i], dcmp_dag_confs[0]
                        break
                
                conf = dcmp_dag.get_conf(session=session)
                if not readonly:
                    if dcmp_dag.editing:
                        if user.id != dcmp_dag.editing_user_id:
                            flash(gettext("You can not change this DAG config for the moment, %s is editing." % dcmp_dag.editing_user_name), "warning")
                            readonly = True
                    else:
                        flash(gettext(Markup("This DAG config is locked when you are editing. If you don't want to edit, use <a href='?dag_name=%s&readonly=True'><strong>readonly mode</strong></a>." % dag_name)))
                        dcmp_dag.start_editing(user)
                        session.commit()
            else:
                conf = None
        if conf is None:
            conf = self.DEFAULT_CONF
        return self.render(template,
            can_access_approver=can_access_approver(),
            need_approver=need_approver(),
            readonly=readonly,
            conf=conf,
            dcmp_dag=dcmp_dag,
            dcmp_dag_confs=dcmp_dag_confs,
            **self.CONSTANT_KWS)

    @expose("/edit")
    @login_required
    @provide_session
    def edit(self, session=None):
        return self._edit("dcmp/edit.html", session=session)

    @expose("/graph")
    @login_required
    @provide_session
    def graph(self, session=None):
        return self._edit("dcmp/graph.html", session=session)

    @expose("/raw")
    @login_required
    @provide_session
    def raw(self, session=None):
        return self._edit("dcmp/raw.html", session=session)

    @expose("/details")
    @login_required
    @provide_session
    def details(self, session=None):
        conf = None
        dcmp_dag = None
        dag_name = request.args.get("dag_name")
        highlight = request.args.get("highlight")
        if dag_name:
            dcmp_dag = session.query(DcmpDag).filter(
                DcmpDag.dag_name == dag_name,
            ).first()
            if dcmp_dag:
                conf = dcmp_dag.get_conf(session=session)
            else:
                conf = None
        if conf is None:
            conf = self.DEFAULT_CONF
        return self.render("dcmp/details.html",
            can_access_approver=can_access_approver(),
            conf=conf,
            dcmp_dag=dcmp_dag,
            command_render=command_render)

    def conf_diff_preprocess(self, conf):
        MULTILINE_FLAGS = ['"command": "']
        res = []
        for line in json.dumps(conf, indent=4, ensure_ascii=False).split("\n"):
            for multiline_flag in MULTILINE_FLAGS:
                if line.strip().startswith(multiline_flag) and line.find("\\n") != -1:
                    space_num = len(line) - len(line.lstrip()) + len(multiline_flag)
                    space = " " * space_num
                    multilines = line.split("\\n")
                    res.append(multilines[0] + "\n")
                    for multiline in multilines[1:]:
                        res.append(space + multiline + "\n")
                    break
            else:
                res.append(line + "\n")
        return res

    @expose("/approve")
    @login_required
    @provide_session
    def approve(self, session=None):
        if not can_access_approver():
            raise Exception("Ooops")
        conf = None
        dcmp_dag = None
        dag_name = request.args.get("dag_name")
        version = request.args.get("version") or None
        if not dag_name:
            raise Exception("Ooops")
        dcmp_dag = session.query(DcmpDag).filter(
            DcmpDag.dag_name == dag_name,
        ).first()
        if not dcmp_dag:
            raise Exception("Ooops")
        conf = dcmp_dag.get_conf(version=version, session=session)
        if not conf:
            raise Exception("Ooops")
        approved_conf = dcmp_dag.get_conf(version=dcmp_dag.approved_version, session=session)
        diff_table = difflib.HtmlDiff().make_table(
            self.conf_diff_preprocess(approved_conf),
            self.conf_diff_preprocess(conf),
        )
        return self.render("dcmp/approve.html",
            can_access_approver=can_access_approver(),
            diff_table=diff_table,
            dcmp_dag=dcmp_dag,
            command_render=command_render)

    @expose("/compare")
    @login_required
    @provide_session
    def compare(self, session=None):
        conf1 = {}
        conf2 = {}
        conf = None
        dcmp_dag = None
        dag_name = request.args.get("dag_name")
        version1 = request.args.get("version1")
        version2 = request.args.get("version2")
        if dag_name:
            dcmp_dag = session.query(DcmpDag).filter(
                DcmpDag.dag_name == dag_name,
            ).first()
            if dcmp_dag:
                conf = dcmp_dag.get_conf(session=session)
                conf1 = dcmp_dag.get_conf(version=version1, session=session)
                conf2 = dcmp_dag.get_conf(version=version2, session=session)
                
                dcmp_dag_confs = session.query(DcmpDagConf).filter(
                    DcmpDagConf.dag_id == dcmp_dag.id,
                ).order_by(DcmpDagConf.version.desc())
                
        diff_table = difflib.HtmlDiff().make_table(
            self.conf_diff_preprocess(conf1),
            self.conf_diff_preprocess(conf2),
        )
        if conf is None:
            conf = self.DEFAULT_CONF
        return self.render("dcmp/compare.html",
            can_access_approver=can_access_approver(),
            diff_table=diff_table,
            conf=conf,
            version1=version1,
            version2=version2,
            dcmp_dag=dcmp_dag,
            dcmp_dag_confs=dcmp_dag_confs,
            command_render=command_render)

    @csrf.exempt
    @expose("/graph_display", methods=["GET", "POST"])
    @login_required
    @provide_session
    def graph_display(self, session=None):
        conf = request.form.get("conf")
        active_job_id = request.form.get("active_job_id", "")
        try:
            conf = json.loads(conf)
            conf = dag_converter.clean_dag_dict(conf)
        except Exception as e:
            conf = self.DEFAULT_CONF
        return self.render("dcmp/graph_display.html",
            readonly=True,
            conf=conf,
            active_job_id=active_job_id,
            **self.CONSTANT_KWS)

    @expose("/params")
    @provide_session
    def params(self, session=None):
        return self.render("dcmp/params.html")

    @csrf.exempt
    @expose("/api", methods=["GET", "POST"])
    @login_required
    @provide_session
    def api(self, session=None):
        user = get_current_user()
        api = request.args.get("api")
        if api == "delete_dag":
            if not can_access_approver():
                return jsonify({"code": -100, "detail": "no permission", })
            dag_name = request.args.get("dag_name")
            if dag_name:
                dcmp_dag = session.query(DcmpDag).filter(
                    DcmpDag.dag_name == dag_name,
                ).first()
                if dcmp_dag:
                    dcmp_dag.delete_conf(user=user, session=session)
                session.commit()
                dag_converter.refresh_dags()
                return jsonify({"code": 0, "detail": "succeeded", })
            else:
                return jsonify({"code": -1, "detail": "dag name required", })
        elif api == "update_dag":
            dag_name = request.args.get("dag_name")
            data = request.get_json()
            try:
                data = dag_converter.clean_dag_dict(data, strict=True)
            except Exception as e:
                logging.exception("api.update_dag")
                return jsonify({"code": -2, "detail": e.message, })
            new_dag_name = data["dag_name"]
            if new_dag_name != dag_name:
                if dag_name and not can_access_approver():
                    return jsonify({"code": -100, "detail": "no permission to change dag name", })
                dcmp_dag = session.query(DcmpDag).filter(
                    DcmpDag.dag_name == new_dag_name,
                ).first()
                if dcmp_dag:
                    return jsonify({"code": -3, "detail": "dag name duplicated", })
            DcmpDag.create_or_update_conf(data, user=user, session=session)
            if new_dag_name != dag_name and dag_name:
                dcmp_dag = session.query(DcmpDag).filter(
                    DcmpDag.dag_name == dag_name,
                ).first()
                if dcmp_dag:
                    dcmp_dag.delete_conf(user=user, session=session)
            session.commit()
            if not need_approver():
                dag_converter.refresh_dags()
            return jsonify({"code": 0, "detail": "succeeded", })
        elif api == "approve_dag":
            if not can_access_approver():
                return jsonify({"code": -100, "detail": "no permission", })
            dag_name = request.args.get("dag_name")
            version = request.args.get("version")
            try:
                version = int(version)
            except Exception as e:
                return jsonify({"code": -5, "detail": "version invalid", })
            if version <= 0:
                return jsonify({"code": -5, "detail": "version invalid", })
            if dag_name:
                dcmp_dag = session.query(DcmpDag).filter(
                    DcmpDag.dag_name == dag_name,
                ).first()
                if dcmp_dag:
                    if version > dcmp_dag.version or version <= dcmp_dag.approved_version:
                        return jsonify({"code": -5, "detail": "version invalid", })
                    if not dcmp_settings.DAG_CREATION_MANAGER_CAN_APPROVE_SELF and dcmp_dag.last_editor_user_id == user.id:
                        return jsonify({"code": -6, "detail": "can not approve yourself", })
                    dcmp_dag.approve_conf(version=version, user=user, session=session)
                    session.commit()
                    dag_converter.refresh_dags()
                    return jsonify({"code": 0, "detail": "succeeded", })
                else:
                    return jsonify({"code": -2, "detail": "dag does not exists", })
            else:
                return jsonify({"code": -1, "detail": "dag name required", })
        elif api == "get_dag":
            dag_name = request.args.get("dag_name")
            version = request.args.get("version")
            if dag_name:
                dcmp_dag = session.query(DcmpDag).filter(
                    DcmpDag.dag_name == dag_name,
                ).first()
                if dcmp_dag:
                    conf = dcmp_dag.get_conf(version=version, session=session)
                    if conf:
                        return jsonify({"code": 0, "detail": {"conf": conf}, })
                    else:
                        return jsonify({"code": -3, "detail": "version does not exists", })
                else:
                    return jsonify({"code": -2, "detail": "dag does not exists", })
            else:
                return jsonify({"code": -1, "detail": "dag name required", })
            return jsonify({"code": 0, "detail": "succeeded", })
        elif api == "render_task_conf":
            data = request.get_json()
            try:
                ti = dag_converter.create_task_instance_by_task_conf(data)
            except Exception as e:
                logging.exception("api.render_task_conf")
                return jsonify({"code": -2, "detail": e.message, })
            res = self.render_ti(ti, result_format=request.args.get("format", None))
            return jsonify({"code": 0, "detail": res, })
        elif api == "dry_run_task_conf":
            data = request.get_json()
            try:
                ti = dag_converter.create_task_instance_by_task_conf(data)
            except Exception as e:
                logging.exception("api.dry_run_task_conf")
                return jsonify({"code": -2, "detail": e.message, })
            rendered = self.render_ti(ti, result_format=request.args.get("format", None))
            with LogStreamContext() as stream:
                ti.dry_run()
                log = stream.getvalue()
            return jsonify({"code": 0, "detail": {"rendered": rendered, "log": log}, })
        elif api == "end_editing":
            dag_name = request.args.get("dag_name")
            if dag_name:
                dcmp_dag = session.query(DcmpDag).filter(
                    DcmpDag.dag_name == dag_name,
                ).first()
                if dcmp_dag and dcmp_dag.editing and user.id == dcmp_dag.editing_user_id:
                    dcmp_dag.end_editing()
                    session.commit()            
            return jsonify({"code": 0, "detail": "succeeded", })
        return jsonify({"code": -1000, "detail": "no such api", })
        
    def render_ti(self, ti, result_format=None):
        ti.render_templates()
        res = OrderedDict()
        for template_field in ti.task.__class__.template_fields:
            res[template_field] = {"code": getattr(ti.task, template_field)}
        if result_format == "html":
            from airflow.www.views import attr_renderer # can not load views when airflow loads plugins.
            for template_field, content in res.iteritems():
                if template_field in attr_renderer:
                    content["html"] = attr_renderer[template_field](content["code"])
                else:
                    content["html"] = ("<pre><code>" + str(content["code"]) + "</pre></code>")
        return res


dag_creation_manager_view = DagCreationManager(category="Admin", name="DAG Creation Manager")

dag_creation_manager_bp = Blueprint(
    "dag_creation_manager_bp",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/dcmp"
)


class DagCreationManagerPlugin(AirflowPlugin):
    name = "dag_creation_manager"
    operators = []
    flask_blueprints = [dag_creation_manager_bp]
    hooks = []
    executors = []
    admin_views = [dag_creation_manager_view]
    menu_links = []
