Date.prototype.Format = function (fmt) { //author: meizz 
    var o = {
        "M+": this.getMonth() + 1,
        "d+": this.getDate(),
        "h+": this.getHours(),
        "m+": this.getMinutes(),
        "s+": this.getSeconds(),
        "q+": Math.floor((this.getMonth() + 3) / 3),
        "S": this.getMilliseconds()
    };
    if (/(y+)/.test(fmt)) fmt = fmt.replace(RegExp.$1, (this.getFullYear() + "").substr(4 - RegExp.$1.length));
    for (var k in o)
    if (new RegExp("(" + k + ")").test(fmt)) fmt = fmt.replace(RegExp.$1, (RegExp.$1.length == 1) ? (o[k]) : (("00" + o[k]).substr(("" + o[k]).length)));
    return fmt;
};

(function($) {
    var render_help = '<p class="help-block">Can include <a target="_blank" href="params">{{ params }}</a>, <a target="_blank" href="http://pythonhosted.org/airflow/code.html#id2">{{ macros }}</a> and <a target="_blank" href="http://pythonhosted.org/airflow/code.html#default-variables">default variables</a>.</p>';

    window.default_task = {
        task_name: "",
        task_type: "bash",
        command: "",
        priority_weight: 0,
        upstreams: []
    };
    
    window.task_names = [];
    window.refresh_task_names = function() {
        while(task_names.pop() != undefined){};
        $("input[name='task_name']").each(function(){
            var value = $(this).val();
            if(value){
                task_names.push({
                    id: value,
                    text: value
                });
            }
        });
    };
    
    var lastest_task_id = 0;
    
    var get_text_list_single_element = function(field_name, field_value) {
        return [
            '<div style="position: relative; margin-bottom: 15px;">',
                '<input class="form-control input-list" name="' + field_name + '" type="' + 'text' + '" value="' + field_value + '"' + (readonly? ' readonly="readonly" ': '') + '>',
                readonly? '': '<a href="javascript:void(0)" class="btn btn-danger field-delete" role="button" style="position: absolute; top:0; right: 0; z-index: 1;">-</a>',
            '</div>'
        ].join("\n");
    };
    
    var task_to_element_by_field = function(task_id, task, field_name, field_type, required, field_name_verbose, helper_text) {
        if(!field_name_verbose){
            var field_name_list = field_name.split("_");
            for(var i=0; i<field_name_list.length; i++){
                field_name_list[i] = field_name_list[i].charAt(0).toUpperCase() + field_name_list[i].slice(1);
            }
            var field_name_verbose = field_name_list.join(" ");
        }
        var long_label = field_name_verbose.length >= 18;
        var helper_html = "";
        if(helper_text){
            helper_html = ' <span class="glyphicon glyphicon-info-sign task-tooltip" aria-hidden="true" title="' + helper_text.replace(/"/g, '\\"') + '"></span>';
        }
        var field_html = "";
        if(field_type == "queue_pool"){
            field_html = ['<span name="' + field_name + '" class="btn-group btn-group-input btn-group-input-single">'];
            for(var i=0; i<queue_pools.length; i++){
                var queue_pool = queue_pools[i][0];
                field_html.push('<button type="button" class="btn btn-item ' + (queue_pool == task[field_name]? 'active': '') + '" field-value="' + queue_pool + '">' + queue_pool.split("_").join(" ") + ' queue</button>');
            }
            field_html.push('</span>');
            field_html = field_html.join("\n");
        }else if(field_type == "task_category"){
            var field_value = task[field_name] || "";
            field_html = ['<span name="' + field_name + '" class="btn-group btn-group-input btn-group-input-single">'];
            for(var i=0; i<task_categorys.length; i++){
                var task_category = task_categorys[i][0];
                field_html.push('<button type="button" class="btn btn-item ' + (task_category == field_value? 'active': '') + '" field-value="' + task_category + '">' + (task_category? task_category.split("_").join(" "): "default") + '</button>');
            }
            field_html.push('</span>');
            field_html = field_html.join("\n");
        }else if(field_type == "task_type"){
            var get_ace_script = function(task_type, mode, minLines, maxLines) {
                minLines = minLines || 10;
                maxLines = maxLines || "Infinity";
                var ace_id = "ace_" + task_id + "_" + task_type;
                var ace_clone_id = ace_id + "_clone";
                return [
                    "<script>",
                    "$(document).ready(function(){",
                    "    var sql_clone = $('#" + ace_id + "').clone().attr('id', '" + ace_clone_id + "').appendTo($('#" + ace_id + "').parent());",
                    "    var textarea = $('#" + ace_id + "').hide();",
                    "    var editor = ace.edit('" + ace_clone_id + "');",
                    "    editor.setTheme('ace/theme/crimson_editor');",
                    "    editor.setOptions({",
                    readonly? "        readOnly: true,": "",
                    "        minLines: " + minLines + ",",
                    "        maxLines: " + maxLines,
                    "    });",
                    "    editor.getSession().setMode('ace/mode/" + mode + "');",
                    "    editor.getSession().on('change', function(){",
                    "        textarea.val(editor.getSession().getValue());",
                    "    });",
                    "});",
                    "</script>"
                ].join("\n");
            }
            field_html = ['<ul name="' + field_name + '" class="nav nav-tabs">'];
            for(var i=0; i<task_types.length; i++){
                var task_type = task_types[i];
                field_html.push('<li role="presentation" field-value="' + task_type + '" class="' + (task_type == task[field_name]? 'active': '') + '"><a href="#command_' + task_id + '_' + task_type + '">' + task_type.split("_").join(" ") + '</a></li>');
            }
            field_html.push('</ul>');
            field_html.push('<div name="command" class="tab-content" style="padding-top: 5px;">');
            for(var i=0; i<task_types.length; i++){
                var task_type = task_types[i];
                field_html.push('<div role="tabpanel" class="tab-pane ' + (task_type == task[field_name]? 'active': '') + '" id="command_' + task_id + '_' + task_type + '">');
                if(task_type == "time_sensor"){
                    field_html.push('<input ' + (readonly? ' readonly="readonly" ': '') + ' style="margin: 0 0 10px;" class="form-control date-range-picker" data-date-format=\'\\d\\a\\t\\e\\t\\i\\m\\e.\\s\\t\\r\\p\\t\\i\\m\\e("HH:mm","%\\H:%\\M").\\t\\i\\m\\e()\' data-role="timepicker" name="command" type="text" value=\'' + (task_type == task[field_name]? task["command"]: '') + '\'>');
                }else if(task_type == "bash"){
                    field_html.push('<textarea ' + (readonly? ' readonly="readonly" ': '') + ' id="ace_' + task_id + '_' + task_type + '" class="form-control" rows="1" name="command">' + (task_type == task[field_name]? task["command"]: '') + '</textarea>');
                    field_html.push(render_help);
                    field_html.push(get_ace_script(task_type, "sh", 1));
                }else if(task_type == "hql"){
                    field_html.push('<textarea ' + (readonly? ' readonly="readonly" ': '') + ' id="ace_' + task_id + '_' + task_type + '" class="form-control" rows="1" name="command">' + (task_type == task[field_name]? task["command"]: '') + '</textarea>');
                    field_html.push(render_help);
                    field_html.push(get_ace_script(task_type, "sql"));
                }else if(["python", "short_circuit"].indexOf(task_type) != -1){
                    field_html.push('<textarea ' + (readonly? ' readonly="readonly" ': '') + ' id="ace_' + task_id + '_' + task_type + '" class="form-control" rows="1" name="command">' + (task_type == task[field_name]? task["command"]: '') + '</textarea>');
                    field_html.push(get_ace_script(task_type, "python"));
                }else if(task_type == "timedelta_sensor"){
                    field_html.push('<textarea ' + (readonly? ' readonly="readonly" ': '') + ' id="ace_' + task_id + '_' + task_type + '" class="form-control" rows="1" name="command">' + (task_type == task[field_name]? task["command"]: '') + '</textarea>');
                    field_html.push(get_ace_script(task_type, "python", 1));
                }else{
                    field_html.push('<textarea ' + (readonly? ' readonly="readonly" ': '') + ' class="form-control" rows="1" name="command">' + (task_type == task[field_name]? task["command"]: '') + '</textarea>');
                }
                field_html.push('</div>');
            }
            field_html.push('</div>');
            
            field_html.push('<div>');
            field_html.push('<a href="javascript:void(0)" class="btn btn-primary render-btn" role="button">Render</a>')
            field_html.push('<a href="javascript:void(0)" class="btn btn-default render-dry-run-btn" role="button">Render and Dry Run</a>')
            field_html.push('<img class="render-loading" style="width: 25px; display: none; margin-left: 10px;" src="' + loading_img_url + '">')
            field_html.push('<div class="render-result">');
            field_html.push('</div>');
            field_html.push('</div>');
            
            field_html = field_html.join("\n");
        }else if(field_type == "text_list"){
            field_html = ['<div>'];
            for(var i=0; i<task[field_name].length; i++){
                var field_value = task[field_name][i];
                field_html.push(get_text_list_single_element(field_name, field_value));
            }
            field_html.push('</div>');
            if(!readonly){
                field_html.push('<input type="button" class="btn btn-primary text-list-field-add" style="float: right;" field-name="' + field_name + '" field-value="" value="+" />');
            };
            field_html = field_html.join("\n");
        }else if(field_type == "upstreams"){
            var select2_obj_id = "task_upstreams_" + task_id.toString();
            field_html = [
                '<div name="' + field_name + '" select2-obj-id="' + select2_obj_id + '">',
                    '<div id="' + select2_obj_id + '" class="text-select"></div>',
                    '\<script\>',
                        'window.' + select2_obj_id + '= $("#' + select2_obj_id + '").select2({data: task_names, multiple:true});',
                        'window.' + select2_obj_id + '.val("' + task[field_name].join(",") + '");',
                        readonly? ('$("#' + select2_obj_id + '").select2("readonly",true);'): '',
                    '\<\/script\>',
                '</div>'
            ].join("\n");
        }else if(field_type == "textarea"){
            field_html = '<textarea ' + (readonly? ' readonly="readonly" ': '') + ' class="form-control" rows="1" name="' + field_name + '">' + task[field_name] + '</textarea>';
        }else{
            field_html = '<input ' + (readonly? ' readonly="readonly" ': '') + ' class="form-control" name="' + field_name + '" type="' + field_type + '" value="' + task[field_name] + '">';
        }
        return [
            '<div class="form-group">',
                '<label class="col-md-2 control-label' + (long_label? ' long-label': '') + '">' + field_name_verbose + helper_html + ' &nbsp;' + (required? '<strong style="color: red">*</strong>': '') + '</label>',
                '<div class="col-md-10">',
                    field_html,
                '</div>',
            '</div>',
        ].join("\n");
    };
    
    window.task_to_element = function(task) {
        lastest_task_id++;
        return [
            '<div class="task-item" id="task_' + lastest_task_id + '" task_id="task_' + lastest_task_id + '" task_name="' + task.task_name + '">',
                window.refresh_task_graph? '': '<hr>',
                task_to_element_by_field(lastest_task_id, task, "task_name", "text", true, null, "A unique, meaningful name for the task"),
                task_to_element_by_field(lastest_task_id, task, "queue_pool", "queue_pool", true, null, "Queue and pool for the task, choose the right worker to execute the task."),
                task_to_element_by_field(lastest_task_id, task, "task_category", "task_category", false),
                task_to_element_by_field(lastest_task_id, task, "task_type", "task_type", true, "Task"),
//                    task_to_element_by_field(lastest_task_id, task, "command", "textarea", true),
                window.refresh_task_graph? '': task_to_element_by_field(lastest_task_id, task, "upstreams", "upstreams"),

                '<div class="panel panel-default advanced-settings-collapse">',
                '    <div class="panel-heading" role="tab" id="task_' + lastest_task_id + '_advanced_settings_header">',
                '        <h4 class="panel-title">',
                '            <a class="accordion-toggle collapsed" data-toggle="collapse" data-parent="#accordion" href="#task_' + lastest_task_id + '_advanced_settings" aria-expanded="false" aria-controls="task_' + lastest_task_id + '_advanced_settings">Advanced Settings</a>',
                '            <span class="advanced-settings-tool-bar">',
                '                <a href="javascript:void(0)" class="advanced-settings-expand-all advanced-settings-tool">Expand All</a>',
                '                <span class="advanced-settings-tool">/</span>',
                '                <a href="javascript:void(0)" class="advanced-settings-collapse-all advanced-settings-tool" style="margin-right: 20px;">Collapse All</a>',
                '            </span>',
                '        </h4>',
                '    </div>',
                '    <div id="task_' + lastest_task_id + '_advanced_settings" class="panel-collapse collapse" style="background: transparent;" role="tabpanel" aria-labelledby="task_' + lastest_task_id + '_advanced_settings_header">',
                '        <div class="panel-body">',
                task_to_element_by_field(lastest_task_id, task, "priority_weight", "number", null, null, "Priority weight of this task against other task. This allows the executor to trigger higher priority tasks before others when things get backed up."),
                task_to_element_by_field(lastest_task_id, task, "retries", "number", null, null, "The number of retries that should be performed before failing the task."),
                task_to_element_by_field(lastest_task_id, task, "retry_delay_minutes", "number", null, null, "Delay minutes between retries."),
                '        </div>',
                '    </div>',
                '</div>',
                readonly || window.refresh_task_graph? '': '<a href="javascript:void(0)" class="btn btn-danger task-delete" role="button" style="float: right;">Delete</a>',
                '<div style="clear: both;"></div>',
            '</div>'
        ].join("\n");
    };
    
    window.element_to_json = function($element) {
        var res = {};
        $element.find(":input").each(function() {
            if($(this).attr("name") && $(this).attr("name") != "command"){
                if($(this).hasClass("input-list")){
                    if(res[$(this).attr("name")]){
                        res[$(this).attr("name")].push($(this).val());
                    }else{
                        res[$(this).attr("name")] = [$(this).val()];
                    }
                }else if ($(this).attr("type") == "checkbox"){
                    res[$(this).attr("name")] = $(this).is(':checked');
                }else{
                    res[$(this).attr("name")] = $(this).val();
                }
            }
        });
        $element.find(".btn-group-input-single").each(function() {
            var name = $(this).attr("name");
            var value = $(this).find("button.btn-item.active").attr("field-value");
            res[name] = value;
        });
        $element.find(".text-select").each(function() {
            var name = $(this).parent().attr("name");
            var select2_obj_id = $(this).parent().attr("select2-obj-id");
            var $select2_obj = window[select2_obj_id];
            var values = $select2_obj.val().split(",");
            var filtered_values = [];
            for(var i=0; i<values.length; i++){
                if(values[i]){
                    filtered_values.push(values[i]);
                }
            }
            res[name] = filtered_values;
        });
        var task_type_value = $element.find(".nav-tabs[name='task_type'] li.active").attr("field-value") || "";
        res["task_type"] = task_type_value;
        var command_value = $element.find(".tab-content[name='command'] .tab-pane.active :input[name='command']").val() || "";
        res["command"] = command_value;
        return res;
    }
    
    window.form_to_conf = window.form_to_conf || function() {
        var conf = element_to_json($("#dag-container"));
        var tasks = [];
        $("#tasks-container").children("div").each(function() {
            tasks.push(element_to_json($(this)));
        });
        conf["tasks"] = tasks;
        return conf;
    };
    
    var get_task_list_item = function(item_id, item_value, no_move) {
        return [
            '<li class="task-nav" item_id="' + item_id + '">',
                '<a href="#' + item_id + '">' + item_value + (no_move? '': '<span class="glyphicon glyphicon-menu-hamburger move task-nav-move" aria-hidden="true">') + '</span></a>',
            '</li>'
        ].join("\n");
    };
    
    var refresh_task_list = function() {
        if($("#task-list").length){
            $("#task-list").empty();
            $("#tasks-container").children(".task-item").each(function() {
                var task_name = $(this).find("input[name='task_name']").val();
                if(task_name && $("#task-list").length){
                    $("#task-list").append(get_task_list_item($(this).attr("id"), task_name));
                }
            });
            $("body").scrollspy('refresh');
        }
        if(window.refresh_task_graph) window.refresh_task_graph();
    }
    
    window.add_task_html = function(html) {
        $("#tasks-container").append(html);
        refresh_task_names();
        $(".text-select").each(function() {
            window[$(this).parent().attr("select2-obj-id")].trigger("change");
        });
        $(".task-tooltip").tooltip();
        refresh_task_list()
    };
    
    window.init_conf = window.init_conf || function() {
        $("#loading").show();
        if(window.graph_loaded) window.graph_loaded = false;
        $("#dag-container").find(":input").each(function() {
            if($(this).attr("type") == "checkbox"){
                $(this).prop("checked", conf[$(this).attr("name")]);
            }else{
                $(this).val(conf[$(this).attr("name")]);
            }
        });
        $("#dag-container").find("span[name='category']").find("button").removeClass("active");
        $("#dag-container").find("span[name='category']").find("button[field-value='" + conf.category + "']").addClass("active");
        $("#tasks-container").empty();
        var tasks = conf.tasks || [];
        for(var i=0; i<tasks.length; i++){
            var task = tasks[i];
            var html = task_to_element(task);
            add_task_html(html);
        };
        refresh_last_draft_conf_string();
        $("#loading").hide();
    }
    
    $("#container-form").on("click", ".task-delete", function() {
        $(this).parent().remove();
        refresh_task_names();
        refresh_task_list();
    });
    $("#container-form").on("click", ".field-delete", function() {
        $(this).parent().remove();
    });
    $("#container-form").on("click", ".text-list-field-add", function() {
        $(this).siblings("div").append(get_text_list_single_element($(this).attr("field-name"), $(this).attr("field-value")));
    });
    $("#container-form").on("click", ".task-add", function() {
        var html = task_to_element(default_task);
        add_task_html(html);
    });
    $("#container-form").on("click", ".copy-render", function() {
        var self = this;
        var code = $(self).parent().siblings("textarea").val();
        
        var $temp = $("<textarea>");
        $("body").append($temp);
        $temp.val(code).select();
        document.execCommand("copy");
        $temp.remove();
        
        $(self).siblings(".copy-success").hide();
        setTimeout(function(){
            $(self).siblings(".copy-success").show();
        }, 0);
    });

    $("#container-form").on("click", ".render-btn", function() {
        var self = this;
        if($(self).siblings(".render-loading").is(":visible")){
            return false;
        }
        $(self).siblings(".render-loading").show();
        $(self).siblings(".render-result").html("");
        $.ajax({
            url: 'api?api=render_task_conf&format=html',
            type: 'POST',
            contentType: "application/json; charset=utf-8",
            data: JSON.stringify(element_to_json($(self).parents(".task-item"))),
            success: function(data){ 
                if(data.code == 0){
                    var res = [];
                    var rendered = data.detail;
                    for(var key in rendered){
                        res.push("<div>");
                        res.push("<h5>" + key + ' <a class="copy-render" href="javascript:void(0)"><span class="glyphicon glyphicon-copy copy-tooltip" title="copy"></span></a> <span class="glyphicon glyphicon-ok fadein copy-success" style="display: none; color: #0091a1;"></span>' + "</h5>");
                        res.push('<textarea style="display: none;">' + (rendered[key].code || '') + '</textarea>');
                        res.push(rendered[key].html);
                        res.push("</div>");
                    }
                    $(self).siblings(".render-result").html(res.join("\n"));
                    $(".copy-tooltip").tooltip();
                }else{
                    alert(data.detail);
                }
                $(self).siblings(".render-loading").hide();
            },
            error: function(){
                alert('Failed!');
                $(self).siblings(".render-loading").hide();
            }
        });
    });

    $("#container-form").on("click", ".render-dry-run-btn", function() {
        var self = this;
        if($(self).siblings(".render-loading").is(":visible")){
            return false;
        }
        $(self).siblings(".render-loading").show();
        $(self).siblings(".render-result").html("");
        $.ajax({
            url: 'api?api=dry_run_task_conf&format=html',
            type: 'POST',
            contentType: "application/json; charset=utf-8",
            data: JSON.stringify(element_to_json($(self).parents(".task-item"))),
            success: function(data){ 
                if(data.code == 0){
                    var res = [];
                    res.push("<h5>Dry Run Log</h5>");
                    res.push("<pre>" + data.detail.log + "</pre>");
                    res.push("<hr>")
                    var rendered = data.detail.rendered;
                    for(var key in rendered){
                        res.push("<div>");
                        res.push("<h5>" + key + ' <a class="copy-render" href="javascript:void(0)"><span class="glyphicon glyphicon-copy copy-tooltip" title="copy"></span></a> <span class="glyphicon glyphicon-ok fadein copy-success" style="display: none; color: #0091a1;"></span>' + "</h5>");
                        res.push('<textarea style="display: none;">' + (rendered[key].code || '') + '</textarea>');
                        res.push(rendered[key].html);
                        res.push("</div>");
                    }
                    $(self).siblings(".render-result").html(res.join("\n"));
                    $(".copy-tooltip").tooltip();
                }else{
                    alert(data.detail);
                }
                $(self).siblings(".render-loading").hide();
            },
            error: function(){
                alert('Failed!');
                $(self).siblings(".render-loading").hide();
            }
        });
    });

    $("#container-form").on("click", ".nav-tabs a", function(e) {
        e.preventDefault();
        if(!readonly){
            $(this).tab('show');
        };
    });

    $("#container-form").on("click", ".btn-group-input-single button.btn-item", function(e) {
        if(!readonly){
            $(this).siblings("button.btn-item").removeClass("active");
            $(this).addClass("active");
        };
    });
    
    $("#container-form").on("change", "input[name='task_name']", function() {
        $(this).parents(".task-item").attr("task_name", $(this).val());
        refresh_task_names();
        refresh_task_list();
    });
    
    $('body').on('focus', ".date-range-picker", function(){
        faForm.applyStyle($(this), $(this).attr('data-role'));
    });

    $("body").on("click", ".advanced-settings-expand-all", function(e) {
        $('.advanced-settings-collapse .collapse').collapse('show');
    });

    $("body").on("click", ".advanced-settings-collapse-all", function(e) {
        $('.advanced-settings-collapse .collapse').collapse('hide');
    });

    if(!readonly && $("#task-list").length){
        $('#task-list').sortable({
            // Omit this to make then entire <li>...</li> draggable.
            handle: '.move', 
            update: function() {
                var $task_items = new Array($('#task-list').find('.task-nav').length);
                $('#task-list').find('.task-nav').each(function(index, elem) {
                     var $listItem = $(elem),
                         newIndex = $listItem.index();
                     $task_items[newIndex] = $("#tasks-container").children("#" + $listItem.attr("item_id")).detach();
                });
                for(var i=$task_items.length - 1; i >= 0; i--){
                    $("#tasks-container").prepend($task_items[i]);
                }
                $("body").scrollspy('refresh');
            }
        });
    };
    
    $('body').scrollspy({
        target: '.bs-docs-sidebar',
        offset: 40
    });
    
    if($("#task-list").length){
        $(window).resize(function() {
            $("#task-list").height($(window).height() - ($("#task-list").offset().top - $(document).scrollTop()));
        });
        $("#task-list").height($(window).height() - ($("#task-list").offset().top - $(document).scrollTop()));
    }
    
    var update_dag = function(success_cb, error_cb) {
        if(dag_name && dag_name != $("#dag_name").val()){
            if(!confirm("Are you sure to change the DAG name?")){
                return false;
            }
        }
        success_cb = success_cb || function() {};
        error_cb = error_cb || function() { alert('Failed!'); };
        save_draft();
        var data = form_to_conf();
        $.ajax({
            url: 'api?api=update_dag&dag_name=' + dag_name,
            type: 'POST',
            contentType: "application/json; charset=utf-8",
            data: JSON.stringify(data),
            success: success_cb,
            error: error_cb
        });
    };
    var refresh_dag = function(success_cb, error_cb) {
        success_cb = success_cb || function() {};
        error_cb = error_cb || function() {alert('Failed!'); };
        $.ajax({
            url: refresh_url_template.replace(/DAG_ID/g, $("#dag_name").val()),
            type: 'GET',
            success: success_cb,
            error: error_cb
        });
    };
    var view_dag = function() {
        location.href = graph_url_template.replace(/DAG_ID/g, $("#dag_name").val());
    };
    
    $("#save-btn").click(function() {
        update_dag(function(data){ 
            if(data.code == 0){
                location.href = "list";
            }else{
                alert(data.detail);
            }
        });
    });
    $("#save-add-btn").click(function() {
        update_dag(function(data){ 
            if(data.code == 0){
                location.href = "?";
            }else{
                alert(data.detail);
            }
        });
    });
    $("#save-edit-btn").click(function() {
        update_dag(function(data){ 
            if(data.code == 0){
                location.href = "?dag_name=" + $("#dag_name").val();
            }else{
                alert(data.detail);
            }
        });
    });
    $("#save-view").click(function() {
        update_dag(function(data){ 
            if(data.code == 0){
                refresh_dag(view_dag);
            }else{
                alert(data.detail);
            }
        });
    });
    $("#view").click(function() {
        refresh_dag(view_dag);
    });
    $(".checkout-version").click(function() {
        var self = this;
        $("#version-loading").show();
        $.ajax({
            url: 'api?api=get_dag&dag_name=' + dag_name + '&version=' + $(self).attr("version"),
            type: 'GET',
            success: function(data){ 
                if(data.code == 0){
                    $("#version").html($(self).attr("version"));
                    conf = data.detail.conf;
                    init_conf();
                }else{
                    alert(data.detail);
                }
                $("#version-loading").hide();
            },
            error: function() {
                alert('Failed!');
                $("#version-loading").hide();
            }
        });
    });
    
    var DRAFT_LENGTH = 15;
    var DRAFT_AUTO_SAVE_INTERVAL = 1000 * 15;
    var drafts = [];
    var draft_ts_to_id = {};
    var draft_storage_key = "dcmp_dag_" + dag_name + "_draft";
    var last_draft_conf_string = null;
    
    window.refresh_last_draft_conf_string = function() {
        last_draft_conf_string = JSON.stringify(form_to_conf());
    };
    
    var reload_draft = function() {
        drafts = [];
        draft_ts_to_id = {};
        try{
            drafts = JSON.parse(localStorage.getItem(draft_storage_key)) || [];
        }catch(err){
            ;
        };
        $("#draft-container").empty();
        if(drafts.length == 0){
            $("#draft-container").append('<li><a href="javascript:void(0)">None</a></li>');
        }else{
            drafts.sort(function(a, b){return (a.ts < b.ts)? 1: -1});
            for(var i=0; i<drafts.length; i++){
                var draft = drafts[i];
                draft_ts_to_id[draft.ts] = i;
                $("#draft-container").append('<li><a href="javascript:void(0)" class="checkout-draft" ts="' + draft.ts + '">' + (new Date(draft.ts)).Format("yyyy-MM-dd hh:mm:ss") + '</a></li>');
            };
        };
    };
    
    window.save_draft = function() {
        var data = form_to_conf($.extend(true, {}, conf));
        if(last_draft_conf_string != JSON.stringify(data)){
            last_draft_conf_string = JSON.stringify(data);
            var draft = {
                ts: (new Date()).getTime(),
                conf: data
            };
            drafts.unshift(draft);
            while(drafts.length > DRAFT_LENGTH){
                drafts.pop();
            };
            localStorage.setItem(draft_storage_key, JSON.stringify(drafts));
        };
    };

    $("#draft-container").on("click", ".checkout-draft", function() {
        $("#draft-loading").show();
        var draft = drafts[draft_ts_to_id[+$(this).attr("ts")]];
        if(draft && draft.conf){
            conf = draft.conf;
            init_conf();
        }
        $("#draft-loading").hide();
    });

    reload_draft();
    $("#draft-loading").hide();

    if(!readonly){
        var auto_save_draft = function() {
            save_draft();
            reload_draft();
            setTimeout(auto_save_draft, DRAFT_AUTO_SAVE_INTERVAL);
        };
        setTimeout(auto_save_draft, DRAFT_AUTO_SAVE_INTERVAL);
    };

    $("#export-json-btn").click(function() {
        var data_str = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(form_to_conf()));
        var dl_anchor_elem = document.getElementById('download-anchor-elem');
        dl_anchor_elem.setAttribute("href", data_str);
        dl_anchor_elem.setAttribute("download", $("#dag_name").val() + ".json");
        dl_anchor_elem.click();
    });
    
    var readBlob = function(cb) {
        var files = document.getElementById('import-file').files;
        if (!files.length) {
            alert('Please select a file!');
            return;
        }
        var file = files[0];

        var reader = new FileReader();
        
        // If we use onloadend, we need to check the readyState.
        reader.onloadend = function(evt) {
            if (evt.target.readyState == FileReader.DONE) { // DONE == 2
                cb(evt.target.result);
                document.getElementById('import-file').value = "";
            }
        };

        reader.readAsText(file);
    }
    
    $("#import-json-btn").click(function() {
        $("#import-file").click();
    });
    $("#import-file").change(function() {
        readBlob(function(result) {
            conf = JSON.parse(result);
            init_conf();
        });
    });
    
    init_conf();
    
})(jQuery);