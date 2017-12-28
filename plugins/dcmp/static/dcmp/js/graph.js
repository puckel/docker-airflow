(function() {
    window.graph_loaded = false;

    var arrange = "LR";
    var graph_prefix = "graph_";
    
    var job_ids = [];
    var job_id_to_name = {};
    var name_to_job_id = {};
    
    var g = null;
    var layout = null;
    var renderer = null;
    
    var highlight_color = "#000000";
    var upstream_color = "#2020A0";
    var downstream_color = "#0000FF";
    
    var duration = 200;
    
    var count_task_name = function(task_name) {
        var count = 0;
        for(var i=0; i<conf.tasks.length; i++){
            var task = conf.tasks[i];
            if(task.task_name == task_name){
                count++;
            }
        }
        return count;
    }
    
    var highlight_nodes = function(nodes, color) {
        nodes.forEach (function (nodeid) {
            my_node = d3.select('#' + graph_prefix + nodeid + ' rect');
            my_node.style("stroke", color);
        })
    };
    
    var cur_job_name = null;
    var cur_task_index = null;
    
    var get_cur_active_job_id = function(){
        if(cur_task_index == null){
            return "dag-container";
        }else{
            return "" + cur_task_index;
        }
    };
    
    var clear_nodes_class = function() {
        d3.selectAll('.node').each(function(d) {
            $(this).attr("class", "node enter");
        });
    };
    
    var change_nodes_class = function(nodes, class_name) {
        nodes.forEach (function (nodeid) {
            $('#' + graph_prefix + nodeid).attr("class", "node enter " + class_name);
        });
    };
    
    var refresh_job = function(job_id) {
        save_job_containers();
        $("#dag-container").hide();
        if(job_id == "dag-container"){
            cur_task_index = null;
            $("#dag-container").show();
        }else{
            cur_task_index = job_id;
        }
        refresh_job_containers();
        clear_nodes_class();
        change_nodes_class([job_id], "active");
        change_nodes_class(g.predecessors(job_id), "upstream");
        change_nodes_class(g.successors(job_id), "downstream");
    };
    
    var refresh_cur_job = function() {
        refresh_job(get_cur_active_job_id());
    };
    
    var refresh_job_by_name = function(job_name) {
        var job_id = name_to_job_id[job_name] || "dag-container";
        refresh_job(job_id);
    };
    
    window.change_cur_job = function(job_id) {
        if(job_id == null){
            job_id = "dag-container";
        }
        job_id = ""+job_id;
        var job_name = job_id_to_name[job_id];
        if(job_name == null){
            job_name = "DAG";
        }
        if(cur_job_name != job_name){
            refresh_job(job_id);
            cur_job_name = job_name;
        }
    };
    
    var check_cur_name = function() {
        var err_message = "";
        var cur_active_job_id = get_cur_active_job_id();
        if(cur_active_job_id == "dag-container"){
            return true;
        }
        var cur_name = job_id_to_name[cur_active_job_id];
        if(!cur_name){
            alert("Current task name can not be blank.");
            return false;
        }
        if(count_task_name(cur_name) > 1){
            alert("Current task name is not unique.");
            return false;
        }
        return true;
    };
    
    var refresh_upstreams = function() {
        if(cur_task_index != null){
            var new_cur_job_name = $(".task-item input[name='task_name']").val();
            if(new_cur_job_name != null && new_cur_job_name != cur_job_name){
                for(var i=0; i<conf.tasks.length; i++){
                    var task = conf.tasks[i];
                    for(var j=0; j<task.upstreams.length; j++){
                        if(task.upstreams[j] == cur_job_name){
                            task.upstreams[j] = new_cur_job_name;
                        }
                    }
                }
                cur_job_name = new_cur_job_name;
            }
        }
    };
    
    var refresh_dag = function() {
        if(graph_loaded){
            save_job_containers();
            refresh_upstreams();
        }
    
        var nodes = [{
            "id": "dag-container", 
            "value": {
                "style": "fill:#e8f7e4;", 
                "labelStyle": "fill:#000;", 
                "label": "DAG"
            }
        }];
        var edges = [];
    
        job_ids = ["dag-container"];
        job_id_to_name = {
            "dag-container": "DAG"
        };
        name_to_job_id = {
            "DAG": "dag-container"
        };
        for(var i=0; i<conf.tasks.length; i++){
            var task = conf.tasks[i];
            var task_id = "" + i;
            job_ids.push(task_id);
            job_id_to_name[task_id] = task.task_name;
            if(name_to_job_id[task.task_name] == null){
                name_to_job_id[task.task_name] = task_id;
            };
        }
        for(var i=0; i<conf.tasks.length; i++){
            var task = conf.tasks[i];
            nodes.push({
                "id": "" + i, 
                "value": {
                    "style": "fill:" + (task_categorys_dict[task.task_category || ""] || task_category_default_color) + ";", 
                    "labelStyle": "fill:#000;", 
                    "label": task.task_name
                }
            });
            var upstream_ids = [];
            for(var j=0; j<task.upstreams.length; j++){
                var upstream = task.upstreams[j];
                var upstream_id = name_to_job_id[upstream];
                if(upstream_id){
                    upstream_ids.push(upstream_id);
                }else{
                    ;
                }
            }
            if(upstream_ids.length == 0){
                upstream_ids.push("dag-container");
            }
            for(var j=0; j<upstream_ids.length; j++){
                var upstream_id = upstream_ids[j];
                if(job_ids.indexOf(upstream_id) == -1){
                    continue;
                }
                edges.push({
                    "u": upstream_id, 
                    "v": "" + i
                });
            }
        }
        g = dagreD3.json.decode(nodes, edges);
        layout = dagreD3.layout().rankDir(arrange).nodeSep(15).rankSep(15);
        renderer = new dagreD3.Renderer();
        renderer.edgeInterpolate(line_interpolate);
        renderer.layout(layout).run(g, d3.select("#dig"));
        
        d3.selectAll('.node').each(function(d) {
            $(this).attr("id", graph_prefix + d);
        });
    
        if(!graph_loaded){
            refresh_job_by_name(cur_job_name);
            graph_loaded = true;
        }
    
        d3.selectAll("g.node").on("click", function(d){
            if(get_is_changing_upstreams()){
                change_upstreams(d);
            }else{
                if(!check_cur_name()){
                    return false;
                }
                change_cur_job(d);
            }
        });
    
        d3.selectAll("g.node").on("mouseover", function(d){
            d3.select(this).selectAll("rect").style("stroke", highlight_color);
            if(!get_is_changing_upstreams()){
                highlight_nodes(g.predecessors(d), upstream_color);
                highlight_nodes(g.successors(d), downstream_color);
            }
        });
    
        d3.selectAll("g.node").on("mouseout", function(d){
            d3.select(this).selectAll("rect").style("stroke", null);
            highlight_nodes(g.predecessors(d), null);
            highlight_nodes(g.successors(d), null);
        });
    
        d3.select("#searchbox").on("keyup", function(){
            var match = refresh_search_nodes();
            if(match) {
                var transform = d3.transform(d3.select(match).attr("transform"));
                transform.translate = [
                    -transform.translate[0] + ($("#svg_container svg").width() / 2.2),
                    -(transform.translate[1] - ($("#svg_container svg").height() / 2))
                ];
                transform.scale = [1, 1];
    
                d3.select("g.zoom")
                    .transition()
                    .attr("transform", transform.toString());
            }
        });
    };
    
    var refreshing_task_graph = false;
    window.refresh_task_graph = function() {
        if(!refreshing_task_graph){
            refreshing_task_graph = true;
            $("#loading").show();
            setTimeout(function() {
                $("#loading").show();
                refresh_dag();
                refreshing_task_graph = false;
                $("#loading").hide();
            }, 0);
        }
    };
    
    var refresh_dag_container = function() {
        $("#dag-container").find(":input").each(function() {
            if($(this).attr("type") == "checkbox"){
                $(this).prop("checked", conf[$(this).attr("name")]);
            }else{
                $(this).val(conf[$(this).attr("name")]);
            }
        });
        $("#dag-container").find("span[name='category']").find("button").removeClass("active");
        $("#dag-container").find("span[name='category']").find("button[field-value='" + conf.category + "']").addClass("active");
    };
    
    var save_dag_container = function(conf_to_change) {
        if(!conf_to_change){
            conf_to_change = conf;
        };
        if(!readonly){
            var tasks = conf_to_change.tasks || [];
            var new_conf = element_to_json($("#dag-container"));
            for (var prop in conf_to_change) {
                if (conf_to_change.hasOwnProperty(prop)) {
                    delete conf_to_change[prop];
                };
            };
            for (var prop in new_conf) {
                if (new_conf.hasOwnProperty(prop)) {
                    conf_to_change[prop] = new_conf[prop];
                };
            };
            conf_to_change["tasks"] = tasks;
        }
    };
    
    var refresh_task_container = function() {
        $("#tasks-container").empty();
        if(cur_task_index != null){
            var tasks = conf.tasks || [];
            var task = tasks[cur_task_index];
            var html = task_to_element(task);
            add_task_html(html);
        }
    };
    
    var save_task_container = function(conf_to_change) {
        if(!conf_to_change){
            conf_to_change = conf;
        };
        if(!readonly && $(".task-item").length > 0){
            var task = conf_to_change.tasks[+cur_task_index];
            if(task){
                var upstreams = task.upstreams;
                conf_to_change.tasks[+cur_task_index] = element_to_json($(".task-item"));
                conf_to_change.tasks[+cur_task_index]["upstreams"] = upstreams;
            }
        };
    };
    
    var refresh_job_containers = function() {
        refresh_dag_container();
        refresh_task_container();
    };
    
    var save_job_containers = function(conf_to_change) {
        if(!conf_to_change){
            conf_to_change = conf;
        };
        if(!readonly){
            save_dag_container(conf_to_change);
            save_task_container(conf_to_change);
        }
    };
    
    window.init_conf = function() {
        $("#loading").show();
        graph_loaded = false;
        cur_task_index = null;
        refresh_job_containers();
        refresh_task_graph();
        setTimeout(function() {
            refresh_last_draft_conf_string();
        }, 0);
    };
    
    window.form_to_conf = function(conf_to_change) {
        if(!conf_to_change){
            conf_to_change = conf;
        };
        save_job_containers(conf_to_change);
        return conf_to_change;
    };
    
    var disabled_node_ids = [];
    
    var refresh_search_nodes = function() {
        var s = document.getElementById("searchbox").value;
        var match = null;
        d3.selectAll("g.nodes g.node").filter(function(d, i){
            if (s=="" && disabled_node_ids.length==0){
                d3.select("g.edgePaths")
                    .transition().duration(duration)
                    .style("opacity", 1);
                d3.select(this)
                    .transition().duration(duration)
                    .style("opacity", 1)
                    .selectAll("rect")
                    .style("stroke-width", "2px");
            }
            else{
                d3.select("g.edgePaths")
                    .transition().duration(duration)
                    .style("opacity", 0.2);
                if (disabled_node_ids.indexOf(d) == -1 && s && job_id_to_name[d].indexOf(s) > -1) {
                    if (!match)
                        match = this;
                    d3.select(this)
                        .transition().duration(duration)
                        .style("opacity", 1)
                        .selectAll("rect")
                        .style("stroke-width", "10px");
                } else if(disabled_node_ids.indexOf(d) == -1){
                    d3.select(this)
                        .transition().duration(duration)
                        .style("opacity", 1)
                        .selectAll("rect")
                        .style("stroke-width", "2px");
                } else {
                    d3.select(this)
                        .transition()
                        .style("opacity", 0.2).duration(duration)
                        .selectAll("rect")
                        .style("stroke-width", "2px");
                }
            }
        });
        return match;
    }
    
    var get_is_changing_upstreams = function() {
        return $("#change_upstreams").hasClass("active");
    }
    
    var enable_change_upstreams = function() {
        var cur_active_job_id = get_cur_active_job_id();
        if(cur_active_job_id == "dag-container"){
            alert("DAG can not change upstreams, select a task first.");
            return false;
        }
        if(!check_cur_name()){
            return false;
        }
        disabled_node_ids = ["dag-container", cur_active_job_id];
        var add_disable_node_ids = function(job_ids) {
            for(var i=0; i<job_ids.length; i++){
                var job_id = job_ids[i];
                if(disabled_node_ids.indexOf(job_id) == -1){
                    disabled_node_ids.push(job_id);
                    add_disable_node_ids(g.successors(job_id));
                };
            };
        };
        add_disable_node_ids(g.successors(cur_active_job_id));
        refresh_search_nodes();
        $("#change_upstreams").addClass("active");
    };
    
    var disable_change_upstreams = function() {
        disabled_node_ids = [];
        refresh_search_nodes();
        $("#change_upstreams").removeClass("active");
    };
    
    var change_upstreams = function(d) {
        if(disabled_node_ids.indexOf(d) > -1){
            return false;
        }
        var cur_job_id = get_cur_active_job_id();
        var upstream_name = job_id_to_name[d];
        if(!upstream_name){
            return false;
        }
        var task = conf.tasks[+cur_job_id];
        if(!task){
            return false;
        }
        var upstreams = task.upstreams || [];
        var filtered_upstreams = [];
        var add_upstream = true;
        for(var i=0; i<upstreams.length; i++){
            var upstream = upstreams[i];
            if(upstream){
                if(upstream == upstream_name){
                    add_upstream = false;
                    continue;
                }
                filtered_upstreams.push(upstream);
            }
        }
        if(add_upstream){
            filtered_upstreams.push(upstream_name);
        }
        conf.tasks[+cur_job_id]["upstreams"] = filtered_upstreams;
        refresh_task_graph();
        setTimeout(function() {
            refresh_cur_job();
            disable_change_upstreams();
        }, 0);
    };
    
    var get_downstream_tasks = function(task_name) {
        var downstream_tasks = [];
        for(var i=0; i<conf.tasks.length; i++){
            var task = conf.tasks[i];
            if(task.upstreams.indexOf(task_name) > -1){
                downstream_tasks.push(task);
            }
        }
        return downstream_tasks;
    };
    
    (function($) {
        var change_widecreen = function(is_widescreen) {
            if(is_widescreen){
                $(".container").addClass("widescreen");
                $("#svg_container svg").attr("height", 800);
            }else{
                $(".container").removeClass("widescreen");
                $("#svg_container svg").attr("height", 700);
            }
        };
        $("#wide-screen").change(function() {
            Cookies.set("widescreen", $(this).prop("checked")? "1": "0", { expires: 3650 });
            change_widecreen($(this).prop("checked"));
        });
        if($("#wide-screen").length > 0){
            $("#wide-screen").prop("checked", Cookies.get("widescreen") == "1");
            change_widecreen($("#wide-screen").prop("checked"));
        };
        
        $("#add_task").click(function() {
            if(!check_cur_name()){
                return false;
            }
            disable_change_upstreams();
            var task = $.extend({}, default_task);
            var cur_task_name = job_id_to_name[get_cur_active_job_id()];
            if(cur_task_name && cur_task_name != "DAG"){
                task["upstreams"] = [cur_task_name];
            }
            conf.tasks.push(task);
            cur_job_name = null;
            refresh_task_graph();
            setTimeout(function() {
                change_cur_job(""+(conf.tasks.length - 1));
            }, 0);
        });
        $("#remove_task").click(function() {
            if(cur_task_index == null){
                alert("You can not remove DAG.");
                return false;
            }
            if(confirm("Are you sure you want to delete this task?")){
                disable_change_upstreams();
                var cur_task = conf.tasks.splice(+cur_task_index, 1);
                if(cur_task.length > 0){
                    cur_task = cur_task[0];
                    var cur_upstreams = cur_task.upstreams;
                    var downstream_tasks = get_downstream_tasks(cur_task.task_name);
                    for(var i=0; i<downstream_tasks.length; i++){
                        var downstream_task = downstream_tasks[i];
                        downstream_task.upstreams.splice(downstream_task.upstreams.indexOf(cur_task.task_name), 1);
                        for(var j=0; j<cur_upstreams.length; j++){
                            var cur_upstream = cur_upstreams[j];
                            if(downstream_task.upstreams.indexOf(cur_upstream) == -1){
                                downstream_task.upstreams.push(cur_upstream);
                            };
                        };
                    };
                }
                cur_task_index = null;
                cur_job_name = null;
                refresh_job_containers();
                refresh_task_graph();
                setTimeout(function() {
                    change_cur_job("dag-container");
                }, 0);
            }
        });
        $("#container-form #tasks-container").on("change", "input[name='task_name']", function() {
            var cur_name = job_id_to_name[get_cur_active_job_id()];
            if(!$(this).val()){
                alert("Current task name can not be blank.");
                $(this).val(cur_name);
                return false;
            }
            if(count_task_name($(this).val()) > 0){
                alert("Current task name is not unique.");
                $(this).val(cur_name);
                return false;
            }
        });
        $("#container-form").on("click", ".btn-group-input-single[name='task_category'] button.btn-item", function(e) {
            if(!readonly){
                refresh_task_graph();
            };
        });
        $("#change_upstreams").click(function() {
            if(get_is_changing_upstreams()){
                disable_change_upstreams();
            }else{
                enable_change_upstreams();
            }
        });
        $("#view_streams").click(function() {
            if(!check_cur_name()){
                return false;
            }
            var sub_conf = $.extend({}, conf);
            var cur_active_job_id = get_cur_active_job_id();
            var cur_task = conf.tasks[+cur_active_job_id];
            var active_job_id = "dag-container";
            if(cur_task){
                var new_tasks = [];
                
                var downstream_job_ids = [];
                var add_downstream_job_ids = function(job_ids) {
                    for(var i=0; i<job_ids.length; i++){
                        var job_id = job_ids[i];
                        if(downstream_job_ids.indexOf(job_id) == -1){
                            downstream_job_ids.push(job_id);
                            add_downstream_job_ids(g.successors(job_id));
                        };
                    };
                };
                add_downstream_job_ids(g.successors(cur_active_job_id));
                
                var upstream_job_ids = [];
                var add_upstream_job_ids = function(job_ids) {
                    for(var i=0; i<job_ids.length; i++){
                        var job_id = job_ids[i];
                        if(job_id != "dag-container" && upstream_job_ids.indexOf(job_id) == -1){
                            upstream_job_ids.push(job_id);
                            add_upstream_job_ids(g.predecessors(job_id));
                        };
                    };
                };
                add_upstream_job_ids(g.predecessors(cur_active_job_id));
                
                var new_task_names = [];
                
                for(var i=0; i<conf.tasks.length; i++){
                    var new_task = $.extend({}, conf.tasks[i]);
                    var job_id = ""+i;
                    if(job_id == cur_active_job_id){
                        new_tasks.push(new_task);
                        new_task_names.push(new_task.task_name);
                        active_job_id = ""+(new_tasks.length - 1);
                    }else if(downstream_job_ids.indexOf(job_id) > -1 || upstream_job_ids.indexOf(job_id) > -1){
                        new_tasks.push(new_task);
                        new_task_names.push(new_task.task_name);
                    };
                };
                
                for(var i=0; i<new_tasks.length; i++){
                    var new_task = new_tasks[i];
                    var new_task_upstreams = [];
                    for(var j=0; j<new_task.upstreams.length; j++){
                        var upstream = new_task.upstreams[j];
                        if(new_task_names.indexOf(upstream) > -1){
                            new_task_upstreams.push(upstream);
                        };
                    };
                    new_task["upstreams"] = new_task_upstreams;
                };
                
                sub_conf["tasks"] = new_tasks;
            }
            $("#graph-display-conf").val(JSON.stringify(sub_conf));
            $("#graph-display-active-job-id").val(active_job_id);
            $("#graph-display-form").submit();
            $("#graph-display-modal").modal();
        });
        $("#clear_search").click(function() {
            $("#searchbox").val("");
            refresh_search_nodes();
            var transform = d3.transform(d3.select("#" + graph_prefix + get_cur_active_job_id()).attr("transform"));
            transform.translate = [
                -transform.translate[0] + ($("#svg_container svg").width() / 2.2),
                -(transform.translate[1] - ($("#svg_container svg").height() / 2))
            ];
            transform.scale = [1, 1];
    
            d3.select("g.zoom")
                .transition()
                .attr("transform", transform.toString());
        });
        refresh_task_graph();
    })(jQuery);
})();