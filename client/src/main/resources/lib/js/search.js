var instance = jsPlumb.getInstance({
    Endpoint: ["Dot", {radius: 2}],
    Connector:"StateMachine",
    HoverPaintStyle: {strokeStyle: "#f44336", lineWidth: 2 },
    ConnectionOverlays: [
        [ "Arrow", {
            location: 1,
            id: "arrow",
            length: 14,
            foldback: 0.8
        } ]
    ],
    Container: "canvas"
});

instance.registerConnectionType("basic", { anchor:"Continuous", connector:"StateMachine" });

window.jsp = instance;

var canvas = document.getElementById("canvas");
var windows = jsPlumb.getSelector(".statemachine-demo .w");


instance.bind("click", function (c) {
    instance.detach(c);
});


jsPlumb.fire("jsPlumbDemoLoaded", instance);

var initNode = function(el) {
    instance.draggable(el);
    instance.makeSource(el, {
        filter: ".ep",
        anchor: "Continuous",
        connectorStyle: { strokeStyle: "#26a69a", lineWidth: 2, outlineColor: "transparent", outlineWidth: 4 },
        connectionType:"basic",
        extract:{
            "action":"the-action"
        }
    });
    instance.makeTarget(el, {
        dropOptions: { hoverClass: "dragHover" },
        anchor: "Continuous",
        allowLoopback: true
    });
};

var newNode = function(x, y, fields, frontname, backname) {
    var d = document.createElement("div");
    var id = jsPlumbUtil.uuid();
    d.className = "w";
    d.id = "box-" + backname + "-" + id;

    var innerhtml = "";
    innerhtml += "<div class=\"btnClose\"><i class=\"material-icons tiny\">close</i></div>";
    innerhtml += "<div style=\"height:20px\">";
    if(fields.length > 0){
        innerhtml += "<div class=\"btnSettings\" style=\"float:left\"><i class=\"tiny material-icons\">mode_edit</i></div>&nbsp;&nbsp;";
    }
    innerhtml += frontname;
    innerhtml += "</div>";
    if(fields.length > 0){
        innerhtml += "<div class=\"settings\" style=\"display:none\">";
        innerhtml += fields;
        innerhtml += "</div>";
    }
    innerhtml += "<div class=\"res\"></div>";
    innerhtml += "<div class=\"ep\"></div>";


    d.innerHTML = innerhtml;
    d.style.left = x + "px";
    d.style.top = y + "px";
    d.setAttribute("data-adamtype", backname);
    document.getElementById("canvas").appendChild(d);
    initNode(d);

    if(fields.length > 0){
        $("#" + d.id + " > div > .btnSettings").click(function() {
            $("#" + d.id + " > .settings").toggle("slow");
        });
    }

    $("#" + d.id + " > .btnClose").click(function(e) {
        instance.remove($("#" + d.id));
        e.stopPropagation();
    });

    return d;
};



//init
for (var i = 0; i < windows.length; i++) {
    initNode(windows[i], true);
}

//add index
$("#btnAddIndex").click(function(){
    var innerhtml = "";
    innerhtml += "<input type=\"text\" class=\"form-control\" name=\"indexname\" placeholder=\"indexname\">";

    newNode(0, 0, innerhtml, $("#indextype option:selected").text(), $("#indextype").val());
});

//add operation
$("#btnAddOperation").click(function(){
    var innerhtml = "";
    innerhtml += "<div class=\"input-field\"><select id=\"operationorder\" class=\"browser-default\"><option value=\"parallel\" selected=\"selected\">parallel</option><option value=\"left\">left first</option><option value=\"right\">right first</option> </select></div>";

    newNode(0, 0, innerhtml, $("#indexoperation option:selected").text(), $("#indexoperation").val());
});

//submit operation
$("#btnSubmit").click(function(){
    if($("#entityname").val().length === 0){
        showAlert(" Please specify an entity."); return;
    } else if($("#query").val().length === 0){
        showAlert(" Please specify a query vector."); return;
    }

    $("#btnSubmit").addClass('disabled');
    $("#btnSubmit").prop('disabled', true);

    $("#progress").show();


    $.ajax("/query", {
        data: JSON.stringify(evaluate("box-start")),
        contentType: 'application/json',
        type: 'POST',
        success: function(data){
            $.each(data.intermediate_responses, function(idx, val){
                $("#" + val.id + " > .res").html("execution time: " + val.time + "ms" + "<br/>" + "results: " + val.length);
            });
            $("#progress").hide()
            $("#btnSubmit").removeClass('disabled');
            $("#btnSubmit").prop('disabled', false);
        }
    });
});


//evaluate canvas
var evaluate = function(id){
    var type = $("#" + id).attr("data-adamtype");

    switch (type) {
        case "start":
            var result = {};

            var targets = getTargets(id); //TODO: only one target allowed!

            result.id = id;
            result.operation = "start";
            result.options = {};
            result.options.entityname = $("#entityname").val();
            result.options.query = $("#query").val();
            result.targets = $.map(targets, function(val, i){return evaluate(val)});

            return result;
            break;
        case "ecp":
        case "lsh":
        case "pq":
        case "sh":
        case "vaf":
        case "vav":
            var result = {};

            result.id = id;
            result.operation = "indexscan";
            result.options = {};

            result.options.indextype = type;
            $("#" + id + " > .settings").children(":input").each(function() {
                if($(this).val().length > 0){
                    result.options[$(this).attr('name')] =  $(this).val();
                }
            });

            //TODO: possibly switch to "local" information
            result.options.entityname = $("#entityname").val();
            result.options.query = $("#query").val();

            return result;
            break;
        case "union":
        case "intersect":
        case "except":
            var result = {};

            var targets = getTargets(id); //TODO: only two targets allowed!

            result.id = id;
            result.operation = "aggregate";
            result.options = {};
            result.options.aggregation = type;
            result.targets = $.map(targets, function(val, i){return evaluate(val)});

            return result;
            break;
        default:
            return {};
    }
};

var getTargets = function(id) {
    var connections = instance.getConnections({ source:id });
    var targets = $.map( connections, function( val, i ) {
        return val.targetId
    });
    return targets;
};


