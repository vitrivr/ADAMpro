var instance

$(document).ready(function () {
    setupPlumb("canvas")
});


function setupPlumb(id){
    instance = jsPlumb.getInstance({
            Endpoint: ["Dot", {radius: 2}],
            Connector: "StateMachine",
            HoverPaintStyle: {strokeStyle: "#f44336", lineWidth: 2},
            ConnectionOverlays: [
                ["Arrow", {
                    location: 1,
                    id: "arrow",
                    length: 14,
                    foldback: 0.8
                }]
            ],
            Container: id
        });
    
    instance.registerConnectionType("basic", {anchor: "Continuous", connector: "StateMachine"});
    window.jsp = instance;
    
    var canvas = $("#" + id);
    var windows = jsPlumb.getSelector(".statemachine-demo .w");
    
    instance.bind("click", function (c) {
        instance.detach(c);
    });

    jsPlumb.fire("jsPlumbDemoLoaded", instance);
    
    //init
    for (var i = 0; i < windows.length; i++) {
        initNode(windows[i], true);
    }
}


function initNode(node) {
    instance.draggable(node);
    instance.makeSource(node, {
        filter: ".ep",
        anchor: "Continuous",
        connectorStyle: {strokeStyle: "#26a69a", lineWidth: 2, outlineColor: "transparent", outlineWidth: 4},
        connectionType: "basic",
        extract: {
            "action": "the-action"
        }
    });
    instance.makeTarget(node, {
        dropOptions: {hoverClass: "dragHover"},
        anchor: "Continuous",
        allowLoopback: true
    });
};


function newNode (x, y, frontname, operation, subtype, fields) {
    var d = document.createElement("div");
    var id = jsPlumbUtil.uuid();
    d.className = "w";
    d.id = "box-" + subtype + "-" + id;

    var innerhtml = "";
    innerhtml += "<div class=\"btnClose\"><i class=\"material-icons tiny\">close</i></div>";
    innerhtml += "<div style=\"height:20px\">";
    if (fields != null && fields.length > 0) {
        innerhtml += "<div class=\"btnSettings\" style=\"float:left\"><i class=\"tiny material-icons\">mode_edit</i></div>&nbsp;&nbsp;";
    }
    innerhtml += frontname;
    innerhtml += "</div>";
    if (fields != null && fields.length > 0) {
        innerhtml += "<div class=\"settings\" style=\"display:none\">";
        innerhtml += fields;
        innerhtml += "</div>";
    }
    innerhtml += "<div class=\"res\"></div>";
    innerhtml += "<div class=\"ep\"></div>";

    d.innerHTML = innerhtml;
    d.style.left = x + "px";
    d.style.top = y + "px";
    d.setAttribute("data-operation", operation);
    d.setAttribute("data-subtype", subtype);

    $("#canvas").append(d);
    initNode(d);

    if (fields != null && fields.length > 0) {
        $("#" + d.id + " > div > .btnSettings").click(function () {
            $("#" + d.id + " > .settings").toggle("slow");
        });
    }

    $("#" + d.id + " > .btnClose").click(function (e) {
        instance.remove($("#" + d.id));
        e.stopPropagation();
    });

    return d;
};


//add index
$("#btnAddIndex").click(function () {
    var option = $("#indextype option[value=" + $("#indextype").val() + "]");
    var frontname = option.text();
    var operation = option.data("operation");
    var subtype = option.data("subtype");
    var details = option.data("details");
    newNode(0, 0, frontname, operation, subtype, details);
    $('select').material_select();
});

//add operation
$("#btnAddOperation").click(function () {
    var option = $("#indexoperation option[value=" + $("#indexoperation").val() + "]");
    var frontname = option.text();
    var operation = option.data("operation");
    var subtype = option.data("subtype");
    var details = option.data("details");
    newNode(0, 0, frontname, operation, subtype, details);
    $('select').material_select();
});


//evaluate canvas
function evaluate (id) {
    var operation = $("#" + id).data("operation");
    $("#" + id).data("results", null);


    var params = {};
    params.id = id;
    params.operation = operation;

    params.options = {};
    params.options.subtype = $("#" + id).data("subtype");
    params.options.hints = $("#" + id).data("subtype");

    params.options.nofallback = "true";

    params.options.indexonly = "true";

    params.options.informationlevel = $("#informationlevel").val();

    $("#" + id + " > .settings").find(":input").each(function () {
        if ($(this).val().length > 0) {
            params.options[$(this).attr('name')] = $(this).val();

            if($(this).attr('name') == "query" || $(this).attr('name') == "weights"){
                params.options[$(this).attr('name')] = $(this).val().replace("[", "").replace("]", "").trim();
            }
        }
    });

    var targets = getTargets(id);
    if (targets.length > 0) {
        params.targets = $.map(targets, function (val, i) {
            clearResults(val)
            return evaluate(val)
        });
    }

    if (operation === "start") {
        clearResults("box-start");
    }

    return params;
};

function getTargets(id) {
    var connections = instance.getConnections({target: id});
    var targets = $.map(connections, function (val, i) {
        return val.sourceId
    });
    return targets;
};

function clearResults(id) {
    $('#' + id + ' > div.res').html("");
};