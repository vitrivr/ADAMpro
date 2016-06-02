var instance = jsPlumb.getInstance({
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
    Container: "canvas"
});

instance.registerConnectionType("basic", {anchor: "Continuous", connector: "StateMachine"});

window.jsp = instance;

var canvas = document.getElementById("canvas");
var windows = jsPlumb.getSelector(".statemachine-demo .w");


instance.bind("click", function (c) {
    instance.detach(c);
});


jsPlumb.fire("jsPlumbDemoLoaded", instance);

var initNode = function (el) {
    instance.draggable(el);
    instance.makeSource(el, {
        filter: ".ep",
        anchor: "Continuous",
        connectorStyle: {strokeStyle: "#26a69a", lineWidth: 2, outlineColor: "transparent", outlineWidth: 4},
        connectionType: "basic",
        extract: {
            "action": "the-action"
        }
    });
    instance.makeTarget(el, {
        dropOptions: {hoverClass: "dragHover"},
        anchor: "Continuous",
        allowLoopback: true
    });
};

var newNode = function (x, y, frontname, operation, subtype, fields) {
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

    document.getElementById("canvas").appendChild(d);
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


//init
for (var i = 0; i < windows.length; i++) {
    initNode(windows[i], true);
}

//add index
$("#btnAddIndex").click(function () {
    var option = $("#indextype option[value=" + $("#indextype").val() + "]");
    var frontname = option.text();
    var operation = option.data("operation");
    var subtype = option.data("subtype");
    var details = option.data("details");
    newNode(0, 0, frontname, operation, subtype, details);
});

//add operation
$("#btnAddOperation").click(function () {
    var option = $("#indexoperation option[value=" + $("#indexoperation").val() + "]");
    var frontname = option.text();
    var operation = option.data("operation");
    var subtype = option.data("subtype");
    var details = option.data("details");
    newNode(0, 0, frontname, operation, subtype, details);
});


//submit operation
$("#btnSubmit").click(function () {
    if ($("#entityname").val().length === 0) {
        showAlert(" Please specify an entity.");
        return;
    }

    if ($("#query").val().length === 0) {
        showAlert(" Please specify a query vector.");
        return;
    }

    if ($("#column").val().length === 0) {
        showAlert(" Please specify an attribute.");
        return;
    }

    $("#btnSubmit").addClass('disabled');
    $("#btnSubmit").prop('disabled', true);

    $("#progress").show();


    $.ajax("/query/compound", {
        data: JSON.stringify(evaluate("box-start")),
        contentType: 'application/json',
        type: 'POST',
        success: function (data) {
            if (data.code === 200) {
                $.each(data.details.intermediate_responses, function (idx, val) {
                    if (val.id.length > 0) {
                        $("#" + val.id + " > .res").html("execution time: " + val.time + "ms" + "<br/>" + "results: " + val.results.length);

                        var slicedData = val.results.slice(0, Math.min(150, val.results.length));
                        $("#" + val.id).data("results", slicedData);

                        $("#" + val.id).click(function () {
                            $("#resultbox").show();
                            var results = $("#" + val.id).data("results");

                            if (results.length > 0) {
                                var innerhtml = '';
                                innerhtml += '<table class="striped highlight">';
                                innerhtml += '<thead><tr>';

                                $.each(results[0], function (key, value) {
                                    innerhtml += "<th>" + key + "</th>"
                                });

                                innerhtml += '</tr></thead>';
                                innerhtml += '<tbody>';


                                $.each(results, function (key, value) {
                                    innerhtml += "<tr>";
                                    $.each(value, function (attributekey, attributevalue) {
                                        innerhtml += "<td>" + attributevalue + "</td>"
                                    });
                                    innerhtml += "</tr>";
                                });

                                innerhtml += '</tbody>';

                                innerhtml += '</table>';
                            }


                            $("#results").html(innerhtml);
                        });
                    }
                });
            } else {
                showAlert("Error in request: " + data.message);
            }
            $("#progress").hide()
            $("#btnSubmit").removeClass('disabled');
            $("#btnSubmit").prop('disabled', false);
        },
        error: function () {
            $("#progress").hide()
            $("#btnSubmit").removeClass('disabled');
            $("#btnSubmit").prop('disabled', false);
            showAlert("Error in request.");
        }
    });
});


//evaluate canvas
var evaluate = function (id) {
    var operation = $("#" + id).data("operation");
    $("#" + id).data("results", null);


    var result = {};
    result.id = id;
    result.operation = operation;

    result.options = {};
    result.options.subtype = $("#" + id).data("subtype");

    result.options.entityname = $("#entityname").val();
    result.options.column = $("#column").val();
    result.options.query = $("#query").val();

    result.options.informationlevel = $("#informationlevel").val();

    if ($("#k").val().length > 0) {
        result.options.k = $("#k").val();
    }
    $("#" + id + " > .settings").find(":input").each(function () {
        if ($(this).val().length > 0) {
            result.options[$(this).attr('name')] = $(this).val();
        }
    });

    var targets = getTargets(id);
    if (targets.length > 0) {
        result.targets = $.map(targets, function (val, i) {
            clearRes(val)
            return evaluate(val)
        });
    }

    if (operation === "start") {
        clearRes("box-start");
    }

    return result;
};

var getTargets = function (id) {
    var connections = instance.getConnections({target: id});
    var targets = $.map(connections, function (val, i) {
        return val.sourceId
    });
    return targets;
};

var clearRes = function (id) {
    $('#' + id + ' > div.res').html("");
};

