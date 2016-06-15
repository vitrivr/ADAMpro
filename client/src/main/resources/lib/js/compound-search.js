$(document).ready(function () {
    getEntitySelect("centityname");
});

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

    $('#results').html("");
    $('#resultbox').hide();

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

                        //tune here max results to display
                        var slicedData = val.results.slice(0, Math.min(500, val.results.length));
                        $("#" + val.id).data("results", slicedData);
                        $("#" + val.id).click(function(){
                            $("#resultbox").show();

                            var results = $("#" + val.id).data("results");
                            var tabid = "tab-" + val.id;

                            var innerhtml = '';

                            if (results != null && results.length > 0) {
                                innerhtml += '<table id="' + tabid + '" class="striped highlight">';
                                innerhtml += '<thead><tr>';

                                $.each(results[0], function (key, value) {
                                    innerhtml += "<th>" + key + "</th>"
                                });

                                innerhtml += '</tr></thead>';
                                innerhtml += '<tbody>';


                                $.each(results, function (key, value) {
                                    innerhtml += "<tr>";
                                    $.each(value, function (attributekey, attributevalue) {
                                        if(attributekey == "adamprodistance"){
                                            innerhtml += "<td class='dt-body-right'>" + attributevalue + "</td>"
                                        } else {
                                            innerhtml += "<td>" + attributevalue + "</td>"
                                        }
                                    });
                                    innerhtml += "</tr>";
                                });

                                innerhtml += '</tbody>';

                                innerhtml += '</table>';
                            }

                            $("#results").html(innerhtml);

                            $("#" + tabid).dataTable({
                                "oLanguage": {
                                    "sStripClasses": "",
                                    "sSearch": "",
                                    "sSearchPlaceholder": "Enter Keywords Here",
                                    "sInfo": "_START_ -_END_ of _TOTAL_",
                                    "sLengthMenu": '<span>Rows per page:</span><select class="browser-default">' +
                                    '<option value="10">10</option>' +
                                    '<option value="20">20</option>' +
                                    '<option value="30">30</option>' +
                                    '<option value="40">40</option>' +
                                    '<option value="50">50</option>' +
                                    '<option value="-1">All</option>' +
                                    '</select></div>'
                                },
                                bAutoWidth: false
                            });
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
    result.options.query = $("#query").val().replace("[", "").replace("]", "");

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

(function (window, document, undefined) {

    var factory = function ($, DataTable) {
        "use strict";

        $('.search-toggle').click(function () {
            if ($('.hiddensearch').css('display') == 'none')
                $('.hiddensearch').slideDown();
            else
                $('.hiddensearch').slideUp();
        });

        /* Set the defaults for DataTables initialisation */
        $.extend(true, DataTable.defaults, {
            dom: "<'hiddensearch'f'>" +
            "tr" +
            "<'table-footer'lip'>",
            renderer: 'material'
        });

        /* Default class modification */
        $.extend(DataTable.ext.classes, {
            sWrapper: "dataTables_wrapper",
            sFilterInput: "form-control input-sm",
            sLengthSelect: "form-control input-sm"
        });

        /* Bootstrap paging button renderer */
        DataTable.ext.renderer.pageButton.material = function (settings, host, idx, buttons, page, pages) {
            var api = new DataTable.Api(settings);
            var classes = settings.oClasses;
            var lang = settings.oLanguage.oPaginate;
            var btnDisplay, btnClass, counter = 0;

            var attach = function (container, buttons) {
                var i, ien, node, button;
                var clickHandler = function (e) {
                    e.preventDefault();
                    if (!$(e.currentTarget).hasClass('disabled')) {
                        api.page(e.data.action).draw(false);
                    }
                };

                for (i = 0, ien = buttons.length; i < ien; i++) {
                    button = buttons[i];

                    if ($.isArray(button)) {
                        attach(container, button);
                    } else {
                        btnDisplay = '';
                        btnClass = '';

                        switch (button) {

                            case 'first':
                                btnDisplay = lang.sFirst;
                                btnClass = button + (page > 0 ?
                                        '' : ' disabled');
                                break;

                            case 'previous':
                                btnDisplay = '<i class="material-icons">chevron_left</i>';
                                btnClass = button + (page > 0 ?
                                        '' : ' disabled');
                                break;

                            case 'next':
                                btnDisplay = '<i class="material-icons">chevron_right</i>';
                                btnClass = button + (page < pages - 1 ?
                                        '' : ' disabled');
                                break;

                            case 'last':
                                btnDisplay = lang.sLast;
                                btnClass = button + (page < pages - 1 ?
                                        '' : ' disabled');
                                break;

                        }

                        if (btnDisplay) {
                            node = $('<li>', {
                                'class': classes.sPageButton + ' ' + btnClass,
                                'id': idx === 0 && typeof button === 'string' ?
                                settings.sTableId + '_' + button : null
                            })
                                .append($('<a>', {
                                        'href': '#',
                                        'aria-controls': settings.sTableId,
                                        'data-dt-idx': counter,
                                        'tabindex': settings.iTabIndex
                                    })
                                    .html(btnDisplay)
                                )
                                .appendTo(container);

                            settings.oApi._fnBindAction(
                                node, {
                                    action: button
                                }, clickHandler
                            );

                            counter++;
                        }
                    }
                }
            };

            // IE9 throws an 'unknown error' if document.activeElement is used
            // inside an iframe or frame.
            var activeEl;

            try {
                // Because this approach is destroying and recreating the paging
                // elements, focus is lost on the select button which is bad for
                // accessibility. So we want to restore focus once the draw has
                // completed
                activeEl = $(document.activeElement).data('dt-idx');
            } catch (e) {
            }

            attach(
                $(host).empty().html('<ul class="material-pagination"/>').children('ul'),
                buttons
            );

            if (activeEl) {
                $(host).find('[data-dt-idx=' + activeEl + ']').focus();
            }
        };

        /*
         * TableTools Bootstrap compatibility
         * Required TableTools 2.1+
         */
        if (DataTable.TableTools) {
            // Set the classes that TableTools uses to something suitable for Bootstrap
            $.extend(true, DataTable.TableTools.classes, {
                "container": "DTTT btn-group",
                "buttons": {
                    "normal": "btn btn-default",
                    "disabled": "disabled"
                },
                "collection": {
                    "container": "DTTT_dropdown dropdown-menu",
                    "buttons": {
                        "normal": "",
                        "disabled": "disabled"
                    }
                },
                "print": {
                    "info": "DTTT_print_info"
                },
                "select": {
                    "row": "active"
                }
            });

            // Have the collection use a material compatible drop down
            $.extend(true, DataTable.TableTools.DEFAULTS.oTags, {
                "collection": {
                    "container": "ul",
                    "button": "li",
                    "liner": "a"
                }
            });
        }

    }; // /factory

    // Define as an AMD module if possible
    if (typeof define === 'function' && define.amd) {
        define(['jquery', 'datatables'], factory);
    } else if (typeof exports === 'object') {
        // Node/CommonJS
        factory(require('jquery'), require('datatables'));
    } else if (jQuery) {
        // Otherwise simply initialise as normal, stopping multiple evaluation
        factory(jQuery, jQuery.fn.dataTable);
    }

})(window, document);
