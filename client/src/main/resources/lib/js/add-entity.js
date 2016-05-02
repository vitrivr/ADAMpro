//prepare operation
$("#btnSubmitCreateEntity").click(function () {
    if ($("#entityname").val().length === 0) {
        showAlert(" Please specify an entity.");
        return;
    }

    $("#btnSubmitCreateEntity").addClass('disabled');
    $("#btnSubmitCreateEntity").prop('disabled', true);

    $("#progress").show();

    var result = {};
    result.entityname = $("#entityname").val();
    result.fields = $.map(fields, function (value, index) {
        return [value];
    });


    $.ajax("/entity/add", {
        data: JSON.stringify(result),
        contentType: 'application/json',
        type: 'POST',
        success: function (data) {
            if (data.code === 200) {
                showAlert(data.entityname + " created");
            }
            $("#progress").hide()
            $("#btnSubmitCreateEntity").removeClass('disabled');
            $("#btnSubmitCreateEntity").prop('disabled', false);
        },
        error: function () {
            $("#progress").hide()
            $("#btnSubmitCreateEntity").removeClass('disabled');
            $("#btnSubmitCreateEntity").prop('disabled', false);
            showAlert("Error in request.");
            return;
        }
    });
});


$("#btnSubmitInsertData").click(function () {
    if ($("#entityname").val().length === 0) {
        showAlert(" Please specify an entity.");
        return;
    }

    $("#btnSubmitInsertData").addClass('disabled');
    $("#btnSubmitInsertData").prop('disabled', true);

    $("#progress").show();

    var result = {};
    result.entityname = $("#entityname").val();
    result.ntuples = $("#ntuples").val();
    result.ndims = $("#ndims").val();
    result.fields = $.map(fields, function (value, index) {
        return [value];
    });


    $.ajax("/entity/insertdemo", {
        data: JSON.stringify(result),
        contentType: 'application/json',
        type: 'POST',
        success: function (data) {
            if (data.code === 200) {
                showAlert("data inserted into " + data.entityname);
            }
            $("#progress").hide()
            $("#btnSubmitInsertData").removeClass('disabled');
            $("#btnSubmitInsertData").prop('disabled', false);
        },
        error: function () {
            $("#progress").hide()
            $("#btnSubmitInsertData").removeClass('disabled');
            $("#btnSubmitInsertData").prop('disabled', false);
            showAlert("Error in request.");
            return;
        }
    });
});

$("#btnSubmitCreateIndex").click(function () {
    if ($("#entityname").val().length === 0) {
        showAlert(" Please specify an entity.");
        return;
    }

    $("#btnSubmitCreateIndex").addClass('disabled');
    $("#btnSubmitCreateIndex").prop('disabled', true);

    $("#progress").show();

    var result = {};
    result.entityname = $("#entityname").val();


    $.ajax("/entity/indexall", {
        data: JSON.stringify(result),
        contentType: 'application/json',
        type: 'POST',
        success: function (data) {
            if (data.code === 200) {
                showAlert("indexes created");
            }
            $("#progress").hide()
            $("#btnSubmitCreateIndex").removeClass('disabled');
            $("#btnSubmitCreateIndex").prop('disabled', false);
        },
        error: function () {
            $("#progress").hide()
            $("#btnSubmitCreateIndex").removeClass('disabled');
            $("#btnSubmitCreateIndex").prop('disabled', false);
            showAlert("Error in request.");
            return;
        }
    });
});

var fieldId = 0;
var fields = new Object();

$("#btnAddField").click(function () {
    fieldId += 1;

    if ($("#fieldname").val().length === 0) {
        showAlert(" Please specify a name for the field.");
        return;
    }

    if (!$("#datatype").val() || $("#datatype").val().length === 0) {
        showAlert(" Please specify a datatype for the field.");
        return;
    }

    var field = {}
    field.name = $("#fieldname").val();
    field.datatype = $("#datatype").val();
    if ($('#indexed').is(':checked')) {
        field.indexed = true;
    } else {
        field.indexed = false;
    }

    $("#fields").append($('<option></option>').attr("value", fieldId).text(function () {
        var text = "";
        text += field.name;
        text += " (";
        text += field.datatype;

        if (field.indexed) {
            text += ", indexed";
        }

        text += ")";

        return text;
    }));

    fields[fieldId] = field;

    $("#fieldname").val("");
    $('#indexed').prop("checked", false);
});

$("#btnRemoveField").click(function () {
    var fieldIdToRemove = $("#fields").val();
    $("#fields option:selected").remove();
    delete fields[fieldIdToRemove];
});
