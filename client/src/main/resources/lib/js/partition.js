$("#btnSubmitIndex").click(function () {
    if ($("#indexname").val().length === 0) {
        showAlert(" Please specify an index.");
        return;
    }

    if ($("#indexpartitions").val().length === 0) {
        showAlert(" Please specify the number of partitions.");
        return;
    }


    $("#btnSubmitIndex").addClass('disabled');
    $("#btnSubmitIndex").prop('disabled', true);

    $("#progress").show();

    var result = {};
    result.entity = $("#indexname").val();
    result.partitions = $("#indexpartitions").val();
    result.materialize = $('#indexmaterialize').is(':checked');
    result.replace = $('#indexreplace').is(':checked');
    result.columns = $("#indexcolumns").val().split(",");


    $.ajax("/index/repartition", {
        data: JSON.stringify(result),
        contentType: 'application/json',
        type: 'POST',
        success: function (data) {
            if (data.code === 200) {
                showAlert("index repartitioned to " + data.message);
            } else {
                showAlert("Error in request: " + data.message);
            }
            $("#progress").hide()
            $("#btnSubmitIndex").removeClass('disabled');
            $("#btnSubmitIndex").prop('disabled', false);
        },
        error : function() {
            $("#progress").hide()
            $("#btnSubmitIndex").removeClass('disabled');
            $("#btnSubmitIndex").prop('disabled', false);
            showAlert("Unspecified error in request.");
        }
    });
});


$("#btnSubmitEntity").click(function () {
    if ($("#entityname").val().length === 0) {
        showAlert(" Please specify an entity.");
        return;
    }

    if ($("#entitypartitions").val().length === 0) {
        showAlert(" Please specify the number of partitions.");
        return;
    }


    $("#btnSubmitEntity").addClass('disabled');
    $("#btnSubmitEntity").prop('disabled', true);

    $("#progress").show();

    var result = {};
    result.entity = $("#entityname").val();
    result.partitions = $("#entitypartitions").val();
    result.materialize = true;
    result.replace = true;
    result.columns = $("#entitycolumns").val().split(",");

    $.ajax("/entity/repartition", {
        data: JSON.stringify(result),
        contentType: 'application/json',
        type: 'POST',
        success: function (data) {
            if (data.code === 200) {
                showAlert("index repartitioned to " + data.message);
            } else {
                showAlert("Error in request: " + data.message);
            }
            $("#progress").hide()
            $("#btnSubmitEntity").removeClass('disabled');
            $("#btnSubmitEntity").prop('disabled', false);
        },
        error : function() {
            $("#progress").hide()
            $("#btnSubmitEntity").removeClass('disabled');
            $("#btnSubmitEntity").prop('disabled', false);
            showAlert("Unspecified error in request.");
        }
    });
});

