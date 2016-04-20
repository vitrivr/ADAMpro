$("#btnSubmit").click(function () {
    if ($("#indexname").val().length === 0) {
        showAlert(" Please specify an index.");
        return;
    }

    if ($("#partitions").val().length === 0) {
        showAlert(" Please specify the number of partitions.");
        return;
    }


    $("#btnSubmit").addClass('disabled');
    $("#btnSubmit").prop('disabled', true);

    $("#progress").show();

    var result = {};
    result.indexname = $("#indexname").val();
    result.partitions = $("#partitions").val();

    $.ajax("/index/repartition", {
        data: JSON.stringify(result),
        contentType: 'application/json',
        type: 'POST',
        success: function (data) {
            if (data.code === 200) {
                showAlert("index " + result.indexname + " repartitioned to " + data.indexname);
            }
            $("#progress").hide()
            $("#btnSubmit").removeClass('disabled');
            $("#btnSubmit").prop('disabled', false);
        }
    });
});
