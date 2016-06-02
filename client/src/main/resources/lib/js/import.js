$("#btnSubmit").click(function () {
    $("#progress").show();

    var result = {};
    result.host = $("#host").val();
    result.database = $("#database").val();
    result.username = $("#username").val();
    result.password = $("#password").val();

    $("#btnSubmit").addClass('disabled');
    $("#btnSubmit").prop('disabled', true);

    $.ajax("/import", {
        data: JSON.stringify(result),
        contentType: 'application/json',
        type: 'POST',
        success: function (data) {
            if (data.code === 200) {
                showAlert("Imported!");
            } else {
                showAlert("Error in request: " + data.message);
            }
            $("#progress").hide();
            $("#btnSubmit").removeClass('disabled');
            $("#btnSubmit").prop('disabled', false);
        },
        error: function () {
            $("#progress").hide()
            showAlert("Unspecified error in request.");
            $("#btnSubmit").removeClass('disabled');
            $("#btnSubmit").prop('disabled', false);
        }
    });
});