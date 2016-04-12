//prepare operation
$("#btnSubmit").click(function () {
    if ($("#entityname").val().length === 0) {
        showAlert(" Please specify an entity.");
        return;
    }


    $("#btnSubmit").addClass('disabled');
    $("#btnSubmit").prop('disabled', true);

    $("#progress").show();

    var result = {};
    result.entityname = $("#entityname").val();
    result.norm = $("#norm").val();
    result.indextype = $("#indextype").val();
    result.options = {};

    $("#" + result.indextype + "-info :input").each(function () {
        if ($(this).val() != null && $(this).val().length > 0) {
            result.options[this.name] = $(this).val();
        }
    })

    $.ajax("/index/add", {
        data: JSON.stringify(result),
        contentType: 'application/json',
        type: 'POST',
        success: function (data) {
            if (data.code === 200) {
                showAlert("index for " + data.entityname + " created");
            }
            $("#progress").hide()
            $("#btnSubmit").removeClass('disabled');
            $("#btnSubmit").prop('disabled', false);
        }
    });
});


$("#indextype").change(function (e) {
    $(".indexinfo").hide();
    var indextype = $("#indextype").val();
    $("#" + indextype + "-info").fadeIn();
});