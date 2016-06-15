$(document).ready(function () {
    getEntitySelect("centityname");
});

//prepare operation
$("#btnSubmit").click(function () {
    if ($("#entityname").val().length === 0) {
        showAlert(" Please specify an entity.");
        return;
    }

    if ($("#column").val().length === 0) {
        showAlert(" Please specify an attribute.");
        return;
    }


    $("#btnSubmit").addClass('disabled');
    $("#btnSubmit").prop('disabled', true);

    $("#progress").show();

    var result = {};
    result.entityname = $("#entityname").val();
    result.column = $("#column").val();

    if($("#norm").val()){
        result.norm = $("#norm").val();
    } else {
        result.norm = $("#norm").attr("placeholder");
    }

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
                showAlert("index created");
            } else {
                showAlert("Error in request: " + data.message);
            }
            $("#progress").hide()
            $("#btnSubmit").removeClass('disabled');
            $("#btnSubmit").prop('disabled', false);
        },
        error : function() {
            $("#progress").hide()
            $("#btnSubmit").removeClass('disabled');
            $("#btnSubmit").prop('disabled', false);
            showAlert("Unspecified error in request.");
        }
    });
});


$("#indextype").change(function (e) {
    $(".indexinfo").hide();
    var indextype = $("#indextype").val();
    $("#" + indextype + "-info").fadeIn();
});