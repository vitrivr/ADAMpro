$(document).ready(function () {
    $("#progress").show();
    $.ajax("/entity/list", {
        data: "",
        contentType: 'application/json',
        type: 'GET',
        success: function (data) {
            if (data.code === 200) {
                jQuery.each(data.entities, function (index, value) {
                    $("#entityname").append($('<option>',
                        {
                            value: value,
                            text: value
                        }));
                });


                $(document).ready(function () {
                    $('select').material_select();
                });

            } else {
                showAlert("Error in request: " + data.message);
            }
            $("#progress").hide();
        }
        ,
        error: function () {
            $("#progress").hide()
            showAlert("Unspecified error in request.");
        }
    });
});


$("#btnSubmit").click(function () {
    if ($("#entityname").val() === null || $("#entityname").val().length === 0) {
        showAlert(" Please specify an entity.");
        return;
    }

    $("#info").html("");

    $("#progress").show();
    $.ajax("/entity/preview?entityname=" + $("#entityname").val(), {
        data: "",
        contentType: 'application/json',
        type: 'GET',
        success: function (data) {
            if (data.code === 200) {
                if (data.details.length < 1) {
                    return;
                }

                var innerhtml = '';
                innerhtml += '<table class="striped highlight">';
                innerhtml += '<thead><tr>';

                $.each( data.details[0], function( key, value ) {
                    innerhtml += "<th>" + key + "</th>"
                });

                innerhtml += '</tr></thead>';
                innerhtml += '<tbody>';


                $.each( data.details, function( key, value ) {
                    innerhtml += "<tr>";
                    $.each( value, function( attributekey, attributevalue ) {
                        innerhtml += "<td>" + attributevalue + "</td>"
                    });
                    innerhtml += "</tr>";
                });

                innerhtml += '</tbody>';

                innerhtml += '</table>';

                $("#info").append(innerhtml);

            } else {
                showAlert("Error in request: " + data.message);
            }
            $("#progress").hide();
        }
        ,
        error: function () {
            $("#progress").hide()
            showAlert("Unspecified error in request.");
        }
    });
});