$(document).ready(function () {
    $("#progress").show();
    $.ajax("/entity/list", {
        data: "",
        contentType: 'application/json',
        type: 'GET',
        success: function (data) {
            if (data.code === 200) {
                jQuery.each(data.entities, function (index, value) {
                    var innerhtml = '';
                    innerhtml += "<li>";
                    innerhtml += "<div class='collapsible-header'>" + value.entityname + "</div>";
                    innerhtml += "<div class='collapsible-body'>";
                    innerhtml += "<table class='striped'>";
                    innerhtml += "<thead><tr><th>option</th><th>value</th></tr></thead>";
                    innerhtml += "<tbody>";
                    jQuery.each(value.details, function (detailsindex, detailsvalue) {
                        innerhtml += "<tr>";
                        innerhtml += "<td>";
                        innerhtml += detailsindex;
                        innerhtml += "</td>";
                        innerhtml += "<td>";
                        innerhtml += detailsvalue;
                        innerhtml += "</td>";
                        innerhtml += "</tr>";
                    });
                    innerhtml += "</tbody>";
                    innerhtml += "</table>";
                    innerhtml += "</div>";
                    innerhtml += "</li>";
                    $("#info").append(innerhtml);
                });

                $('.collapsible').collapsible({
                    accordion : false
                });
            }
        },
        error : function() {
            $("#progress").hide()
            showAlert("Error in request."); return;
        }
    });
    $("#progress").hide();
});