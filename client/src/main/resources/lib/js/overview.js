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
                    innerhtml += "<li id='" + value + "' data-entityname='" + value + "'>";
                    innerhtml += "<div class='collapsible-header'>" + value + "</div>";
                    innerhtml += "<div class='collapsible-body'>";
                    innerhtml += "<table class='striped'>";
                    innerhtml += "<thead><tr><th>option</th><th>value</th></tr></thead>";
                    innerhtml += "<tbody>";
                    innerhtml += "</tbody>";
                    innerhtml += "</table>";
                    innerhtml += "</div>";
                    innerhtml += "</li>";
                    $("#info").append(innerhtml);

                    var handler = function () {
                        $("#progress").show();
                        $.ajax("/entity/details?entityname=" + value, {
                            data: "",
                            contentType: 'application/json',
                            type: 'GET',
                            success: function (data) {
                                if (data.code === 200) {
                                    var innerinnerhtml = '';
                                    jQuery.each(data.details, function (detailsindex, detailsvalue) {
                                        innerinnerhtml += "<tr>";
                                        innerinnerhtml += "<td>";
                                        innerinnerhtml += detailsindex;
                                        innerinnerhtml += "</td>";
                                        innerinnerhtml += "<td>";
                                        innerinnerhtml += detailsvalue;
                                        innerinnerhtml += "</td>";
                                        innerinnerhtml += "</tr>";
                                    });

                                    $('#' + value + ' > div > table > tbody').html(innerinnerhtml);
                                    $('#' + value + ' > div.collapsible-header').unbind("click", handler);
                                    $("#progress").hide()
                                } else {
                                    $("#progress").hide()
                                    showAlert("Error in request: " + data.message);
                                }
                            }
                        });
                    };


                    $('#' + value + ' > div.collapsible-header').bind("click", handler);

                    $('.collapsible').collapsible({
                        accordion: false
                    });
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