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
                    innerhtml += "<li id='" + value + "' data-entityname='" + value + "' class='collection-item avatar' '>";
                    innerhtml += "<i class='material-icons circle'>view_list</i>";
                    innerhtml += "<span class='title'>" + value + "</span>";
                    innerhtml += "<div class='details' style='display:none'>";
                    innerhtml += "<table class='striped'>";
                    innerhtml += "<thead><tr><th>option</th><th>value</th></tr></thead>";
                    innerhtml += "<tbody>";
                    innerhtml += "</tbody>";
                    innerhtml += "</table>";
                    innerhtml += "</div>";
                    innerhtml += "<div class='secondary-content'>";
                    innerhtml += "<a href='#!' class='btndelete'><i class='material-icons'>delete</i></a>";
                    innerhtml += "<a href='#!' class='btninfo'><i class='material-icons'>info</i></a>";
                    innerhtml += "</div>";
                    innerhtml += "</li>";
                    $("#info").append(innerhtml);

                    var handler = function () {
                        $('#' + value + ' > div.details').show();
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

                    $('#' + value + ' > div > a.btndelete').bind("click", function(){showAlert("Not implemented yet.")});
                    $('#' + value + ' > div > a.btninfo').bind("click", handler);

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