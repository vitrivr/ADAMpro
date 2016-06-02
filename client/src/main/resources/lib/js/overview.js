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
                    innerhtml += "<li id='" + value + "' data-entityname='" + value + "' class='collection-item'" + ">";
                    innerhtml += "<span class='title' style='font-size: 16px;'>" + value + "</span>";
                    innerhtml += "<div class='secondary-content green-text'>";
                    innerhtml += "<a href='#!' class='btndelete'><i class='material-icons'>delete</i></a>";
                    innerhtml += "<a href='#!' class='btninfo'><i class='material-icons'>info</i></a>";
                    innerhtml += "</div>";
                    innerhtml += "<div class='details' style='display:none'>";
                    innerhtml += "<table>";
                    innerhtml += "<tbody>";
                    innerhtml += "</tbody>";
                    innerhtml += "</table>";
                    innerhtml += "</div>";
                    innerhtml += "</li>";
                    $("#info").append(innerhtml);

                    var dropHandler = function(){
                        $("#progress").show();
                        $.ajax("/entity/drop?entityname=" + value, {
                            data: "",
                            contentType: 'application/json',
                            type: 'GET',
                            success: function (data) {
                                if (data.code === 200) {
                                    $("#" + value).hide();
                                    showAlert("Entity dropped");
                                    $("#progress").hide();
                                } else {
                                    $("#progress").hide();
                                    showAlert("Error in request: " + data.message);
                                }
                            }
                        });
                    }


                    var infoHandler = function () {
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
                                        innerinnerhtml += "<td style='font-weight: bold; font-size: 12px; padding: 0px 0px;' width='150px'>";
                                        innerinnerhtml += detailsindex;
                                        innerinnerhtml += "</td>";
                                        innerinnerhtml += "<td style='font-size: 12px; padding: 0px 0px;'>";
                                        innerinnerhtml += detailsvalue;
                                        innerinnerhtml += "</td>";
                                        innerinnerhtml += "</tr>";
                                    });

                                    $('#' + value + ' > div > table > tbody').html(innerinnerhtml);
                                    $('#' + value + ' > div.collapsible-header').unbind("click", infoHandler);
                                    $("#progress").hide()
                                } else {
                                    $("#progress").hide()
                                    showAlert("Error in request: " + data.message);
                                }
                            }
                        });
                    };

                    $('#' + value + ' > div > a.btndelete').bind("click", dropHandler);
                    $('#' + value + ' > div > a.btninfo').bind("click", infoHandler);
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