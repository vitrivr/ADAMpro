//alerts
var showAlert = function (message) {
    Materialize.toast(message, 4000);
};


function guid() {
    function s4() {
        return Math.floor((1 + Math.random()) * 0x10000)
            .toString(16)
            .substring(1);
    }

    return s4() + s4() + '-' + s4() + '-' + s4() + '-' +
        s4() + '-' + s4() + s4() + s4();
}


function getEntitySelect(id) {
    $.ajax("/entity/list", {
        data: "",
        contentType: 'application/json',
        type: 'GET',
        success: function (data) {
            if (data.code === 200) {
                var sel = $(' <select id="entityname" data-collapsible="accordion"><option value="" disabled selected>entity</option></select>');

                jQuery.each(data.entities, function (index, value) {
                    sel.append($('<option>', {value: value, text: value}));
                });

                $("#" + id).append(sel);
                $("#entityname").material_select();

            } else {
                showAlert("Error in request: " + data.message);
            }
        },
        error: function () {
            showAlert("Unspecified error in request.");
        }
    });
}