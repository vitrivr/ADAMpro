//prepare operation
$("#btnSubmit").click(function(){
    if($("#entityname").val().length === 0){
        showAlert(" Please specify an entity."); return;
    }

    $("#progress").show();

    var result = {};
    result.entityname = $("#entityname").val();
    result.ntuples = $("#ntuples").val();
    result.ndims = $("#ndims").val();

    $.ajax("/prepare", {
        data: JSON.stringify(result),
        contentType: 'application/json',
        type: 'POST',
        success: function(data){
            if(data.code === 200){
                showAlert(data.entityname + " with " + data.ntuples + " tuples of dim " + data.ndims + " created");
            }
            $("#progress").hide()
        }
    });
});