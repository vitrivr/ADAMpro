/**
 *
 * @param message
 */
function showAlert(message) {
    Materialize.toast(message, 4000);
}

/**
 *
 * @param message
 */
function raiseError(message = "Unspecified error.") {
    showAlert("Error in request: " + message)
}

/**
 *
 */
function startTask() {
    $("#progress").show();
    $("#btnSubmit").addClass('disabled');
    $("#btnSubmit").prop('disabled', true);
}

/**
 *
 */
function stopTask() {
    $("#progress").hide();
    $("#btnSubmit").removeClass('disabled');
    $("#btnSubmit").prop('disabled', false);
}

/**
 *
 * @returns {string}
 */
function guid() {
    function s4() {
        return Math.floor((1 + Math.random()) * 0x10000).toString(16).substring(1);
    }

    return s4() + s4() + '-' + s4() + '-' + s4() + '-' + s4() + '-' + s4() + s4() + s4();
}

/**
 *
 * @param entityname name of entity
 * @param attributes list of attributes
 */
function entityCreate(entityname, attributes) {
    if (entityname === null || entityname.length == 0) {
        raiseError("Please specify an entity."); return;
    }

    if (attributes.length == 0) {
        raiseError("Please specify at least one attribute."); return;
    }

    startTask();

    var params = {};
    params.entityname = entityname;
    params.attributes = $.map(attributes, function (value, index) {
        return [value];
    });

    $.ajax(ADAM_CLIENT_HOST + "/entity/add", {
        data: JSON.stringify(params),
        contentType: 'application/json',
        type: 'POST',
        success: function (data) {
            if (data.code === 200) {
                showAlert("Entity " + entityname + " has been created.");
            } else {
                raiseError(data.message);
            }
            stopTask();
        },
        error: function () {
            raiseError();
            stopTask();
        }
    });
}

/**
 *
 * @param entityname name of entity
 * @param ntuples number of tuples
 * @param ndims number of dims for feature attributes
 */
function entityFillData(entityname, ntuples, ndims) {
    if (entityname === null || entityname.length == 0) {
        raiseError("Please specify an entity."); return;
    }

    startTask();

    var params = {};
    params.entityname = entityname;
    params.ntuples = ntuples;
    params.ndims = ndims;

    $.ajax(ADAM_CLIENT_HOST + "/entity/insertdemo", {
        data: JSON.stringify(params),
        contentType: 'application/json',
        type: 'POST',
        success: function (data) {
            if (data.code === 200) {
                showAlert("Inserted data into entity " + entityname + ".");
            } else {
                raiseError(data.message);
            }
            stopTask();
        },
        error: function () {
            raiseError();
            stopTask();
        }
    });
}

/**
 *
 * @param host
 * @param database
 * @param username
 * @param password
 */
function entityImportData(host, database, username, password) {
    startTask();

    var params = {};
    params.host = host;
    params.database = database;
    params.username = username;
    params.password = password;

    $.ajax(ADAM_CLIENT_HOST + "/import", {
        data: JSON.stringify(params),
        contentType: 'application/json',
        type: 'POST',
        success: function (data) {
            if (data.code === 200) {
                showAlert("Imported data from host " + host + ".");
            } else {
                raiseError(data.message);
            }
            stopTask();
        },
        error: function () {
            raiseError();
            stopTask();
        }
    });
}

/**
 *
 * @param handler
 */
function entityList(handler) {
    startTask();
    $.ajax(ADAM_CLIENT_HOST + "/entity/list", {
        data: "",
        contentType: 'application/json',
        type: 'GET',
        success: function (data) {
            if (data.code === 200) {
                handler(data);
            } else {
                raiseError(data.message);
            }
            stopTask();
        },
        error: function () {
            raiseError();
            stopTask();
        }
    });
}

/**
 *
 * @param parentid id of parent div
 */
function entityListGetSelect(parentid) {
    entityList(function (data) {
        var sel = $(' <select id="entityname" data-collapsible="accordion"><option value="" disabled selected>name of entity</option></select>');

        jQuery.each(data.entities, function (index, value) {
            sel.append($('<option>', {value: value, text: value}));
        });

        $("#" + parentid).append(sel);
        $("#entityname").material_select();
    });
}

/**
 *
 * @param entityname name of entity
 * @param handler to handle the incoming data
 */
function entityDetails(entityname, handler) {
    if (entityname === null || entityname.length == 0) {
        raiseError("Please specify an entity."); return;
    }

    startTask();
    $.ajax(ADAM_CLIENT_HOST + "/entity/details?entityname=" + entityname, {
        type: 'GET',
        success: function (data) {
            if (data.code === 200) {
                handler(data)
            } else {
                raiseError(data.message);
            }
            stopTask();
        },
        error: function () {
            raiseError();
            stopTask();
        }
    });
}

/**
 *
 * @param entityname
 * @param attributes
 * @param materialize
 * @param replace
 * @param npartitions
 */
function entityPartition(entityname, attributes, materialize, replace, npartitions) {
    if (entityname === null || entityname.length == 0) {
        raiseError("Please specify an entity."); return;
    }

    if (npartitions <= 0) {
        raiseError("Please specify a proper number of partitions."); return;
    }

    startTask();

    var params = {};
    params.entityname = entityname;
    params.attributes = attributes;
    params.materialize = materialize;
    params.replace = replace;
    params.npartitions = npartitions;

    $.ajax(ADAM_CLIENT_HOST + "/entity/partition", {
        data: JSON.stringify(params),
        contentType: 'application/json',
        type: 'POST',
        success: function (data) {
            if (data.code === 200) {
                showAlert("Partitioned entity " + entityname + ".");
            } else {
                raiseError(data.message);
            }
            stopTask();
        },
        error: function () {
            raiseError();
            stopTask();
        }
    });
}

/**
 *
 * @param entityname
 * @param handler to handle the incoming data
 */
function entityRead(entityname, handler) {
    if (entityname === null || entityname.length == 0) {
        raiseError("Please specify an entity."); return;
    }

    startTask();
    $.ajax(ADAM_CLIENT_HOST + "/entity/preview?entityname=" + entityname, {
        data: "",
        contentType: 'application/json',
        type: 'GET',
        success: function (data) {
            if (data.code === 200) {
                handler(data);
            } else {
                raiseError(data.message);
            }
            stopTask();
        },
        error: function () {
            raiseError();
            stopTask();
        }
    });
}

/**
 *
 * @param entityname
 */
function entityBenchmark(entityname, attribute) {
    if (entityname === null || entityname.length == 0) {
        raiseError("Please specify an entity."); return;
    }

    startTask();
    $.ajax(ADAM_CLIENT_HOST + "/entity/benchmark?entityname=" + entityname + "&attribute=" + attribute, {
        type: 'GET',
        success: function (data) {
            if (data.code === 200) {
                showAlert("Adjusted relevance weights for entity " + entityname + " and indexes.")
            } else {
                raiseError(data.message);
            }
            stopTask();
        },
        error: function () {
            raiseError();
            stopTask();
        }
    });
}

/**
 *
 * @param entityname
 */
function entitySparsify(entityname, attribute) {
    if (entityname === null || entityname.length == 0) {
        raiseError("Please specify an entity."); return;
    }

    startTask();
    $.ajax(ADAM_CLIENT_HOST + "/entity/sparsify?entityname=" + entityname + "&attribute=" + attribute, {
        type: 'GET',
        success: function (data) {
            if (data.code === 200) {
                showAlert("Sparsified entity " + entityname + ".")
            } else {
                raiseError(data.message);
            }
            stopTask();
        },
        error: function () {
            raiseError();
            stopTask();
        }
    });
}

/**
 *
 * @param entityname
 */
function entityDrop(entityname) {
    if (entityname === null || entityname.length == 0) {
        raiseError("Please specify an entity."); return;
    }

    startTask();
    $.ajax(ADAM_CLIENT_HOST + "/entity/drop?entityname=" + entityname, {
        type: 'GET',
        success: function (data) {
            if (data.code === 200) {
                showAlert("Entity " + entityname + " has been dropped.")
            } else {
                raiseError(data.message);
            }
            stopTask();
        },
        error: function () {
            raiseError();
            stopTask();
        }
    });
}

/**
 *
 * @returns {string[]}
 */
function getIndexTypes() {
    return ['ecp', 'lsh', 'mi', 'pq', 'sh', 'vaf', 'vav'];
}

/**
 *
 * @param entityname
 * @param attributes
 */
function indexCreateAll(entityname, attributes) {
    if (entityname === null || entityname.length == 0) {
        raiseError("Please specify an entity."); return;
    }

    startTask();

    var params = {};
    params.entityname = entityname;
    params.attributes = attributes;

    $.ajax(ADAM_CLIENT_HOST + "/entity/indexall", {
        data: JSON.stringify(params),
        contentType: 'application/json',
        type: 'POST',
        success: function (data) {
            if (data.code === 200) {
                showAlert("Created indexes for entity " + entityname + ".");
            } else {
                raiseError(data.message);
            }
            stopTask();
        },
        error: function () {
            raiseError();
            stopTask();
        }
    });
}

/**
 *
 * @param entityname
 * @param attribute
 * @param norm
 * @param indextype
 * @param options
 */
function indexCreate(entityname, attribute, norm, indextype, options) {
    if (entityname === null || entityname.length == 0) {
        raiseError("Please specify an entity."); return;
    }

    if (attribute === null || attribute.length == 0) {
        raiseError("Please specify an attribute."); return;
    }

    startTask();

    var params = {};
    params.entityname = entityname;
    params.attribute = attribute;
    params.norm = norm;
    params.indextype = indextype;
    params.options = options;

    $.ajax(ADAM_CLIENT_HOST + "/entity/index/add", {
        data: JSON.stringify(params),
        contentType: 'application/json',
        type: 'POST',
        success: function (data) {
            if (data.code === 200) {
                showAlert("Created index " + data.message + " for " + entityname + ".");
            } else {
                raiseError(data.message);
            }
            stopTask();
        },
        error: function () {
            raiseError();
            stopTask();
        }
    });
}

/**
 *
 * @param indexname
 * @param attributes
 * @param materialize
 * @param replace
 * @param npartitions
 */
function indexPartition(indexname, attributes, materialize, replace, npartitions) {
    if (indexname === null || indexname.length == 0) {
        raiseError("Please specify an index."); return;
    }

    if (npartitions <= 0) {
        raiseError("Please specify a proper number of partitions."); return;
    }

    startTask();

    var params = {};
    params.indexname = indexname;
    params.attributes = attributes;
    params.materialize = materialize;
    params.replace = replace;
    params.npartitions = npartitions;

    $.ajax(ADAM_CLIENT_HOST + "/entity/index/partition", {
        data: JSON.stringify(params),
        contentType: 'application/json',
        type: 'POST',
        success: function (data) {
            if (data.code === 200) {
                showAlert("Partitioned index " + indexname + " to " + data.message + ".");
            } else {
                raiseError(data.message);
            }
            stopTask();
        },
        error: function () {
            raiseError();
            stopTask();
        }
    });
}

/**
 *
 * @param params
 * @param handler
 */
function searchCompound(params, handler) {
    startTask();

    $.ajax(ADAM_CLIENT_HOST + "/search/compound", {
        data: JSON.stringify(params),
        contentType: 'application/json',
        type: 'POST',
        success: function (data) {
            if (data.code === 200) {
                handler(data);
            } else {
                raiseError(data.message);
            }
            stopTask();
        },
        error: function () {
            raiseError();
            stopTask();
        }
    });
}

/**
 *
 * @param params
 * @param successHandler
 * @param updateHandler
 * @param errorHandler
 */
function searchProgressive(id, params, successHandler, updateHandler, errorHandler) {
    startTask();

    $.ajax(ADAM_CLIENT_HOST + "/search/progressive", {
        data: JSON.stringify(params),
        contentType: 'application/json',
        type: 'POST',
        success: function (data) {
            successHandler(data);

            var dataPollIntervalId = setInterval(function () {
                $.ajax(ADAM_CLIENT_HOST + "/query/progressive/temp?id=" + id, {
                    contentType: 'application/json',
                    type: 'GET',
                    success: function (data) {
                        if (data.status === "finished") {
                            stopTask();
                        } else if (data.status === "error") {
                            errorHandler(dataPollIntervalId);
                            raiseError(data.results.source);
                            stopTask();
                        }
                        updateHandler(data, dataPollIntervalId);
                    },
                    error: function () {
                        errorHandler();
                        raiseError();
                        stopTask();
                    }
                });
            }, 500);
        },
        error: function () {
            errorHandler();
            raiseError();
            stopTask();
        }
    });
}



function storageHandlerList(handler){
    startTask();

    $.ajax(ADAM_CLIENT_HOST + "/storagehandlers/list", {
        contentType: 'application/json',
        type: 'GET',
        success: function (data) {
            if (data.code === 200) {
                handler(data);
            } else {
                raiseError(data.message);
            }
            stopTask();
        },
        error: function () {
            raiseError();
            stopTask();
        }
    });
}