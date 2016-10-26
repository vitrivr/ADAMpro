const divMsec = "sw-msec";

var msec = 0;

var interval;


/**
 *
 * @param parentid id of parent div
 */
function addStopwatch(parentid) {
    var sel = $('<p>total execution time: <span id="stopwatch-' + divMsec + '">0</span>&nbsp;msec</p>');
    $("#" + parentid).append(sel);
}

/**
 *
 */
function startStopwatch() {
    clearInterval(interval);
    msec = 0;

    displayMsec();

    interval = setInterval(updateStopwatch, 5);
}

/**
 *
 */
function updateStopwatch() {
    msec += 5;
    displayMsec();
}


/**
 *
 */
function stopStopwatch(){
    clearInterval(interval);
}

/**
 *
 */
function displayMsec(){
    $("#stopwatch-" + divMsec).html(msec);
}