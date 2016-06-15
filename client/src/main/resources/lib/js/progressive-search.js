$(document).ready(function () {
    getEntitySelect("centityname");
});

//submit operation
var indextypes = ['ecp', 'lsh', 'mi', 'pq', 'sh', 'vaf', 'vav', ''].reverse();

$("#btnSubmit").click(function () {
    if ($("#entityname").val().length === 0) {
        showAlert(" Please specify an entity.");
        return;
    } else if ($("#query").val().length === 0) {
        showAlert(" Please specify a query vector.");
        return;
    }

    $("#btnSubmit").addClass('disabled');
    $("#btnSubmit").prop('disabled', true);

    $("#progress").show();

    var id = guid();

    var query = {};
    query.id = id;
    query.entityname = $("#entityname").val();
    query.column = $("#column").val();
    query.query = $("#query").val();
    query.hints = $("#indextype").val();

    if ($("#k").val().length > 0) {
        query.k = $("#k").val();
    } else {
        query.k = 100;
    }

    Highcharts.setOptions({
        global: {
            useUTC: false
        }
    });

    var chartUpdatingIntervalId = 0;

    var chart = $('#container').highcharts();
    if (chart) {
        chart.destroy();
    }

    $('#container').highcharts({
        chart: {
            type: 'scatter',
            zoomType: 'xy',
            style: {
                fontFamily: 'Roboto',
                fontSize : "11px"
            },
            animation: Highcharts.svg, // don't animate in old IE
            marginRight: 10,
            events: {
                load: function () {
                    var series = this.series[0];
                    chartUpdatingIntervalId = setInterval(function () {
                        var x = (new Date()).getTime();
                        series.addPoint({x: x, y: 0, fillColor: 'transparent', enabled: false, radius: 0}, true, true);
                    }, 1000);
                }
            }
        },
        xAxis: {
            type: 'datetime',
            tickPixelInterval: 100
        },
        title: {
            text: null
        },
        yAxis: {
            title: {
                text: null
            },
            categories: indextypes,
            crosshair: true,
            min : 0,
            max : indextypes.length - 1
        },
        plotOptions: {
            line: {
                states: {
                    hover: {}
                }
            },
            series: {
                marker: {
                    states: {
                        hover: {
                            enabled: false,
                        }
                    }
                },
                stickyTracking: false,
                animation: {
                    duration: 400
                },
                states: {
                    hover: {
                        enabled : false
                    }
                }
            }
        },
        legend: {
            enabled: false
        },
        exporting: {
            enabled: false
        },
        tooltip: {
            backgroundColor: "white",
            borderColor : "#ababab",
            borderRadius: 8,
            shadow : false,
            style: {
                opacity: 0.8
            },
            formatter: function () {
                if (this.point.source && this.point.time) {
                    var html = this.point.source.toUpperCase() + '<br/>';
                    html += 'execution time: ' + this.point.time + 'ms' + '<br/>';
                    html += 'results: ' + this.point.results;
                    return html;
                } else if (this.point.source === 'start' || this.point.source == 'end') {
                    var html = this.point.source;
                    return html;
                } else {
                    return false;
                }
            }
        },
        series: [{
            name: 'Progressive Querying',
            data: (function () {
                // generate an array of random data
                var data = [],
                    time = (new Date()).getTime(),
                    i;

                for (i = -20; i <= 0; i += 1) {
                    var x = time + i * 1000;
                    data.push({x: x, y: 0, fillColor: 'transparent', enabled: false, radius: 0, enableMouseTracking: false});
                }
                return data;
            }())
        }]
    });

    var chart = $('#container').highcharts();

    var series = chart.series[0];

    var startTime = (new Date()).getTime();

    $.ajax("/query/progressive", {
        data: JSON.stringify(query),
        contentType: 'application/json',
        type: 'POST',
        success: function (data) {
            series.addPoint({
                x: (new Date()).getTime(),
                y: 0,
                source: "start",
                fillColor: "#f4511e",
                radius: 10
            }, true, true);

            var dataPollIntervalId = setInterval(function () {
                var status = {};
                status.id = id;

                $.ajax("/query/progressive/temp", {
                    data: JSON.stringify(status),
                    contentType: 'application/json',
                    type: 'POST',
                    success: function (data) {
                        if (data.status === "running") {
                            var x = (new Date()).getTime();
                            series.addPoint({
                                x: x,
                                y: indextypes.indexOf(data.results.sourcetype),
                                source: data.results.source,
                                sourcetype : data.results.sourcetype,
                                confidence: data.results.confidence,
                                time: Math.abs(startTime - x),
                                results : data.results.results.length,
                                fillColor: "#26a69a",
                                radius: 10
                            }, true, true);
                        } else if (data.status === "finished") {
                            /*series.addPoint({
                                x: (new Date()).getTime(),
                                y: 0,
                                source: "end",
                                fillColor: "#f4511e",
                                radius: 10
                            }, true, true);*/
                            clearInterval(dataPollIntervalId);
                            clearInterval(chartUpdatingIntervalId);
                            $("#progress").hide()
                            $("#btnSubmit").removeClass('disabled');
                            $("#btnSubmit").prop('disabled', false);
                        } else {
                            //no status?
                        }

                    }
                });
            }, 500);
        },
        error: function () {
            $("#progress").hide()
            $("#btnSubmit").removeClass('disabled');
            $("#btnSubmit").prop('disabled', false);
            showAlert("Error in request.");
            return;
        }
    });
});