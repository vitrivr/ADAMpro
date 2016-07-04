var indextypes = [""].concat(getIndexTypes().reverse());

var chartUpdatingIntervalId = 0;
var dataPollIntervalId = 0;

$("#btnSubmit").click(function () {
    var entityname = $("#entityname").val();
    var attribute = $("#column").val();

    if (entityname === null || entityname.length == 0) {
        raiseError("Please specify an entity."); return;
    }

    if (attribute === null || attribute.length == 0) {
        raiseError("Please specify an attribute."); return;
    }

    var q = $("#query").val();
    var hints = $("#indextype").val();

    if ($("#k").val().length > 0) {
        var k = $("#k").val();
    } else {
        var k = 100;
    }
    
    var chart = setupChart("container");
    var series = chart.series[0];
    var startTime = (new Date()).getTime();
    
    searchProgressive(entityname, attribute, q, hints, k, function(){successHandler(series);}, function(data, intervallId){ dataPollIntervalId = intervallId; updateHandler(startTime, series, data); }, function(intervallId){dataPollIntervalId = intervallId; stopUpdating();});
});

function setupChart(id){
    Highcharts.setOptions({
        global: {
            useUTC: false
        }
    });

    var chart = $("#" + id).highcharts();
    if (chart) {
        chart.destroy();
    }

    $("#" + id).highcharts({
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

    return $("#" + id).highcharts();
}


function successHandler(series){series.addPoint({ x: (new Date()).getTime(), y: 0, source: "start", fillColor: "#f4511e", radius: 10}, true, true);};
        
function updateHandler(startTime, series, data){
    if (data.status === "running") {
        var x = (new Date()).getTime();
        series.addPoint({ x: x, y: indextypes.indexOf(data.results.sourcetype), source: data.results.source, sourcetype : data.results.sourcetype, confidence: data.results.confidence, time: Math.abs(startTime - x), results : data.results.results.length, fillColor: "#26a69a", radius: 10 }, true, true);
    } else if (data.status === "finished") {
        stopUpdating();
    }
};

function stopUpdating(){
    clearInterval(dataPollIntervalId);
    clearInterval(chartUpdatingIntervalId);
}