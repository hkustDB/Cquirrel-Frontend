var myChart = echarts.init(document.getElementById('result_chart'));

var option = {
    title: {
        text: 'AJU Result Chart'
    },
    xAxis: {
        type: 'value',
        min: 'dataMin',
        max: 'dataMax',
        name: 'timestamp'
    },
    yAxis: {
        type: 'value',
        min: 'dataMin',
        max: 'dataMax',
        name: 'aggregate'
    },
    series: {
        type: 'line',
        data: []
    }

};

$(document).ready(function () {
    let socket = io.connect(location.protocol + '//' + document.domain + ':' + location.port);
    socket.on('result_figure_data', function (res) {
        var line_list = res.data;
        option.series.data.push([line_list[0], line_list[2]]);
        myChart.setOption(option);
    });
});




