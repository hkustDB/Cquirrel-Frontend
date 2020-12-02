var myChart = echarts.init(document.getElementById('result_chart'));

var option = {
    title: {
        text: 'AJU Result Chart'
    },
    tooltip:{},
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
// option.series.data.push([11,22]);
// myChart.setOption(option);

$(document).ready(function () {
    let socket = io.connect(location.protocol + '//' + document.domain + ':' + location.port);
    socket.on('result_figure_data', function (res) {
        var line_list = res.data;
        console.log('received: ' + (line_list).toString())
        option.series.data.push([line_list[0], line_list[2]]);
        myChart.setOption(option);
    });
});




