function isEmptyObject(obj) {
    for (var n in obj) {
        return false
    }
    return true;
}

function Queue() {

    var items = [];

    this.getItemAt = function (idx) {
        return items[idx];
    }

    this.enqueue = function (element) {
        items.push(element);
    }

    this.dequeue = function () {
        return items.shift();
    }

    this.front = function () {
        return items[0];
    }

    this.isEmpty = function () {
        return items.length === 0;
    }

    this.size = function () {
        return items.length;
    }

    this.print = function () {
        return items.toString();
    }
}


var myChart = echarts.init(document.getElementById('result_chart'));

var option = {
    title: {
        text: 'AJU Result Chart'
    },
    tooltip: {},
    legend: {
        type: 'scroll',
        orient: 'vertical',
        right: '0%',
        top: '10%',
        bottom: '10%',
        textStyle: {
            fontSize: 6
        },
        data: [],
        selected: {}
    },
    grid: {
        show:true,
        width:"63%"
    },
    dataZoom: {
        type: "slider",
        show: true,
        showDetail: true,
        realtime: true
    },
    xAxis: {
        type: 'category',
        // min: 'dataMin',
        // max: 'dataMax',
        name: 'timestamp',
        data: []
    },
    yAxis: {
        type: 'value',
        min: 'dataMin',
        max: 'dataMax',
        name: 'revenue'
    },
    series: []
};
// option.series.data.push([11,22]);
// myChart.setOption(option);

$(document).ready(function () {
    // let socket = io.connect(location.protocol + '//' + document.domain + ':' + location.port);
    const socket = io(location.protocol + '//' + document.domain + ':' + location.port);

    var local_data = [];
    var x_timestamp = [];
    var legend_data = [];
    var selected_data = {};
    var selected_queue = new Queue();
    var q6_serie ={name:"revenue", type:"line", data: []}

    socket.on('connect', () => {
        console.log('socketio connected.')
    });

    socket.on('result_figure_data', function (res) {
        $('#start_to_run_flink').text('Receiving flink results data...')
        var line_list = res.data;
        // console.log('received: ' + (line_list).toString());

        x_timestamp.push(line_list[8]);
        if (line_list.length % 2 === 0) {
            alert('received data format is not correct.');
        }
        // q6
        if (line_list.length === 3) {
            option.title.text = "AJU Result Chart - TPC-H Query 6";
            q6_serie.data.push(line_list[0]);
            option.xAxis.data.push(line_list[2]);
            option.series[0] = q6_serie;
        }
        // q3
        if (line_list.length === 9) {
            option.title.text = "AJU Result Chart - TPC-H Query 3";
            let key_tag = line_list[4] + ":" + line_list[0] + ", "
                + line_list[5] + ":" + line_list[1] + ", "
                + line_list[6] + ":" + line_list[2];
            if (local_data[key_tag] === undefined) {
                if (local_data.length !== 0) {
                    for (var i in local_data) {
                        local_data[i].push(null);
                    }
                }

                local_data[key_tag] = [];
                for (var i = 0; i < x_timestamp.length; i++) {
                    local_data[key_tag].push(null);
                }
                local_data[key_tag].push(line_list[3]);
                legend_data.push(key_tag);

                if (selected_queue.size() > 6) {
                    selected_data[selected_queue.front()] = false;
                    selected_queue.dequeue();
                }
                selected_queue.enqueue(key_tag);
                for (var i = 0; i < selected_queue.size(); i++) {
                    selected_data[selected_queue.getItemAt(i)] = true;
                }

            } else {
                for (var i in local_data) {
                    local_data[i].push(null);
                }
                local_data[key_tag].pop();
                local_data[key_tag].push(line_list[3]);
            }
            x_timestamp.push(line_list[8]);
            option.xAxis.data = x_timestamp;
            let aserie = {name: key_tag, type: "line", data: local_data[key_tag]};
            option.series.push(aserie);
            option.legend.data = legend_data;
            option.legend.selected = selected_data;
        }
        myChart.setOption(option);
    });

    socket.on('start_to_run_flink_job', function (res) {
        console.log('start_to_run_flink_job' + res.data.toString())
        if (res.data === 1) {
            alert('start to run flink job... please wait ~');
            $('#start_to_run_flink').text('Start to run flink job... please wait ~')

            option.xAxis.data = [];
            option.series = [];
            option.legend.data.selected = {};
            local_data = [];
            x_timestamp = [];
            legend_data = [];
            selected_data = {};
            selected_queue.clear();
            myChart.setOption(option);
        }
    });


});



