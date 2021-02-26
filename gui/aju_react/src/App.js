import React, {Component} from 'react';
import {io} from 'socket.io-client';
import InstructSteps from './components/InstructSteps'
import SqlEntry from './components/SqlEntry';
import JsonFileUploader from './components/JsonFileUploader';
import CodegenResult from "./components/CodegenResult";
import FlowDiag from "./components/FlowDiag";
import QueryTable from "./components/QueryTable";
// import QueryFig from "./components/QueryFig"
import Settings from "./components/Settings";
import About from "./components/About";
import {Layout, Menu, Row, Col, message, Card, Button, InputNumber} from 'antd';
import {PlayCircleOutlined, PauseCircleOutlined} from '@ant-design/icons';
import './App.css';

import echarts from "echarts/lib/echarts";
import 'echarts/lib/chart/line';
import 'echarts/lib/chart/bar';
import 'echarts/lib/component/tooltip';
import 'echarts/lib/component/title';
import 'echarts/lib/component/legend';
import 'echarts/lib/component/legendScroll';
import 'echarts/lib/component/grid';
import 'echarts/lib/component/dataZoom';
import {consoleLog} from "echarts/lib/util/log";


class App extends Component {

    constructor(props) {
        super(props);
        // var myChart = {};
        this.state = {
            socket: null,
            connected: false,
            codegen_log: "",
            cur_step: 0,
            table_cols: [],
            table_data: [],
            table_title: "Query Result Table: ",
            topN_input_disabled: false,

            chart_option: {
                animation: false,
                title: {
                    text: 'AJU Result Chart'
                },
                tooltip: {},
                legend: {
                    type: 'scroll',
                    orient: 'vertical',
                    // left: '4%',
                    // right: '0%',
                    top: '78%',
                    bottom: '0%',
                    textStyle: {
                        fontSize: 10
                    },
                    data: []
                },
                grid: {
                    left: "5%",
                    bottom: "32%",
                    // x: "1%",
                    // x2: "30%",
                    show: true
                },
                dataZoom: {
                    type: "slider",
                    show: true,
                    showDetail: true,
                    realtime: true,
                    bottom: '23%'
                },
                xAxis: {
                    type: 'category',
                    name: 'timestamp',
                    data: []
                },
                yAxis: {
                    type: 'value',
                    axisLabel: {
                        inside: true
                    },
                    name: 'revenue'
                },
                series: []
            },
            chart_option1: {
                title: {text: 'ECharts 入门示例'},
                tooltip: {},
                xAxis: {
                    data: ["衬衫", "羊毛衫", "雪纺衫", "裤子", "高跟鞋", "袜子"]
                },
                yAxis: {},
                series: [{
                    name: '销量',
                    type: 'bar',
                    data: [5, 20, 36, 10, 10, 20]
                }]
            },
        };
    }

    componentDidMount() {
        this.build();
        window.addEventListener('beforeunload', this.beforeUnload);

        // this.setState({myChart: echarts.init(document.getElementById('queryChart')) })
        // myChart = echarts.init(document.getElementById('queryChart'));
        // this.state.myChart.setOption(this.state.chart_option);
    }

    componentWillUnmount() {
        window.removeEventListener('beforeunload', this.beforeUnload);
        this.disconnect();
    }

    beforeUnload = e => {
        this.disconnect();
    };

    build = () => {
        // init echarts
        var myChart = echarts.init(document.getElementById('queryChart'));
        myChart.setOption(this.state.chart_option);

        // var local_data = [];
        var x_timestamp = [];
        var legend_data = [];
        // var selected_data = {};
        // var selected_queue = new Queue();
        var q6_serie = {name: "revenue", type: "line", data: []};

        // init websocket
        const _socket = io('http://localhost:5000/ws', {
            transports: ['websocket']
        });
        // const _socket = io('http://localhost:5000/ws', {
        //     transports: ['polling']
        // });
        _socket.on('connect', () => {
            console.log("socket connect.");
            this.setState({connected: true});
        });
        _socket.on('disconnect', () => {
            console.log("socket disconnect.")
            this.setState({connected: false});
        });
        _socket.on('r_codegen_log', data => {
            this.setState({codegen_log: data.codegen_log}, () => {
                console.log("r_codegen_log: ", this.state.codegen_log)
            });
            if (data.retcode === 0) {
                this.setState({cur_step: 3}, () => {
                    console.log("cur_step: ", this.state.cur_step)
                });
            } else {
                this.setState({cur_step: 1}, () => {
                    console.log("cur_step: ", this.state.cur_step)
                });
            }
        });

        _socket.on('r_set_step', data => {
            console.log("received r_set_step signal: ", data)
            this.setState({cur_step: data.step}, () => {
                console.log("r_set_step:", this.state.cur_step)
            });
        });

        _socket.on('r_start_to_send_data', data => {

            if (data.status === "start") {
                // local_data = [];
                x_timestamp = [];
                legend_data = [];
                q6_serie = {name: "revenue", type: "line", data: []};

                this.setState({
                    table_cols: [],
                    table_data: [],
                })
            }

        });

        _socket.on('r_figure_data', res => {

            this.setState({topN_input_disabled: true})
            // this.setState({topN_input_disabled: true}, () => {
            //     console.log("topN_input_disabled: ", this.state.topN_input_disabled)
            // })

            // console.log("r_figure_data: ", res)
            if (res.isTopN === 0) {
                // refresh table
                let t_cols = [
                    {
                        title: 'timestamp',
                        dataIndex: 'timestamp',
                    },
                    {
                        title: 'revenue',
                        dataIndex: 'revenue',
                    },
                ]
                // let t_data = [...this.state.table_data];
                let t_data = [{
                    key: (this.state.table_data.length + 1).toString(),
                    timestamp: res.data[2],
                    revenue: res.data[0],
                }, ...this.state.table_data];
                let t_title = "TPC-H Query Result Table: "

                this.setState({table_cols: t_cols, table_data: t_data, table_title: t_title});

                // refresh chart
                let line_list = res.data
                let c_option = {...this.state.chart_option};
                c_option.title.text = "AJU Result Chart - TPC-H Query";
                q6_serie.data.push(line_list[0]);
                c_option.xAxis.data.push(line_list[2]);
                c_option.series[0] = q6_serie;
                let q6_total_length = c_option.xAxis.data.length;
                c_option.dataZoom.startValue = ((q6_total_length - 100) > 0) ? q6_total_length - 100 : 1;
                myChart.setOption(c_option);
                this.setState({chart_option: c_option});
            } else if (res.isTopN === 1) {
                let top_value_data = res.top_value_data;
                console.log(res);
                let line_list = res.data
                let line_list_len = line_list.length;
                let attribute_length = ((line_list_len - 1) / 2);
                // refresh table

                // construct column name
                let t_cols = [
                    {
                        title: 'timestamp',
                        dataIndex: 'timestamp',
                    },
                ]
                for (var i = 0; i < attribute_length; i++) {
                    let tmp_col = {
                        title: line_list[attribute_length + i],
                        dataIndex: line_list[attribute_length + i],
                    }
                    t_cols.push(tmp_col);
                }

                // construct table data index
                let tmp_t_data = {
                    key: (this.state.table_data.length + 1).toString(),
                }
                for (var i = 0; i < attribute_length; i++) {
                    tmp_t_data[line_list[attribute_length + i]] = line_list[i];
                    tmp_t_data["timestamp"] = line_list[line_list_len - 1];
                }
                let t_data = [tmp_t_data, ...this.state.table_data];
                let t_title = "TPC-H Query Result Table: "

                this.setState({table_cols: t_cols, table_data: t_data, table_title: t_title});

                // refresh chart
                // let line_list = res.data
                let c_option = {...this.state.chart_option};
                x_timestamp = res.x_timestamp;


                c_option.title.text = "AJU Result Chart - TPC-H Query  -- Top " + Object.keys(top_value_data).length;
                legend_data = []
                var series_data = []

                for (var key_tag in top_value_data) {
                    legend_data.push(key_tag);
                    let aserie = {name: key_tag, type: "line", data: top_value_data[key_tag]};
                    series_data.push(aserie);
                }
                // option.series.push(aserie);
                c_option.series = series_data;
                c_option.legend.data = legend_data;
                c_option.xAxis.data = x_timestamp;
                c_option.dataZoom.startValue = ((x_timestamp.length - 100) > 0) ? x_timestamp.length - 100 : 1;
                myChart.setOption(c_option);
                this.setState({chart_option: c_option});
            }
                // else if (res.queryNum === 3) {
                //     let top_value_data = res.top_value_data;
                //     console.log(res);
                //     // refresh table
                //     let t_cols = [
                //         {
                //             title: 'timestamp',
                //             dataIndex: 'timestamp',
                //         },
                //         {
                //             title: 'orderkey',
                //             dataIndex: 'orderkey',
                //         },
                //         {
                //             title: 'orderdate',
                //             dataIndex: 'orderdate',
                //         },
                //         {
                //             title: 'shippriority',
                //             dataIndex: 'shippriority',
                //         },
                //         {
                //             title: 'revenue',
                //             dataIndex: 'revenue',
                //         },
                //     ]
                //     // let t_data = [...this.state.table_data];
                //     let t_data = [{
                //         key: (this.state.table_data.length + 1).toString(),
                //         timestamp: res.data[8],
                //         orderkey: res.data[0],
                //         orderdate: res.data[1],
                //         shippriority: res.data[2],
                //         revenue: res.data[3],
                //     }, ...this.state.table_data];
                //     let t_title = "TPC-H Query " + res.queryNum + " Result Table: "
                //
                //     this.setState({table_cols: t_cols, table_data: t_data, table_title: t_title});
                //
                //     // refresh chart
                //     // let line_list = res.data
                //     let c_option = {...this.state.chart_option};
                //     x_timestamp = res.x_timestamp;
                //
                //
                //     c_option.title.text = "AJU Result Chart - TPC-H Query " + res.queryNum + "  -- Top " + Object.keys(top_value_data).length;
                //     legend_data = []
                //     var series_data = []
                //
                //     for (var key_tag in top_value_data) {
                //         legend_data.push(key_tag);
                //         let aserie = {name: key_tag, type: "line", data: top_value_data[key_tag]};
                //         series_data.push(aserie);
                //     }
                //     // option.series.push(aserie);
                //     c_option.series = series_data;
                //     c_option.legend.data = legend_data;
                //     c_option.xAxis.data = x_timestamp;
                //     c_option.dataZoom.startValue = ((x_timestamp.length - 100) > 0) ? x_timestamp.length - 100 : 1;
                //     myChart.setOption(c_option);
                //     this.setState({chart_option: c_option});
                // }
                // else if (res.queryNum === 10) {
                //     console.log(res)
                //
                //     // refresh table
                //     let t_cols = [
                //         {
                //             title: 'timestamp',
                //             dataIndex: 'timestamp',
                //         },
                //         {
                //             title: 'custkey',
                //             dataIndex: 'custkey',
                //         },
                //         {
                //             title: 'c_name',
                //             dataIndex: 'c_name',
                //         },
                //         {
                //             title: 'c_acctbal',
                //             dataIndex: 'c_acctbal',
                //         },
                //         {
                //             title: 'c_phone',
                //             dataIndex: 'c_phone',
                //         },
                //         // {
                //         //     title: 'c_address',
                //         //     dataIndex: 'c_address',
                //         // },
                //         // {
                //         //     title: 'c_comment',
                //         //     dataIndex: 'c_comment',
                //         // },
                //         {
                //             title: 'n_name',
                //             dataIndex: 'n_name',
                //         },
                //         {
                //             title: 'revenue',
                //             dataIndex: 'revenue',
                //         },
                //     ]
                //     // let t_data = [...this.state.table_data];
                //     let t_data = [{
                //         key: (this.state.table_data.length + 1).toString(),
                //
                //         custkey: res.data[0],
                //         c_name: res.data[1],
                //         c_acctbal: res.data[2],
                //         c_phone: res.data[3],
                //         // c_address: res.data[4],
                //         // c_comment: res.data[5],
                //         n_name: res.data[4],
                //         // n_name: res.data[6],
                //         revenue: res.data[5],
                //         // revenue: res.data[7],
                //         timestamp: res.data[12],
                //     }, ...this.state.table_data];
                //     let t_title = "TPC-H Query " + res.queryNum + " Result Table: "
                //
                //     this.setState({table_cols: t_cols, table_data: t_data, table_title: t_title});
                //
                //     // refresh chart
                //     // let line_list = res.data
                //     let top_value_data = res.top_value_data;
                //     let c_option = {...this.state.chart_option};
                //     x_timestamp = res.x_timestamp;
                //
                //
                //     c_option.title.text = "AJU Result Chart - TPC-H Query " + res.queryNum + "  -- Top " + Object.keys(top_value_data).length;
                //     legend_data = []
                //     var series_data = []
                //
                //     for (var key_tag in top_value_data) {
                //         legend_data.push(key_tag);
                //         let aserie = {name: key_tag, type: "line", data: top_value_data[key_tag]};
                //         series_data.push(aserie);
                //     }
                //     // option.series.push(aserie);
                //     c_option.series = series_data;
                //     c_option.legend.data = legend_data;
                //     c_option.xAxis.data = x_timestamp;
                //     c_option.dataZoom.startValue = ((x_timestamp.length - 100) > 0) ? x_timestamp.length - 100 : 1;
                //     myChart.setOption(c_option);
                //     this.setState({chart_option: c_option});
                // }
                // else if (res.queryNum === 18) {
                //     console.log(res)
                //     // refresh table
                //     let t_cols = [
                //         {
                //             title: 'timestamp',
                //             dataIndex: 'timestamp',
                //         },
                //         {
                //             title: 'custkey',
                //             dataIndex: 'custkey',
                //         },
                //         {
                //             title: 'c_name',
                //             dataIndex: 'c_name',
                //         },
                //         {
                //             title: 'orderkey',
                //             dataIndex: 'orderkey',
                //         },
                //         {
                //             title: 'o_orderdate',
                //             dataIndex: 'o_orderdate',
                //         },
                //         {
                //             title: 'o_totalprice',
                //             dataIndex: 'o_totalprice',
                //         },
                //         {
                //             title: 'aggregate',
                //             dataIndex: 'aggregate',
                //         },
                //     ]
                //     // let t_data = [...this.state.table_data];
                //     let t_data = [{
                //         key: (this.state.table_data.length + 1).toString(),
                //
                //         custkey: res.data[1],
                //         c_name: res.data[0],
                //         orderkey: res.data[2],
                //         o_orderdate: res.data[3],
                //         o_totalprice: res.data[4],
                //         aggregate: res.data[5],
                //         timestamp: res.data[12],
                //     }, ...this.state.table_data];
                //     let t_title = "TPC-H Query " + res.queryNum + " Result Table: "
                //
                //     this.setState({table_cols: t_cols, table_data: t_data, table_title: t_title});
                //
                //
                //     // refresh chart
                //     // let line_list = res.data
                //     let top_value_data = res.top_value_data;
                //     let c_option = {...this.state.chart_option};
                //     x_timestamp = res.x_timestamp;
                //
                //
                //     // c_option.title.text = "AJU Result Chart - TPC-H Query " + res.queryNum + "  -- Top " + Object.keys(top_value_data).length;
                //     c_option.title.text = "AJU Result Chart - TPC-H Query " + res.queryNum;
                //     legend_data = []
                //     var series_data = []
                //
                //     for (var key_tag in top_value_data) {
                //         legend_data.push(key_tag);
                //         let aserie = {name: key_tag, type: "line", data: top_value_data[key_tag]};
                //         series_data.push(aserie);
                //     }
                //     // option.series.push(aserie);
                //     c_option.series = series_data;
                //     c_option.legend.data = legend_data;
                //     c_option.xAxis.data = x_timestamp;
                //     c_option.yAxis.name = "aggregate";
                //     c_option.dataZoom.startValue = ((x_timestamp.length - 100) > 0) ? x_timestamp.length - 100 : 1;
                //     myChart.setOption(c_option);
                //     this.setState({chart_option: c_option});
            // }
            else {
                console.log('query number is not supported yet.')
            }
        });

        _socket.on("r_message", data => {
            if (data.m_type === "info") {
                message.info(data.message);
            } else if (data.m_type === "success") {
                message.success(data.message);
            } else if (data.m_type === "error") {
                message.error(data.message);
            } else if (data.m_type === "warning") {
                message.warning(data.message);
            } else {
                console.log("unknow message type", data)
            }
        });

        // _socket.onAny((eventName, data) => {
        //     console.log("listen on any: ", eventName)
        //     console.log(data)
        // });

        this.setState({socket: _socket}, () => {
            let {socket, connected} = this.state;
            if (!connected) {
                socket.connect();
            }
        });
    };

    disconnect = () => {
        try {
            let {socket, connected} = this.state;
            if (null != socket && connected) {
                socket.disconnect();
            }
        } catch (error) {
            console.log(error);
        }
    };

    send = () => {
        try {
            let {socket, connected} = this.state;
            if (null != socket && connected) {
                // send message to server
                socket.emit('topic', '新消息来啦~');
            }
        } catch (error) {
            console.log(error);
        }
    };

    stopServerSendDataThread = () => {
        let _socket = this.state.socket;
        _socket.emit("r_stop_send_data_thread", {"status": "stop"})
    }

    pauseServerSendData = () => {
        let _socket = this.state.socket;
        _socket.emit("r_send_data_control", {"command": "pause"})
        this.setState({topN_input_disabled: false})
        console.log("pause server send data")
    }

    restartServerSendData = () => {
        let _socket = this.state.socket;
        _socket.emit("r_send_data_control", {"command": "restart"})
        this.setState({topN_input_disabled: true})
        console.log("restart server send data")
    }

    set_topN_value = () => {

    }


    render() {
        const {Header, Content, Footer} = Layout;
        return (
            <div className="App">
                <Layout>
                    <Header style={{position: 'fixed', zIndex: 1, width: '100%'}}>
                        <div className="logo"/>
                        <Menu theme="dark" mode="horizontal" defaultSelectedKeys={['1']}>
                            <Menu.Item key="1"><a href="/" onClick={this.stopServerSendDataThread.bind(this)}>Acyclic
                                Join under Updates Demo</a> </Menu.Item>
                            <Menu.Item key="2"><Settings/></Menu.Item>
                            <Menu.Item key="3"><About/></Menu.Item>
                        </Menu>
                    </Header>
                    <Content className="site-layout" style={{padding: '0 50px', marginTop: 64}}>
                        <div className="site-layout-background" style={{padding: 24, minHeight: 640}}>
                            <Row>
                                <InstructSteps cur_step={this.state.cur_step}/>
                            </Row>
                            <hr/>
                            <Row>
                                <Col span={12}><SqlEntry/> </Col>
                                <Col span={12}><span>Or you can upload the json file:</span> <JsonFileUploader/> </Col>
                            </Row>
                            <hr/>
                            <Row>
                                <Col span={12}> <CodegenResult logContent={this.state.codegen_log}/> </Col>
                                {/*<Col span={12}> <CodegenResult/> </Col>*/}
                                <Col span={12}> <FlowDiag/> </Col>

                            </Row>

                            <hr/>
                            <Row>
                                <Col span={12}>
                                    <QueryTable table_cols={this.state.table_cols} table_data={this.state.table_data}
                                                table_title={this.state.table_title}/>
                                </Col>
                                <Col span={12}>
                                    {/*<QueryFig/>*/}
                                    <div className="queryfig-card">
                                        <Card title="Query Result Figure: " extra={
                                            <div>
                                                <span>TopN:</span>
                                                <InputNumber defaultValue={10} size="small"
                                                             disabled={this.state.topN_input_disabled}
                                                             onPressEnter={this.set_topN_value.bind(this)}/>
                                                &nbsp;&nbsp;
                                                <Button type="primary" size="small" icon={<PlayCircleOutlined/>}
                                                        title="start to send data"
                                                        onClick={this.restartServerSendData.bind(this)}/>
                                                &nbsp;
                                                <Button type="primary" size="small" icon={<PauseCircleOutlined/>}
                                                        title="stop to send data"
                                                        onClick={this.pauseServerSendData.bind(this)}/>
                                            </div>}>
                                            <div id="queryChart" style={{width: "100%", height: 650}}></div>
                                        </Card>
                                    </div>
                                </Col>
                            </Row>
                        </div>
                    </Content>
                    {/*<Footer style={{textAlign: 'center'}}>Aclyclic Join under Updates Demo</Footer>*/}
                    <Footer style={{textAlign: 'center'}}></Footer>
                </Layout>
            </div>
        );
    }

}

export default App;
