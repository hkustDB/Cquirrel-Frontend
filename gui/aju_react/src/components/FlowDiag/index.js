import React, {Component} from 'react';
import ReactDOM from 'react-dom';
import {Button, Card} from 'antd';
import {Graph, DataUri} from '@antv/x6'
import {DagreLayout} from '@antv/layout'

import "./flowdiag-card.css"
import {DownloadOutlined} from "@ant-design/icons";


export default class Flowdiag extends Component {
    static defaultProps = {
        diag: "Here are flow diagram."
    }

    constructor(props) {
        super(props);
        this.containerRef = React.createRef();


    }


    data = {
        // 节点
        nodes: [
            {
                id: 'node1',
                // x: 30,
                // y: 30,
                width: 120,
                height: 60,
                attrs: {
                    body: {
                        // ref: 'text',
                        // refWidth: 20,
                        // refHeight: 15,
                        // refX: -8,
                        // refY: -8,
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    // label: {
                    //     text: 'Data Source / Input Stream Splitter Reader'
                    // },
                    // label: {
                    text: {
                        textWrap: {
                            text: 'Data Source / Input Stream Splitter Reader',
                            width: -10,
                        }
                    },
                    // fontSize: 10,    // 文字大小
                    // },
                },
            },
            {
                id: 'node2',
                // x: 30,
                // y: 180,
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    // label: {
                    //     text: 'Customer\nProcess\nFunction',
                    //     fontSize: 10,    // 文字大小
                    // },
                    text: {
                        textWrap: {
                            text: 'Customer Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'node3',
                // x: 130,
                // y: 180,
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    // label: {
                    //     text: 'Orders\nProcess\nFunction',
                    //     fontSize: 10,    // 文字大小
                    // },
                    text: {
                        textWrap: {
                            text: 'Orders Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'node4',
                // x: 230,
                // y: 180,
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    // label: {
                    //     text: 'Lineitem\nProcess\nFunction',
                    //     fontSize: 10,    // 文字大小
                    // },
                    text: {
                        textWrap: {
                            text: 'Lineitem Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'node5',
                // x: 330,
                // y: 180,
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    // label: {
                    //     text: 'Aggregation\nProcess\nFunction',
                    //     fontSize: 10,    // 文字大小
                    // },
                    text: {
                        textWrap: {
                            text: 'Aggregation Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'node6',
                // x: 430,
                // y: 180,
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    // label: {
                    //     text: 'Data\nSink',
                    //     fontSize: 10,    // 文字大小
                    // },
                    text: {
                        textWrap: {
                            text: 'Data Sink',
                            width: -10,
                        }
                    },
                },
            },
        ],
        // 边
        edges: [
            {
                source: 'node1',
                target: 'node2',
                labels: [
                    {
                        attrs: {
                            label: {
                                text: 'HASH',
                                // fontSize: 10
                            }
                        },
                    },
                ],
            },
            {
                source: 'node2',
                target: 'node3',
                labels: [
                    {
                        attrs: {
                            label: {
                                text: 'HASH',
                                // fontSize: 10
                            }
                        },
                    },
                ],
            },
            {
                source: 'node3',
                target: 'node4',
                labels: [
                    {
                        attrs: {
                            label: {
                                text: 'HASH',
                                // fontSize: 10
                            }
                        },
                    },
                ],
            },
            {
                source: 'node4',
                target: 'node5',
                labels: [
                    {
                        attrs: {
                            label: {
                                text: 'HASH',
                                // fontSize: 10
                            }
                        },
                    },
                ],
            },
            {
                source: 'node5',
                target: 'node6',
                labels: [
                    {
                        attrs: {
                            label: {
                                text: 'REBALANCE',
                                // fontSize: 10
                            }
                        },
                    },
                ],
            },
            {
                source: 'node1',
                target: 'node3',
                labels: [
                    {
                        attrs: {
                            label: {
                                text: 'HASH',
                                // fontSize: 10
                            }
                        },
                    },
                ],
            },
            {
                source: 'node1',
                target: 'node4',
                labels: [
                    {
                        attrs: {
                            label: {
                                text: 'HASH',
                                // fontSize: 10
                            }
                        },
                    },
                ],
            },
        ],
    }

    download_flow_diagram = () => {
        if (this.graph) {
            this.graph.toSVG((dataUri: string) => {
                // 下载
                DataUri.downloadDataUri(DataUri.svgToDataUrl(dataUri), 'flow_figure.svg')
            })
        }
    }

    shouldComponentUpdate(nextProps, nextState) {
        if (nextProps.showFlowDiag) {
            const graph = new Graph({
                container: ReactDOM.findDOMNode(this.containerRef.current),
                height: 290,
                width: "100%",
                mousewheel: {
                    enabled:true,
                    factor: 1.1,
                    minScale: 0.1,
                    maxScale: 10,
                },
                panning: true,
            })
            this.graph = graph;

            const dagreLayout = new DagreLayout({
                type: 'dagre',
                rankdir: 'LR',
                align: 'DR',
                ranksep: 45,
                nodesep: 25,
                controlPoints: true,
                nodeSize: [80, 40],
            })

            const model = dagreLayout.layout(this.data)

            graph.fromJSON(model)
            graph.centerContent()
        } else {
            if (this.graph) {
                this.graph.dispose()
            }
        }
        return true
    }

    render() {

        return (
            <div className="flowdiag-card">
                <Card title="Flow Diagram: " extra={
                    <div>
                        <Button size="small" type="link" icon={<DownloadOutlined/>}
                                onClick={this.download_flow_diagram}>Download Flow Diagram</Button>
                    </div>
                }>
<div className="flowdiag-content">
    <div  ref={this.containerRef}/>
</div>

                </Card>
            </div>
        )
    }
}
