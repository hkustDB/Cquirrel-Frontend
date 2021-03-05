import React, {Component} from 'react';
import ReactDOM from 'react-dom';
import {Button, Card} from 'antd';

import "./relationgraph-card.css"


export default class RelationGraph extends Component {
    constructor(props) {
        super(props);
    }


    data = {
        "binary": [
            "c_custkey = o_custkey",
            "l_orderkey = o_orderkey",
            "l_receiptdate > l_commitdate"
        ],
        "relations": [
            "lineitem",
            "orders",
            "customer"
        ],
        "unary": [
            {
                "lineitem": [
                    "l_shipdate > DATE '1995-03-15'"
                ]
            },
            {
                "orders": [
                    "o_orderdate < DATE '1995-03-15'"
                ]
            },
            {
                "customer": [
                    "c_mktsegment = 'BUILDING'"
                ]
            }
        ]
    }

    render() {
        return (
            <div className="relationgraph-card">
                <Card title="Relations Graph: ">
                    {
                        this.props.showRelationGraph ? (
                            <div style={{textAlign:"left"}}>
                                Base Relations:<br/>
                                <div>
                                    {this.data.relations.map((rel) => {
                                        return <Button shape="round" size="small">{rel}</Button>
                                    })}
                                </div>
                                Binary Predicates:<br/>
                                <div>
                                    {this.data.binary.map((rel) => {
                                        return <Button shape="round" size="small">{rel}</Button>
                                    })}
                                </div>
                                Unary Predicates:<br/>
                                <div>
                                    {this.data.unary.map((rel) => {
                                        for (var key in rel) {
                                            return <Button shape="round" size="small">{rel[key]}</Button>
                                        }
                                    })}
                                </div>

                            </div>
                        ):null
                    }
                </Card>
            </div>
        )
    }
}
