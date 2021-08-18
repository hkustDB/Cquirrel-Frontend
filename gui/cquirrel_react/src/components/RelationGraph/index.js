import React, {Component} from 'react';
import ReactDOM from 'react-dom';
import {Button, Card} from 'antd';

import "./relationgraph-card.css"


export default class RelationGraph extends Component {
    constructor(props) {
        super(props);
    }

    render() {
        return (
            <div className="relationgraph-card">
                <Card title="Relations Graph: ">
                    {
                        this.props.showRelationGraph ? (
                            <div style={{textAlign: "left"}}>
                                Base Relations:<br/>
                                <div>
                                    {this.props.relationsData.relations.map((rel) => {
                                        if (rel === "lineitemps") {
                                            return
                                        } else if (rel === "lineitems") {
                                            return
                                        } else if (rel === "lineitemorder") {
                                            return <Button shape="round" size="small">lineitem</Button>
                                        } else if (rel === "partsupps") {
                                            return
                                        } else if (rel === "partsuppp") {
                                            return <Button shape="round" size="small">partsupp</Button>
                                        } else {
                                            return <Button shape="round" size="small">{rel}</Button>
                                        }
                                    })}
                                </div>
                                Binary Predicates:<br/>
                                <div>
                                    {this.props.relationsData.binary.map((rel) => {
                                        return <Button shape="round" size="small">{rel}</Button>
                                    })}
                                </div>
                                Unary Predicates:<br/>
                                <div>
                                    {
                                        this.props.relationsData.unary.map(
                                            (rel) => {
                                                var tmpUnary = [];
                                                for (var key in rel) {
                                                    if (rel[key] instanceof Array) {
                                                        var len = rel[key].length
                                                        for (var i = 0; i < len; i++) {
                                                            if (rel[key][i].length > 40) {
                                                                function newline(str,n){
                                                                    var len = str.length;
                                                                    var strTemp = '';
                                                                    if(len > n){
                                                                        strTemp = str.substring(0,n);
                                                                        str = str.substring(n,len);
                                                                        return strTemp+'\n'+newline(str,n);
                                                                    }else{
                                                                        return str;
                                                                    }
                                                                }
                                                                var tmpStr = newline(rel[key][i], 40);
                                                                console.log("tmpStr: ", tmpStr)
                                                                tmpUnary.push(<Button shape="round"
                                                                                      size="small">{rel[key][i].replace(/\n\t/g, "")}</Button>)
                                                            } else {
                                                                tmpUnary.push(<Button shape="round"
                                                                                      size="small">{rel[key][i]}</Button>)
                                                            }
                                                        }
                                                    }
                                                }
                                                return tmpUnary
                                            })
                                    }
                                </div>

                            </div>
                        ) : null
                    }
                </Card>
            </div>
        )
    }
}
