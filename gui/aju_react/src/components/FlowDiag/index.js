import React, {Component} from 'react';
import {Card} from 'antd';
import "./flowdiag-card.css"
import q3_flow_img from "./q3_flow_img.png"

export default class Flowdiag extends Component {
    static defaultProps = {
        diag :"Here are flow diagram."
    }


    render() {

        return (
            <div className="flowdiag-card">
                <Card title="Flow Diagram: ">
                    {this.props.diag}
                    {/*<img src={q3_flow_img} style={{width:"100%"}} alt={"Here are flow diagram."}/>*/}
                </Card>
            </div>
        )
    }
}
