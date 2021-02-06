import React, {Component} from 'react'
import {Input, Card, Button} from 'antd'
import "./sqlentry-card.css"
export default class SqlEntry extends Component {


    render() {

        const { TextArea } = Input;
        return (
            <div className="sqlentry-card">
                <Card title="Input SQL:" extra={<Button size="small" type="primary">Submit SQL</Button>}>
                    <TextArea placeholder="Please enter SQL here." showCount></TextArea>
                </Card>

            </div>
        );
    }
}
