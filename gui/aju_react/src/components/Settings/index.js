import React, {Component} from 'react'
import {Input, Modal, Switch, Form, Button} from 'antd'
import axios from "axios";

export default class Settings extends Component {

    state = {
        visible: false
    };

    showModal = (e) => {
        this.setState({
            visible: true
        })
    };

    hideModal = () => {
        this.setState({
            visible: false,
        });
    };

    handleSaveSettings = () => {
        // TODO
        this.setState({
            visible: false,
        });
    };

    handleRemoteFlinkChecked = (checked, ev) => {

    }

    onFinish = (values) => {
        console.log('Success:', values);
        console.log(typeof values)
        console.log(JSON.stringify(values))
        axios.post("http://localhost:5000/r/save_settings", (values), {
            headers: {
                'Access-Control-Allow-Origin': '*',
            }
        }).then(
            res => {
                console.log(res);
            }
        )
    }

    onFinishFailed = (errorInfo) => {
        console.log('Failed:', errorInfo);
    }

    render() {
        const layout = {
            labelCol: {span: 8},
            wrapperCol: {span: 16},
        };

        return (
            <div>
                <div onClick={this.showModal.bind(this)}>Settings</div>
                <Modal
                    title="Settings"
                    visible={this.state.visible}
                    onOk={this.handleSaveSettings}
                    onCancel={this.hideModal}
                    okText="Save"
                    footer={null}
                >
                    <Form
                        {...layout}
                        name="basic"
                        initialValues={{remember: true}}
                        onFinish={this.onFinish}
                        onFinishFailed={this.onFinishFailed}
                    >
                        <Form.Item
                            label="Remote Flink"
                            name="remote_flink"
                            initialValue = {false}
                        >
                            <Switch defaultChecked={false}/>
                        </Form.Item>

                        <Form.Item
                            label="Remote Flink Url"
                            name="remote_flink_url"
                            initialValue = "47.93.121.10:8081"
                        >
                            <Input placeholder="47.93.121.10:8081" />
                        </Form.Item>

                        <Form.Item
                            label="Flink Home Path"
                            name="flink_home_path"
                            initialValue = "/Users/chaoqi/Programs/flink-1.11.2"
                        >
                            <Input placeholder="/Users/chaoqi/Programs/flink-1.11.2"/>
                        </Form.Item>

                        <Form.Item
                            label="Flink Parallelism"
                            name="flink_parallelism"
                            initialValue = "2"
                        >
                            <Input placeholder="2"/>
                        </Form.Item>

                        {/*<Form.Item*/}
                        {/*    label="Q3 Input Data File"*/}
                        {/*    name="q3_input_datafile"*/}
                        {/*>*/}
                        {/*    <Input placeholder="/Users/chaoqi/Projects/AJU/code/gui-codegen/gui/aju_flask/aju_app/resources/input_data_q3.csv"/>*/}
                        {/*</Form.Item>*/}

                        {/*<Form.Item*/}
                        {/*    label="Q6 Input Data File"*/}
                        {/*    name="q6_input_datafile"*/}
                        {/*>*/}
                        {/*    <Input placeholder="/Users/chaoqi/Projects/AJU/code/gui-codegen/gui/aju_flask/aju_app/resources/input_data_q6.csv"/>*/}
                        {/*</Form.Item>*/}

                        {/*<Form.Item*/}
                        {/*    label="Q10 Input Data File"*/}
                        {/*    name="q10_input_datafile"*/}
                        {/*>*/}
                        {/*    <Input placeholder="/Users/chaoqi/Projects/AJU/code/gui-codegen/gui/aju_flask/aju_app/resources/input_data_q10.csv"/>*/}
                        {/*</Form.Item>*/}
                        {/*<Form.Item*/}
                        {/*    label="Q18 Input Data File"*/}
                        {/*    name="q18_input_datafile"*/}
                        {/*>*/}
                        {/*    <Input placeholder="/Users/chaoqi/Projects/AJU/code/gui-codegen/gui/aju_flask/aju_app/resources/input_data_q18.csv"/>*/}
                        {/*</Form.Item>*/}
                        <Button type="primary" htmlType="submit">Save</Button> <span> </span>
                        <Button type="primary" onClick={this.hideModal}>Cancel</Button>

                    </Form>


                </Modal>
            </div>

        )
    }
}
