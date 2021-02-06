import React, {Component} from 'react'
import {Input, Modal, Switch, Form} from 'antd'

class SettingsContent extends Component {

    handleRemoteFlinkChecked = (checked, ev) => {

    }

    render() {
        return (
            <div>
                Here are settings. <br/>
                <span>Remote Flink: </span> <Switch defaultChecked={false}
                                                    onChange={this.handleRemoteFlinkChecked.bind(this)}/>
                <span>Flink URL: </span>
            </div>
        )
    }


}

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

    render() {
        const layout = {
            labelCol: { span: 8 },
            wrapperCol: { span: 16 },
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
                >
                    <Form
                        {...layout}
                        name="basic"
                        initialValues={{ remember: true }}
                    >
                        <Form.Item
                            label="Remote Flink"
                            name="remote_flink"
                        >
                            <Switch defaultChecked={false}/>
                        </Form.Item>

                        <Form.Item
                            label="Remote Flink Url"
                            name="remote_flink_url"
                        >
                            <Input placeholder="47.93.121.10:8081" disabled/>
                        </Form.Item>

                        <Form.Item
                            label="Flink Home Path"
                            name="flink_home_path"
                        >
                            <Input placeholder="/Users/chaoqi/Programs/flink-1.11.2"/>
                        </Form.Item>

                        <Form.Item
                            label="Flink Parallelism"
                            name="flink_parallelism"
                        >
                            <Input placeholder="2"/>
                        </Form.Item>

                        <Form.Item
                            label="Q3 Input Data File"
                            name="q3_input_datafile"
                        >
                            <Input placeholder="/Users/chaoqi/Projects/AJU/code/gui-codegen/gui/aju_flask/aju_app/resources/input_data_q3.csv"/>
                        </Form.Item>

                        <Form.Item
                            label="Q6 Input Data File"
                            name="q6_input_datafile"
                        >
                            <Input placeholder="/Users/chaoqi/Projects/AJU/code/gui-codegen/gui/aju_flask/aju_app/resources/input_data_q6.csv"/>
                        </Form.Item>

                        <Form.Item
                            label="Q10 Input Data File"
                            name="q10_input_datafile"
                        >
                            <Input placeholder="/Users/chaoqi/Projects/AJU/code/gui-codegen/gui/aju_flask/aju_app/resources/input_data_q10.csv"/>
                        </Form.Item>
                        <Form.Item
                            label="Q18 Input Data File"
                            name="q18_input_datafile"
                        >
                            <Input placeholder="/Users/chaoqi/Projects/AJU/code/gui-codegen/gui/aju_flask/aju_app/resources/input_data_q18.csv"/>
                        </Form.Item>

                    </Form>


                </Modal>
            </div>

        )
    }
}
