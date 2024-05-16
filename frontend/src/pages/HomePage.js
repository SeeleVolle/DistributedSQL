import React from 'react';
import MainContent from "../components/MainContent"
import StatusBar from "../components/StatusBar";
import { Layout } from "antd";
import {CloudOutlined} from '@ant-design/icons';

const { Header, Sider, Content } = Layout;

class HomePage extends React.Component {
    constructor(props) {
        super(props);
    }

    render() {
        return (
            <div>
                <Layout style={{ minHeight: '100vh' }}>
                    <Header>
                        <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
                            <CloudOutlined style={{ fontSize: '20px', color: 'white', marginRight: '8px' }} />
                            <span style={{ color: "white", fontSize: "25px" }}>分布式数据库</span>
                        </div>
                    </Header>
                    <Layout>
                        <Sider style={{ backgroundColor: 'lightgrey' }}>
                            <StatusBar />
                        </Sider>
                        <Content>
                            <MainContent />
                        </Content>
                    </Layout>
                </Layout>
            </div>
        )
    }
}
export default HomePage;