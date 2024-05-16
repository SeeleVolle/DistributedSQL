import React, { useEffect, useState } from 'react';
import axios from "axios";
import { Table, Button } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';

const server = "http://127.0.0.1:8081";

const StatusBar = () => {
    const address = server + "/meta_info";
    const data = {};

    const [tables, setTables] = useState([]);

    useEffect(() => {
        fetchData();
    }, []);

    const fetchData = async () => {
        try {
            const response = await axios.post(address, data);
            if (response.data && response.data.data && response.data.data.meta && response.data.data.meta.data) {
                const data = response.data.data.meta.data;
                const allTables = data.reduce((acc, curr) => {
                    return [...acc, ...curr.tables];
                }, []);
                setTables(allTables);
            } else {
                console.error('Invalid response data structure:', response.data);
            }
        } catch (error) {
            console.error('Error fetching data:', error);
        }
    };

    const handleRefresh = () => {
        fetchData();
    };

    const columns = [
        {
            title: <span style={{ fontSize: '20px' }}>可用 Tables：</span>,
            dataIndex: 'table',
            key: 'table',
        },
    ];

    return (
        <div style={{ backgroundColor: 'lightgrey', padding: '20px' }}>
            <div style={{ textAlign: 'left' }}>
                <Button type="primary" icon={<ReloadOutlined />} onClick={handleRefresh} style={{ marginBottom: '10px', marginRight: '10px', backgroundColor: 'white', color: 'black' }}/>
            </div>
            <Table
                dataSource={tables.map((table, index) => ({ table, key: index }))}
                columns={columns}
                bordered
            />
        </div>
    );
}

export default StatusBar;