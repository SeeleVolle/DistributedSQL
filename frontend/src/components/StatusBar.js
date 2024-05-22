import React, { useEffect, useState } from 'react';
import axios from "axios";
import { Table, Button } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';

// const server = "http://127.0.0.1:8080";
const MasterNode = [
    'http://10.193.161.72:8080',
    // 'http://10.193.161.72:8082',
    // 'http://10.193.161.72:8083',
    // 'http://10.193.161.72:8084'
];

const StatusBar = () => {
    const data = {};

    const [tables, setTables] = useState([]);

    useEffect(() => {
        fetchData();
    }, []);

    const fetchData = async () => {
        for (const node of MasterNode) {
            try {
                const response = await Promise.race([
                    axios.post(node + "/meta_info", data),
                    new Promise((_, reject) =>
                        setTimeout(() => reject(new Error('Request timeout')), 5000) // 设置超时时间为5秒
                    )
                ]);

                console.log(response);

                if (response.data && response.data.data && response.data.data.meta && response.data.data.meta.regions) {
                    const responseData = response.data.data.meta.regions;
                    const allTables = responseData.reduce((acc, curr) => {
                        return [...acc, ...curr.tables];
                    }, []);
                    setTables(allTables);
                    return;
                }
            } catch (error) {
                console.error(`Error fetching data from ${node}:`, error);
            }
        }

        console.error('All requests timed out');
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