import React, {useEffect, useState} from 'react';
import {Button, Col, Input, Layout, List, Row, Table} from 'antd';
import axios from 'axios';
import {CheckCircleOutlined, CloseCircleOutlined, ThunderboltOutlined} from '@ant-design/icons';

const { TextArea } = Input;
const MasterNode = {
    NODE_1: 'http://10.194.223.161:8081',
    NODE_2: 'http://10.194.223.161:8082',
    NODE_3: 'http://10.194.223.161:8083',
    NODE_4: 'http://10.194.223.161:8084'
};
// const server = "http://127.0.0.1:8080";

const MainContent = () => {
    const [text, setText] = useState('');
    const [msgList, setMsgList] = useState([]);
    const [columns, setColumns] = useState([]);
    const [dataSource, setDataSource] = useState([]);

    useEffect(() => {
        handleSubmit();
    }, []);

    const handleChange = (event) => {
        setText(event.target.value);
    };

    const parseSQLStatements = (sqlText) => {
        const sqlStatements = sqlText.split('\n');
        const trimmedStatements = [];

        sqlStatements.forEach((statement) => {

            // 如果语句中包含小括号，则在其前面添加一个空格
            if (statement.includes('(')) {
                statement = statement.replace('(', ' (');
            }

            // 将原始语句中的小括号内部数据提取出来
            const parts = statement.split(/\(([^)]+)\)/);

            // 转换除小括号内部以外的部分为大写
            const trimmedStatement = parts.map((part, index) => {
                // 如果是小括号内部的部分，则保持原样
                if (index % 2 === 1) {
                    return `(${part.trim()})`;
                } else {
                    return part.trim().toUpperCase();
                }
            }).join(' ');

            const lastIndex = trimmedStatement.length - 1;
            trimmedStatements.push(trimmedStatement.substring(0, lastIndex) + ' ' + trimmedStatement[lastIndex]);
        });

        return trimmedStatements;
    };

    const handleSubmit = async () => {
        const trimmedStatements = parseSQLStatements(text);

        for (const trimmedStatement of trimmedStatements) {
            let type = '';
            let tableName = '';
            let pkValue = '';

            if (trimmedStatement.startsWith('CREATE TABLE')) {
                type = 'CREATE';
                tableName = trimmedStatement.split(' ')[2];
            } else if (trimmedStatement.startsWith('SELECT')) {
                type = 'QUERY';
                const fromIndex = trimmedStatement.indexOf('FROM') + 4;
                tableName = trimmedStatement.substring(fromIndex).trim();
            } else if (trimmedStatement.startsWith('DROP TABLE')) {
                type = 'DROP';
                tableName = trimmedStatement.split(' ')[2];
            } else if (trimmedStatement.startsWith('INSERT INTO')) {
                type = 'INSERT';
                const splitStatement = trimmedStatement.split(' ');
                tableName = splitStatement[2];
                const valuesStartIndex = trimmedStatement.indexOf('VALUES') + 6;
                const valuesEndIndex = trimmedStatement.length - 1;
                const valuesString = trimmedStatement.substring(valuesStartIndex, valuesEndIndex);
                const valuesArray = valuesString.split(',');
                pkValue = valuesArray[0].replace('(', '').replace(')', '').trim();
            } else if (trimmedStatement.startsWith('UPDATE')) {
                type = 'UPDATE';
                tableName = trimmedStatement.split(' ')[1];
            }

            for (let i = 0; i < MasterNode.length; i++) {
                const server = MasterNode[i];
                let url = '';
                if (type === 'CREATE') {
                    url = server + `/create_table?tableName=${tableName}`;
                } else if (type === 'QUERY') {
                    url = server + `/query_table?tableName=${tableName}`;
                } else if (type === 'INSERT') {
                    url = server + `/insert?tableName=${tableName}&${pkValue}`;
                } else if (type === 'UPDATE') {
                    url = server + `/update?tableName=${tableName}`;
                } else if (type === 'DROP') {
                    url = server + `/drop_table?tableName=${tableName}`
                } else if (type === 'DELETE') {
                    url = server + `/delete?tableName=${tableName}`
                }
                // console.log(tableName);
                // console.log(url);

                try {
                    // 设置响应超时时间为1秒
                    const responseTimeout = 1000;
                    let isResponseReceived = false;
                    // 给每条指令 0.1 秒的执行时间
                    await new Promise(resolve => setTimeout(resolve, 100));

                    const requestPromise = axios.post(url, {text})
                        .then(response => {
                            const hostNames = response.data.data.hostNames;
                            const masterStatus = response.data.status;
                            const masterMessage = response.data.message;
                            // const hostNames = ["127.0.0.1"];
                            // console.log(hostNames);
                            // console.log(masterMessage);

                            if (masterStatus !== '200'){
                                const icon = <CloseCircleOutlined style={{color: 'red'}}/>;
                                const newMsg = (
                                    <span>
                                    {icon} {trimmedStatement} {masterMessage}
                                </span>
                                );
                                setMsgList(prevMsgList => {
                                    return [...prevMsgList, newMsg].slice(-8);
                                });
                            } else {
                                const requests = hostNames.map(hostName => {
                                    const newUrl = `http://${hostName}:9090/${type.toLowerCase()}`;
                                    return axios.post(newUrl, {
                                        sql: trimmedStatement,
                                        tableName: tableName
                                    });
                                });

                                Promise.all(requests)
                                    .then(responses => {
                                        let allMsg = 'Success!';
                                        let allStatus = '200';
                                        responses.forEach(response => {
                                            const { msg, status } = response.data;
                                            if (status !== '200') {
                                                allStatus = status;
                                                allMsg = `${msg}`;
                                            }
                                        });

                                        const icon = allStatus === '200' ? <CheckCircleOutlined style={{color: 'green'}}/> :
                                            <CloseCircleOutlined style={{color: 'red'}}/>;
                                        const newMsg = (
                                            <span>
                                    {icon} {trimmedStatement} {allMsg}
                                </span>
                                        );
                                        setMsgList(prevMsgList => {
                                            return [...prevMsgList, newMsg].slice(-8);
                                        });

                                        if (type === 'QUERY' && allStatus === '200') {
                                            const newColumns = [];
                                            const newDataSource = [];

                                            responses.forEach(response => {
                                                const { status, msg, ...data } = response.data;
                                                console.log(response.data);
                                                if (status === '200') {
                                                    Object.keys(data).forEach(key => {
                                                        if (key === 'Column Name') {
                                                            const columnNames = data[key].split(" ");
                                                            columnNames.forEach(name => {
                                                                newColumns.push({
                                                                    title: name.toUpperCase(),
                                                                    dataIndex: name.toLowerCase(),
                                                                    key: name.toLowerCase()
                                                                });
                                                            });
                                                        }
                                                    });
                                                    console.log(newColumns);
                                                    Object.keys(data).forEach(key => {
                                                        if (key !== 'Column Name'){
                                                            const rowKey = key.match(/\d+/)[0];
                                                            const rowData = data[key].split(" ");
                                                            const rowObject = { key: rowKey };
                                                            rowData.forEach((value, index) => {
                                                                const columnName = newColumns[index].dataIndex;
                                                                rowObject[columnName] = value;
                                                            });
                                                            newDataSource.push(rowObject);
                                                        }
                                                    });
                                                }
                                            });

                                            newDataSource.sort((a, b) => {
                                                return a.key - b.key;
                                            });

                                            setColumns(newColumns);
                                            setDataSource(newDataSource);
                                            // console.log(columns);
                                            // console.log(dataSource);
                                        }
                                    })
                                    .catch(error => {
                                        console.error('Error:', error);
                                    });
                            }
                        })
                        .catch(error => {
                            console.error('Error:', error);
                        });
                    // 设置定时器，在指定的时间后检查是否收到响应
                    const timeoutPromise = new Promise((resolve, reject) => {
                        setTimeout(() => {
                            if (!isResponseReceived) {
                                reject(new Error('Response timeout'));
                            }
                        }, responseTimeout);
                    });
                    // 等待请求完成或超时
                    await Promise.race([requestPromise, timeoutPromise]);

                    break;
                } catch (error) {
                    console.error('Error from node ' + server + ':', error);
                }
                throw new Error('All nodes failed to respond');
            }
        }
    };

    return (
        <div>
            <Layout style={{ padding: '24px', minHeight: 280 }}>
                <Row justify="start" align="top">
                    <Col span={14}>
                        <Layout.Content style={{ background: 'white', padding: '24px', minHeight: 280 }}>
                            <div style={{ textAlign: 'left', marginBottom: '15px' }}>
                                <Button type="primary" onClick={handleSubmit} style={{ backgroundColor: '#faad14', marginRight: '10px' }}>
                                    <ThunderboltOutlined /> 执行
                                </Button>
                            </div>
                            <TextArea
                                value={text}
                                onChange={handleChange}
                                autoSize={{ minRows: 14, maxRows: 14 }}
                                placeholder="请输入 sql 命令..."
                            />
                        </Layout.Content>
                    </Col>
                    <Col span={10}>
                        <Layout.Content style={{ background: '#f5f5f5', padding: '24px', minHeight: 280 }}>
                            <List
                                bordered
                                dataSource={msgList}
                                renderItem={item => (
                                    <List.Item style={{ fontSize: '12px', color: 'grey' }}>{item}</List.Item>
                                )}
                                style={{ background: 'white' }}
                            />
                        </Layout.Content>
                    </Col>
                </Row>
            </Layout>

            <Layout.Content style={{ background: '#f5f5f5', padding: '24px', minHeight: 280 }}>
                <Table dataSource={dataSource} columns={columns} />
            </Layout.Content>
        </div>
    );
};

export default MainContent;