import React, {useEffect, useState} from 'react';
import {Button, Col, Input, Layout, List, Row, Table} from 'antd';
import axios from 'axios';
import {CheckCircleOutlined, CloseCircleOutlined, ThunderboltOutlined} from '@ant-design/icons';

const { TextArea } = Input;
const server = "http://127.0.0.1:8081";
const cache = {}; // tableName -> hostName 缓存

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
            console.log(trimmedStatement);

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
                tableName = trimmedStatement.split(' ')[2];
            } else if (trimmedStatement.startsWith('UPDATE')) {
                type = 'UPDATE';
                tableName = trimmedStatement.split(' ')[1];
            }

            let url = '';
            if (type === 'CREATE') {
                url = server + `/create_table?tableName=${tableName}`;
            } else if (type === 'QUERY') {
                url = server + `/query_table?tableName=${tableName}`;
            } else {
                url = server + `/write_table?tableName=${tableName}`;
            }
            console.log(tableName);
            console.log(url);

            // 给每条指令 0.1 秒的执行时间
            await new Promise(resolve => setTimeout(resolve, 100));
            console.log(cache);

            if (cache[tableName]) {
                let newUrl = '';
                if (type === 'CREATE') {
                    newUrl = `http://${cache[tableName]}:9090/create`;
                } else if (type === 'QUERY') {
                    newUrl = `http://${cache[tableName]}:9090/query`;
                } else if (type === 'DROP') {
                    newUrl = `http://${cache[tableName]}:9090/drop`;
                } else if (type === 'UPDATE') {
                    newUrl = `http://${cache[tableName]}:9090/update`;
                }
                axios.post(newUrl, {
                    sql: trimmedStatement,
                    tableName: tableName
                })
                    .then(response => {
                        const {msg, status} = response.data;
                        const icon = status === '200' ? <CheckCircleOutlined style={{color: 'green'}}/> :
                            <CloseCircleOutlined style={{color: 'red'}}/>;
                        const newMsg = (
                            <span>
                                    {icon} {trimmedStatement} {msg}
                                </span>
                        );
                        setMsgList(prevMsgList => {
                            // 最多保留8条消息
                            return [...prevMsgList, newMsg].slice(-8);
                        });
                        if (type === 'QUERY' && status === '200'){
                            const { status, msg, ...data } = response.data;
                            const newColumns = [];
                            const newDataSource = [];

                            Object.keys(data).forEach(key => {
                                if (key === 'Column Name') {
                                    const columnNames = data["Column Name"].split(" ");
                                    columnNames.forEach(name => {
                                        newColumns.push({
                                            title: name.toUpperCase(),
                                            dataIndex: name.toLowerCase(),
                                            key: name.toLowerCase()
                                        });
                                    });
                                } else {
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
                            newDataSource.sort((a, b) => {
                                return a.key - b.key;
                            });
                            setColumns(newColumns);
                            setDataSource(newDataSource);

                            console.log(columns);
                            console.log(dataSource);
                        }
                    })
                    .catch(error => {
                        console.error('Error:', error);
                        delete cache[tableName];
                        handleSubmit();
                    });
            }else{
                axios.post(url, {text})
                    .then(response => {
                        const hostName = response.data.data.hostName;
                        // const hostName = "127.0.0.1";
                        console.log(hostName);
                        cache[tableName] = hostName;

                        // 构建新的 URL
                        let newUrl = '';
                        if (type === 'CREATE') {
                            newUrl = `http://${hostName}:9090/create`;
                        } else if (type === 'QUERY') {
                            newUrl = `http://${hostName}:9090/query`;
                        } else if (type === 'DROP') {
                            newUrl = `http://${hostName}:9090/drop`;
                        } else if (type === 'UPDATE') {
                            newUrl = `http://${hostName}:9090/update`;
                        }

                        axios.post(newUrl, {
                            sql: trimmedStatement,
                            tableName: tableName
                        })
                            .then(response => {
                                const {msg, status} = response.data;
                                const icon = status === '200' ? <CheckCircleOutlined style={{color: 'green'}}/> :
                                    <CloseCircleOutlined style={{color: 'red'}}/>;
                                const newMsg = (
                                    <span>
                                    {icon} {trimmedStatement} {msg}
                                </span>
                                );
                                setMsgList(prevMsgList => {
                                    // 最多保留8条消息
                                    return [...prevMsgList, newMsg].slice(-8);
                                });
                                if (type === 'QUERY' && status === '200'){
                                    const { status, msg, ...data } = response.data;
                                    const newColumns = [];
                                    const newDataSource = [];

                                    Object.keys(data).forEach(key => {
                                        if (key === 'Column Name') {
                                            const columnNames = data["Column Name"].split(" ");
                                            columnNames.forEach(name => {
                                                newColumns.push({
                                                    title: name.toUpperCase(),
                                                    dataIndex: name.toLowerCase(),
                                                    key: name.toLowerCase()
                                                });
                                            });
                                        } else {
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
                                    newDataSource.sort((a, b) => {
                                        return a.key - b.key;
                                    });
                                    setColumns(newColumns);
                                    setDataSource(newDataSource);

                                    console.log(columns);
                                    console.log(dataSource);
                                }
                            })
                            .catch(error => {
                                console.error('Error:', error);
                            });
                    })
                    .catch(error => {
                        console.error('Error:', error);
                    });
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