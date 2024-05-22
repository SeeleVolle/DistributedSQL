import React, {useEffect, useState} from 'react';
import {Button, Col, Input, Layout, List, Row, Table} from 'antd';
import axios from 'axios';
import {CheckCircleOutlined, CloseCircleOutlined, ThunderboltOutlined} from '@ant-design/icons';

const { TextArea } = Input;
const MasterNode = [
    // 'http://172.25.2.229:8080'
    'http://172.25.2.229:8081',
    'http://172.25.2.229:8082',
    'http://172.25.2.229:8083',
    'http://172.25.2.229:8084'
];

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
        // 分割语句，使用分号作为分隔符，并去除每个语句末尾的空白字符
        const sqlStatements = sqlText.split(';').map(statement => statement.trim());

        const trimmedStatements = [];

        sqlStatements.forEach((statement) => {
            if (!statement) {
                return;
            }

            // 将单个语句中的换行符替换为空白符
            const normalizedStatement = statement.replace(/\n/g, ' ');

            let inSingleQuotes = false;
            let inParentheses = false;
            let currentPart = '';

            for (let i = 0; i < normalizedStatement.length; i++) {
                const char = normalizedStatement[i];

                if (inSingleQuotes) {
                    if (char === "'" && normalizedStatement[i - 1] !== '\\') {
                        inSingleQuotes = false;
                    }
                    currentPart += char;
                } else if (inParentheses) {
                    if (char === ')') {
                        inParentheses = false;
                    }
                    currentPart += char;
                } else {
                    if (char === '\'') {
                        inSingleQuotes = true;
                        currentPart += char;
                    } else if (char === '(') {
                        inParentheses = true;
                        currentPart += ' (';
                    } else if (char === ')') {
                        currentPart += ' )';
                    } else {
                        // 将非小括号和单引号内的内容转换为大写
                        currentPart += char.toUpperCase();
                    }
                }
            }

            if (!currentPart.endsWith(';')) {
                currentPart += ' ;';
            }

            trimmedStatements.push(currentPart);
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
                tableName = trimmedStatement.slice(fromIndex).match(/\w+\b/)[0];
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
            } else if (trimmedStatement.startsWith('DELETE')) {
                type = 'DELETE';
                const fromIndex = trimmedStatement.indexOf('FROM') + 4;
                tableName = trimmedStatement.slice(fromIndex).match(/\w+\b/)[0];
            }

            let count = 0;
            for (count = 0; count < MasterNode.length; count++) {
                const server = MasterNode[count];
                let url = '';
                if (type === 'CREATE') {
                    url = server + `/create_table?tableName=${tableName}`;
                } else if (type === 'QUERY') {
                    url = server + `/query_table?tableName=${tableName}`;
                } else if (type === 'INSERT') {
                    url = server + `/insert?tableName=${tableName}&pkValue=${pkValue}`;
                } else if (type === 'UPDATE') {
                    url = server + `/update?tableName=${tableName}`;
                } else if (type === 'DROP') {
                    url = server + `/drop_table?tableName=${tableName}`
                } else if (type === 'DELETE') {
                    url = server + `/delete?tableName=${tableName}`
                }
                console.log(tableName);
                console.log(url);

                try {
                    // 设置响应超时时间为1秒
                    const responseTimeout = 1000;
                    let isResponseReceived = false;
                    // 给每条指令 0.1 秒的执行时间
                    await new Promise(resolve => setTimeout(resolve, 100));

                    const requestPromise = axios.post(url, {text})
                        .then(response => {
                            isResponseReceived = true;
                            const hostNames = response.data.data.hostNames;
                            if (response.data && response.data.data && response.data.data.hostNames) {
                                for (let i = 0; i < hostNames.length; i++) {
                                    if (typeof hostNames[i] === 'string') {
                                        hostNames[i] = hostNames[i].slice(0, -5);
                                    }
                                }
                            }
                            const masterStatus = response.data.status;
                            const masterMessage = response.data.message;
                            // const hostNames = ["127.0.0.1"];
                            console.log(hostNames);
                            console.log(masterMessage);
                            console.log(masterStatus);

                            if (masterStatus !== 200){
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
                                    let newUrl = '';
                                    if (type === 'CREATE' || type === 'QUERY' || type === 'DROP') {
                                        newUrl = `http://${hostName}:9090/${type.toLowerCase()}`;
                                    } else if (type === 'INSERT' || type === 'UPDATE' || type === 'DELETE') {
                                        newUrl = `http://${hostName}:9090/update`;
                                    }
                                    console.log(newUrl);
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

                    if (isResponseReceived) {
                        break;
                    }
                } catch (error) {
                    console.error('Error from node ' + server + ':', error);
                }
                if (count === MasterNode.length)
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