from cgi import test
import requests
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

masterHost = "localhost:8085"

def testCreateTable():
    testCases = ["t1", "t2"]
    for test in testCases:
        url = "http://" + masterHost + "/create_table?tableName={}".format(test)
        response = requests.post(url)
        logging.info("Create Table Response: " + response.text)

def testQueryTable():
    testCases = [
        {
            "tableName": "t1",
            "pkValue": "1"
        }
    ]
    for test in testCases:
        url = "http://" + masterHost + "/query_table?tableName={}&pkValue={}".format(test["tableName"], test["pkValue"])
        response = requests.post(url)
        logging.info("Query Table Response: " + response.text)

def testDropTable():
    testCases = ["t1"]
    for test in testCases:
        url = "http://" + masterHost + "/drop_table?tableName={}".format(test)
        response = requests.post(url)
        logging.info("Drop Table Response: " + response.text)

def testInsert():
    testCases = [
        {
            "tableName": "t1",
            "pkValue": "1",
        }
    ]
    for test in testCases:
        url = "http://" + masterHost + "/insert?tableName={}&pkValue={}".format(test["tableName"], test["pkValue"])
        response = requests.post(url)
        logging.info("Insert Response: " + response.text)

def testUpdate():
    testCases = [
        "t1"
    ]

    for test in testCases:
        url = "http://" + masterHost + "/update?tableName={}".format(test)
        response = requests.post(url)
        logging.info("Update Response: " + response.text)

def testDelete():
    testCases = [
        "t1"
    ]

    for test in testCases:
        url = "http://" + masterHost + "/delete?tableName={}".format(test)
        response = requests.post(url)
        logging.info("Delete Response: " + response.text)

def testMetaInfo():
    url = "http://" + masterHost + "/meta_info"
    response = requests.post(url)
    logging.info("Meta Info Response: " + response.text)

if __name__ == "__main__":    
    testMetaInfo()
    testCreateTable()
    testQueryTable()
    testDropTable()
    testInsert()
    testUpdate()
    testDelete()