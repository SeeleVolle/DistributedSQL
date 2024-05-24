from cgi import test
from doctest import master
import requests
import logging
import json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Test:
    masterHost = None

    def __init__(self, masterHost:str = None):
        self.masterHost = masterHost

    def testCreateTable(self, testCase:str):
        url = "http://" + self.masterHost + "/create_table?tableName={}".format(testCase)
        response = requests.post(url)
        logging.info("Create Table Response: " + response.text)

    def testQueryTable(self, testCase:str):
        url = "http://" + self.masterHost + "/query_table?tableName={}".format(testCase)
        response = requests.post(url)
        logging.info("Query Table Response: " + response.text)

    def testDropTable(self, testCase:str):
        url = "http://" + self.masterHost + "/drop_table?tableName={}".format(testCase)
        response = requests.post(url)
        logging.info("Drop Table Response: " + response.text)

    def testInsert(self, testCase:dict):
        url = "http://" + self.masterHost + "/insert?tableName={}&pkValue={}".format(testCase["tableName"], testCase["pkValue"])
        response = requests.post(url)
        logging.info("Insert Response: " + response.text)
            

    def testUpdate(self, testCase:str):
        url = "http://" + self.masterHost + "/update?tableName={}".format(testCase)
        response = requests.post(url)
        logging.info("Update Response: " + response.text)

    def testDelete(self, testCase:str):
        url = "http://" + self.masterHost + "/delete?tableName={}".format(testCase)
        response = requests.post(url)
        logging.info("Delete Response: " + response.text)

    def testMetaInfo(self):
        url = "http://" + self.masterHost + "/meta_info"
        response = requests.post(url)
        logging.info("Meta Info Response: " + response.text)
        print(json.dumps(json.loads(response.text)))

if __name__ == "__main__":    
    masters = [Test("localhost:8081"), Test("localhost:8082"), Test("localhost:8083")]
    for i in range(0, 10):
        for master in masters:
            master.testQueryTable("PRODUCTS")