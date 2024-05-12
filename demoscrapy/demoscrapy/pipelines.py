# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import psycopg2
import mysql.connector as mysql
from pymongo import MongoClient

class DemoscrapyPipeline:
    def process_item(self, item, spider):
        return item


class SavingToMysqlPipeline(object):
    
    def __init__(self):
        self.create_connection()
        
    def create_connection(self):
        self.connection = mysql.connect(
            host='localhost',
            user='root',
            password='26052003',
            database='mydata',
            port='3333'
        )
        self.curr = self.connection.cursor()
        
    def process_item(self, item, spider):
        self.store_db(item)
        return item
    
    def store_db(self, item):
        try:
            self.curr.execute("""INSERT INTO products (text, author) VALUES (%s, %s)""", (
                item["text"],
                item["author"]
            ))
            self.connection.commit()
        except mysql.Error as e:
            print("Error while storing data to MySQL:", e)
            self.connection.rollback()

    def close_spider(self, spider):
        self.curr.close()
        self.connection.close()

class SavingToPostgresPipeline(object):
    
    def __init__(self):
        self.create_connection()
        
    def create_connection(self):
        self.connection = psycopg2.connect(
            host='localhost',
            user='postgres',
            password='26052003',
            database='mydata',
        )
        self.curr = self.connection.cursor()
        
    def process_item(self, item, spider):
        self.store_db(item)
        return item
    
    def store_db(self, item):
        try:
            self.curr.execute("""INSERT INTO products (text, author) VALUES (%s, %s)""", (
                item["text"],
                item["author"]
            ))
            self.connection.commit()
        except psycopg2.Error as e:
            print("Error while storing data to PostgreSQL:", e)
            self.connection.rollback()

    def close_spider(self, spider):
        self.curr.close()
        self.connection.close()

class SavingToMongoDBPipeline(object):
    
    def __init__(self):
        self.create_connection()
        
    def create_connection(self):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client['mydata']
        self.collection = self.db['products']
        
    def process_item(self, item, spider):
        self.store_db(item)
        return item
    
    def store_db(self, item):
        try:
            self.collection.insert_one({
                "text": item["text"],
                "author": item["author"]
            })
        except Exception as e:
            print("Error while storing data to MongoDB:", e)

    def close_spider(self, spider):
        self.client.close()

