from pymongo import MongoClient # type: ignore
from bson.objectid import ObjectId  # type: ignore

class AnimalShelter(object):
    """ CRUD operations for Animal collection in MongoDB """

    def __init__(self):
        """
        Initializing the MongoClient. This helps to 
        access the MongoDB databases and collections.
        This is hard-wired to use the aac database, the 
        animals collection, and the aac user.
        Definitions of the connection string variables are
        unique to the individual Apporto environment.

        You must edit the connection variables below to reflect
        your own instance of MongoDB!
        """

        # Connection Variables
        USER = 'aacuser'
        PASS = 'kakarot'
        HOST = 'nv-desktop-services.apporto.com'
        PORT = 32980
        DB = 'AAC'
        COL = 'animals'

        # Initialize Connection
        self.client = MongoClient('mongodb://%s:%s@%s:%d' % (USER,PASS,HOST,PORT))
        self.database = self.client['%s' % (DB)]
        self.collection = self.database['%s' % (COL)]

    def create(self, data):
        """
        Inserts a document into the MongoDB collection.
        :param data: A dictionary representing the key-value pairs to be inserted as a document.
        :return: True if the insertion is successful and acknowledged by MongoDB, else False.
        :raises ValueError: If the input data is invalid (not a dictionary or empty).
        """
        if isinstance(data, dict) and data:  # Ensure data is a non-empty dictionary
            try:
                result = self.collection.insert_one(data)  # Insert the data into the collection
                return result.acknowledged  # Return True if the insertion is acknowledged
            except Exception as e:
                print(f"Error inserting data: {e}")  # Log the error
                return False  # Return False if there was an error
        else:
            raise ValueError("Invalid data: Must be a non-empty dictionary")  # Raise an exception for invalid input
        
    def read(self, query):
        """
        Queries for documents from the MongoDB collection based on the query.
        :param query: A dictionary representing the key/value lookup pair for the query.
        :return: A list of matching documents if successful, else an empty list.
        """
        if isinstance(query, dict):  # Ensure the query is a dictionary
            try:
                results = self.collection.find(query)  # Query the collection
                return [doc for doc in results]  # Convert the cursor to a list of documents
            except Exception as e:
                print(f"Error reading data: {e}")  # Log the error
                return []  # Return an empty list if there was an error
        else:
            raise ValueError("Invalid query: Must be a dictionary")  # Raise an exception for invalid input

    def update(self, filter_criteria, new_values, update_many=False):
        """
        Updates document(s) in the MongoDB collection.
        :param filter_criteria: Key-value pairs to filter documents for update.
        :param new_values: Key-value pairs to update the matching documents.
        :param update_many: Boolean, if True, updates multiple documents; otherwise, only one.
        :return: The number of objects modified.
        """
        try:
            update_query = {"$set": new_values}  # Build the update query
            if update_many:
                result = self.collection.update_many(filter_criteria, update_query)
            else:
                result = self.collection.update_one(filter_criteria, update_query)
            return result.modified_count  # Return the count of modified documents
        except Exception as e:
            print(f"An error occurred during update: {e}")
            return 0  # Return 0 if an error occurs

    def delete(self, filter_criteria, delete_many=False):
        """
        Deletes document(s) from the MongoDB collection.
        :param filter_criteria: Key-value pairs to filter documents for deletion.
        :param delete_many: Boolean, if True, deletes multiple documents; otherwise, only one.
        :return: The number of objects deleted.
        """
        try:
            if delete_many:
                result = self.collection.delete_many(filter_criteria)
            else:
                result = self.collection.delete_one(filter_criteria)
            return result.deleted_count  # Return the count of deleted documents
        except Exception as e:
            print(f"An error occurred during deletion: {e}")
            return 0  # Return 0 if an error occurs