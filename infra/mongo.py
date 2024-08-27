from __future__ import annotations

from os import getenv
from typing import Any, Optional
from urllib.parse import urlencode

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.results import UpdateResult

from infra.query import Query


class MongoRepository:
    """
    A client-side representation of a MongoDB repository.
    """

    __version__ = "1.0.0"
    _client: MongoClient = None
    _database: Database = None
    _connection_options: dict = {
        "authSource": "admin",
        "readPreference": "secondaryPreferred",
        "retryWrites": "true",
        "w": "majority",
    }

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super(MongoRepository, cls).__new__(cls)
        return cls._instance

    def __init__(
        self,
        protocol: str = None,
        user: str = None,
        password: str = None,
        host: str = None,
        database: str = None,
        connection_string: dict = None,
    ) -> None:
        self.__protocol: str = protocol or getenv("MONGO_DB_PROTOCOL")
        self.__user: str = user or getenv("MONGO_DB_USER")
        self.__password: str = password or getenv("MONGO_DB_PASSWORD")
        self.__dns: str = host or getenv("MONGO_DB_DNS")
        self.__database: str = database or getenv("MONGO_DB_NAME")
        if connection_string:
            self._connection_options.update(connection_string)
        self.__init_client_and_database()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_client(self) -> MongoClient:
        return self._client

    def get_database(self) -> Database:
        return self._database

    def get_collection(self, collection: str) -> Collection:
        return self._database[collection]

    def __create_database_uri(self) -> str:
        """
        Creates the DB's URI.
        :return: The DB's URI
        :rtype: str
        """
        return (
            f"{self.__protocol}://{self.__user}:{self.__password}"
            f"@{self.__dns}/{self.__database}"
            f"?{urlencode(self._connection_options)}"
        )

    def __init_client_and_database(self) -> None:
        """
        Creates a new client and DB's session.
        :return:
        :rtype: None
        """
        self._client = MongoClient(self.__create_database_uri(), connect=False)
        self._database = self._client[self.__database]

    def close(self) -> None:
        """
        Cleanup client resources and disconnects from DB.
        :return:
        :rtype: None
        """
        self._client.close()

    def get(self, collection: str, document_id: str) -> Optional[Any]:
        """
        Gets a specific resource from the DB.

        :param collection: The resource's collection's name
        :type collection: str
        :param document_id: The resource's ID
        :type document_id: str
        :return: The retrieved resource if found
        :rtype: object
        """
        q = Query(self._database, collection)
        return q.get_by_id(document_id)

    def create(self, collection: str, document_data: dict) -> None:
        """
        Creates a resource in the DB.

        :param collection: The resource's collection's name
        :type collection: str
        :param document_data: The resource's data
        :type document_data: dict
        :return:
        :rtype: None
        """
        _collection: Collection = self._database[collection]
        _collection.insert_one(document_data)

    def update(self, collection: str, document_id: str, document_data: dict) -> int:
        """
        Updates a resource in the DB.

        :param collection: The resource's collection's name
        :type collection: str
        :param document_id: The resource's ID
        :type document_id: str
        :param document_data: The resource's data
        :type document_data: dict
        :return: Number of docs modified
        :rtype: int
        """
        collection_obj: Collection = self._database[collection]
        result: UpdateResult = collection_obj.update_one(
            {"_id": document_id}, {"$set": document_data}
        )
        return result.modified_count

    def set(self, collection: str, document_id: str, document_data: dict) -> int:
        """
        Sets a resource in the DB.

        :param collection: The resource's collection's name
        :type collection: str
        :param document_id: The resource's ID
        :type document_id: str
        :param document_data: The resource's data
        :type document_data: dict
        :return: Number of docs modified
        :rtype: int
        """
        collection_obj: Collection = self._database[collection]
        result: UpdateResult = collection_obj.update_one(
            {"_id": document_id}, {"$set": document_data}, upsert=True
        )
        return result.modified_count

    def query(self, collection: str, *, verbose: bool = False) -> Query:
        """
        Creates a Query object for running queries in the DB.

        :param collection: The query's collection name
        :param verbose: verbose?
        :type collection: str
        :return: The Query object
        :rtype: Query
        """
        return Query(self._database, collection, verbose=verbose)
