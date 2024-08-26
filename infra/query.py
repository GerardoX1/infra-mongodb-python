from __future__ import annotations

import logging
from copy import deepcopy
from typing import Any, List, Optional, Tuple, TypeVar, Union

from pymongo import ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database

DatabaseT = TypeVar("DatabaseT", bound="Database")
CollectionT = TypeVar("CollectionT", bound="Collection")
QueryT = TypeVar("QueryT", bound="Query")


# TODO check docstrings @all
class Query:
    """
    A client-side representation of a MongoDB Query.
    """

    __filter: dict = None
    __collection: CollectionT = None
    _DEFAULT_QUERY_LIMIT = 500
    ASCENDING_ORDER = ASCENDING
    DESCENDING_ORDER = DESCENDING

    def __init__(
        self,
        db_manager: DatabaseT,
        collection: str,
        *args,
        verbose: bool = False,
        **kwargs,
    ):
        self.__collection = db_manager[collection]
        self.__filter = dict()
        self.verbose = verbose

    @classmethod
    def array_contains_filter_parser(cls, _, conditions: List[tuple]) -> dict:
        parsed_filter = {}
        for field, operator, value in conditions:
            if field in parsed_filter:
                new_condition = cls.method_parser(field, operator, value)
                parsed_filter.get(field).update(**new_condition)
            else:
                parsed_filter.update({field: cls.method_parser(field, operator, value)})
        return {"$elemMatch": parsed_filter}

    @classmethod
    def method_parser(cls, field: str, operator: str, value: Any) -> Any:
        """
        Transforms an operator and a value into Mongo's filter.

        :param operator: filter's operator
        :type operator: str
        :param field: TODO put description
        :param value: filter's value
        :type value: Any
        :return: MongoDB dict filter to be used in a query
        :rtype: Any
        """
        checker = lambda type_, to_validate, successful: (
            successful
            if isinstance(to_validate, type_)
            else logging.error(f"Value must be a {type_}, not {type(to_validate)}")
        )

        conditions_available = {
            "==": lambda _, x: x,
            "!=": lambda _, x: {"$ne": x},
            ">": lambda _, x: {"$gt": x},
            ">=": lambda _, x: {"$gte": x},
            "<": lambda _, x: {"$lt": x},
            "<=": lambda _, x: {"$lte": x},
            "in": lambda _, x: {"$in": x},
            "in_range": lambda _, x: checker(
                dict, x, {"$gte": x["min"], "$lte": x["max"]}
            ),
            "like": lambda _, to_match: {"$regex": to_match},
            "ilike": lambda _, to_match: {"$regex": to_match, "$options": "i"},
            "has_field": lambda _, flag: checker(bool, flag, {"$exists": flag}),
            "as_type": lambda _, type_: {"$type": type_},
            "array_contains": cls.array_contains_filter_parser,
        }
        if operator not in conditions_available:
            logging.error(
                f"Operator {operator} not available. "
                f"Available operators: {conditions_available.keys()}"
            )
            return None
        return conditions_available.get(operator)(field, value)

    def __factory_and_or_nor(
        self, conditions: List[tuple], binary_operator: str
    ) -> None:
        """
        Creates a new filter.

        :param conditions: A list of (field, operator, value) triples specifying the filter
        :type conditions: List[tuple[str, str, Any]]
        :param binary_operator: Filter's operator
        :type binary_operator: Literal['and', 'or', 'nor']
        """
        filter_ = self.__filter.get(f"${binary_operator}", [])
        for field, operator, value in conditions:
            filter_.append({field: self.method_parser(field, operator, value)})
        self.__filter.update({f"${binary_operator}": filter_})

    def and_search(self, conditions: List[tuple]) -> QueryT:
        """
        Updates filter's dict with a new and filter.

        :param conditions: A list of (field, operator, value) triples specifying the filter
        :type conditions: List[tuple[str, str, Any]]
        :return: self
        :rtype: Query
        """
        self.__factory_and_or_nor(conditions, "and")
        return self

    def or_search(self, conditions: List[tuple]) -> QueryT:
        """
        Updates filter's dict with a new or filter.

        :param conditions: A list of (field, operator, value) triples specifying the filter
        :type conditions: List[tuple[str, str, Any]]
        :return: self
        :rtype: Query
        """
        self.__factory_and_or_nor(conditions, "or")
        return self

    def nor_search(self, conditions: List[tuple]) -> QueryT:
        """
        Updates filter's dict with a new nor filter.

        :param conditions: A list of (field, operator, value) triples specifying the filter
        :type conditions: List[tuple[str, str, Any]]
        :return: self
        :rtype: Query
        """
        self.__factory_and_or_nor(conditions, "nor")
        return self

    def search(self, conditions: List[tuple]) -> QueryT:
        """
        Updates filter's dict with a new filter.

        :param conditions: A list of (field, operator, value) triples specifying the filter
        :type conditions: List[tuple[str, str, Any]]
        :return: self
        :rtype: Query
        """
        self.__filter.update(self.parse_filters(conditions))
        return self

    def parse_filters(self, filters: list[Tuple]):
        new_filter = {}
        for field, operator, value in filters:
            if field in new_filter:
                new_condition = self.method_parser(field, operator, value)
                new_filter.get(field).update(**new_condition)
            else:
                new_filter.update({field: self.method_parser(field, operator, value)})
        return new_filter

    def _get_filter(self) -> dict:
        """
        Gets the built filter dictionary.

        :return: Filter's dict that will be used
        :rtype: dict
        """
        if self.verbose:
            logging.warning(f"Query Filter: {self.__filter}")
        return deepcopy(self.__filter)

    def get_by_id(
        self, _id: str, *, projection: Union[list, dict] = None
    ) -> Optional[Any]:
        """
        Gets a specific resource from the DB.

        :param _id: The resource's ID
        :type _id: str
        :param projection: List of fields that should be returned in te result
        :type projection: Union[list, dict]
        :return: The retrieved resource if found
        :rtype: Optional[Any]
        """
        self.__filter = {"_id": _id}
        result = self.get_one_or_none(projection=projection)
        self.__filter = {}
        return result

    def get_all(
        self,
        *,
        sort: Optional[List[Tuple[str, int]]] = None,
        limit: int = _DEFAULT_QUERY_LIMIT,
        projection: Union[list, dict] = None,
    ) -> object:
        """
        Gets all resources that correspond with the filters' list.

        :param sort: A list of (key, direction) pairs specifying the sort order for this query
        :type sort: List[Tuple[str, Union[ASCENDING, DESCENDING]]]
        :param limit: The maximum number of results to return
        :type limit: int
        :param projection: List of fields that should be returned in te result
        :type projection: Union[list, dict]
        :return: The retrieved resources if found
        :rtype: Cursor
        """
        result = self.__collection.find(
            self._get_filter(), sort=sort, limit=limit, projection=projection
        )
        return result

    def get_one_or_none(
        self,
        *,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Union[list, dict] = None,
    ) -> Optional[Any]:
        """
        Gets the first resource that correspond with the filters' list.

        :param sort: A list of (key, direction) pairs specifying the sort order for this query
        :type sort: List[Tuple[str, Union[ASCENDING, DESCENDING]]]
        :param projection: List of fields that should be returned in te result
        :type projection: Union[list, dict]
        :return: The retrieved resource if found
        :rtype: Optional[Any]
        """
        result = self.__collection.find_one(
            self._get_filter(), sort=sort, projection=projection
        )
        return result

    def count(self) -> int:
        """
        Count the number of documents that correspond with the filters' list.

        :return: Number of matched resources
        :rtype: int
        """
        return self.__collection.count_documents(self._get_filter())

    def distinct(self, key: str) -> List[Any]:
        """
        Get a list of distinct values for `key` among all documents
        in this collection.

        :param key: name of the field for which we want to get the distinct values
        :type key: str
        :return: The retrieved values  if found
        :rtype: Cursor
        """
        return self.__collection.distinct(key, self._get_filter())
