from __future__ import annotations

import logging
from copy import deepcopy
from typing import Any, List, Optional, Tuple, TypeVar, Union

from pymongo import ASCENDING, DESCENDING
from pymongo.collection import Collection, Cursor
from pymongo.database import Database

DatabaseT = TypeVar("DatabaseT", bound="Database")
CollectionT = TypeVar("CollectionT", bound="Collection")
QueryT = TypeVar("QueryT", bound="Query")


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
