import logging

from django.conf import settings
from django.db.models import Model
from django.db.models.loading import get_model
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, F

from .errors import ConfigurationError
from .indexers import ModelIndexer


##
# non-indexer searching -- just search and dump, useful for catch all search endpoints

class BasicSearcher(object):

    def __init__(self, es: Elasticsearch, index_name: str=None):
        """

        :param es:
        :param index_name:
        :return:
        """
        self.es = es

        if index_name:
            self.index_name = index_name
        elif hasattr(settings, 'ES_INDEX_NAME'):
            self.index_name = settings.ES_INDEX_NAME
        else:
            logging.error('no index name information found')
            raise ConfigurationError('No index name information found')

        super(BasicSearcher, self).__init__()

    def search(self, query: str, filters: [(str, str)]=None) -> [Model]:
        """performs a search against elasticsearch

        :param query:
        :param filters:
        :return:
        """
        # build up search
        s = Search(using=self.es).index(self.index_name).query('match', _all=query)

        # apply filters
        if filters:
            for key, value in filters:
                s = s.filter(F({'term': {key: value}}))

        # execute search
        res = s.execute()

        # build up django query
        results = {}
        for hit in res:
            # get the model
            dj_type = hit._meta.doc_type
            model = get_model(dj_type)

            # get the pk
            pk_name = model._meta.pk.name
            pk = getattr(hit, pk_name)

            # get the score
            score = hit._meta.score

            # add to mapping
            results.setdefault(model, {})
            results[model][pk] = score

        # get queryset
        querysets = []
        for model, pk_score in results.items():
            qs = model.objects.filter(pk__in=pk_score.keys())
            querysets += list(qs)

        # attach scores to instances
        for instance in querysets:
            score = results[type(instance)][instance.pk]
            instance._meta.es_score = score

        # order by score
        querysets = sorted(querysets, key=lambda i: i._meta.es_score, reverse=True)

        # return
        logging.debug('{} hits for search (query={}, filters={})'.format(len(querysets), query, filters))
        return querysets


##
# indexer searching -- useful for when you want to isolate results to specific types

class ModelSearcher(object):

    def __init__(self, indexer: ModelIndexer):
        """initializes a new searcher

        :param indexer: an instance of ModelIndexer
        :return:
        """
        self.indexer = indexer

    def search(self, query: str, filters: dict=None, only_this_type: bool=True, **kwargs: dict) -> list:
        """performs a search against elasticsearch and then pulls the corresponding data from the db

        :param query: query terms to search by
        :param filters: named (attribute, value) filters to limit the query results
        :param kwargs: additional search keyword arguments
        :return: a list of models with an additional `__score` value added
        """
        # build base search object
        s = Search(using=self.indexer.es).index(self.indexer.index_name)
        if only_this_type:
            s = s.doc_type(self.indexer.doc_type_name)

        # build query
        s = s.query('match', _all=query)

        # add filter
        if filters is not None:
            for attr, value in filters.items():
                s = s.filter(F({'term': {attr: value}}))

        # execute query
        res = s.execute()

        # build up django query
        results = {}
        for hit in res:
            # get the model
            dj_type = hit._meta.doc_type
            model = get_model(dj_type)

            # get the pk
            pk_name = model._meta.pk.name
            pk = getattr(hit, pk_name)

            # get the score
            score = hit._meta.score

            # add to mapping
            results.setdefault(model, {})
            results[model][pk] = score

        # get queryset
        querysets = []
        for model, pk_score in results.items():
            qs = model.objects.filter(pk__in=pk_score.keys())
            querysets += list(qs)

        # attach scores to instances
        for instance in querysets:
            score = results[type(instance)][instance.pk]
            instance._meta.es_score = score

        # order by score
        querysets = sorted(querysets, key=lambda i: i._meta.es_score, reverse=True)

        # return
        return querysets
