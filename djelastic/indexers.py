import logging

from django.conf import settings
from django.db.models import Model
from django.db.models.base import ModelBase
from elasticsearch import Elasticsearch
from six import add_metaclass

from .errors import ConfigurationError
from .fields import IndexableField, get_es_type_mapping


##
# meta class

class IndexerMetaClass(type):
    """
    meta class to set up Indexer objects
    """

    def __new__(mcs, name: str, bases: [object], attributes: dict):
        """sets new attribute `_mapped_fields` of object

        :param name: the object name
        :param bases: base (parent) objects
        :param attributes: object attributes
        :return: super.__new__ -> the object itself
        """
        # get this object's attributes
        fields = []
        for name, obj in list(attributes.items()):
            if isinstance(obj, IndexableField):
                fields.append((name, attributes.pop(name)))

        # descend into base classes. if they are also Indexers, grab and prepend their mapped fields
        for base in bases:
            if hasattr(base, '_mapped_fields'):
                fields = list(base._mapped_fields.items()) + fields

        # set and run super
        attributes['_mapped_fields'] = dict(fields)
        return super(IndexerMetaClass, mcs).__new__(mcs, name, bases, attributes)


##
# objects

@add_metaclass(IndexerMetaClass)
class ModelIndexer(object):
    """
    django model indexer
    """

    def __init__(self, instance: Model=None):
        """initializes a new indexer

        :param instance: an instance of a django model
        """
        # run super to get django model fields mapped in
        super(ModelIndexer, self).__init__()

        # elasticsearch stuff
        self.es = self._get_es()
        self.index_name = self._get_index_name()
        self.doc_type_name = self._get_doc_type_name()
        self.model_pk_name, self.model_pk_type = self._get_model_pk()
        self.mapping = self._make_mapping()

        # make sure the index and the doc type exist
        if not self.es.indices.exists(self.index_name):
            self.es.indices.create(self.index_name)
        if not self.es.indices.exists_type(self.index_name, doc_type=[self.doc_type_name]):
            self.es.indices.put_mapping(doc_type=self.doc_type_name, body=self.mapping, index=self.index_name)

        # set instance
        self.instance = instance

    ##
    # callable methods

    def index(self) -> dict:
        """upserts the document into elasticsearch

        :return: {'_type', 'created', '_version', '_index', '_id'}
        """
        document = self._make_document()
        if len(document):
            doc_id = document[self.model_pk_name]
            res = self.es.index(self.index_name, self.doc_type_name, document, id=doc_id, refresh=True)
            logging.debug('index result: {}'.format(res))
            return res
        logging.debug('index result: None -- there is no document')
        return {'_type': None, 'created': None, '_version': None, '_index': None, '_id': None}

    def delete(self) -> dict:
        """deletes the document from elasticsearch

        :return: {'found', '_type':, '_version''_index', '_id'}
        """
        document = self._make_document()
        if document is not None:
            doc_id = document[self.model_pk_name]
            res = self.es.delete(self.index_name, self.doc_type_name, id=doc_id, refresh=True)
            logging.debug('delete result: {}'.format(res))
            return res
        logging.debug('delete result: None - there is no document')
        return {'found': None, '_type': None, '_version': None, '_index': None, '_id': None}

    ##
    # internal methods

    def _get_es(self) -> Elasticsearch:
        """gets connection to elasticsearch from meta or project settings

        :return: a pooled connection
        :raise ConfigurationError: if no Meta.es property or ES_* properties in django project settings
        """
        # check meta
        if hasattr(self.Meta, 'es'):
            logging.debug('connecting to elasticsearch from passed Meta.es attribute')
            return self.Meta.es

        # create a new connection
        if hasattr(settings, 'ES_HOSTS'):
            if hasattr(settings, 'ES_TRANSPORT'):
                if hasattr(settings, 'ES_KWARGS'):
                    logging.debug('connecting to elasticsearch from django project settings')
                    return Elasticsearch(
                        hosts=settings.ES_HOSTS, transport_class=settings.ES_TRANSPORT, **settings.ES_KWARGS)
                logging.debug('connecting to elasticsearch from django project settings')
                return Elasticsearch(hosts=settings.ES_HOSTS, transport_class=settings.ES_TRANSPORT)
            logging.debug('connecting to elasticsearch from django project settings')
            return Elasticsearch(hosts=settings.ES_HOSTS)

        # raise exception -- no es found
        logging.error('no elasticsearch connection information found')
        raise ConfigurationError('No elasticsearch connection information found')

    def _get_index_name(self) -> str:
        """gets the elasticsearch index name from meta or project settings

        :return: the index name to be used for indexing
        :raise ConfigurationError: if no Meta.index property or ES_INDEX_NAME in django project settings
        """
        # check meta
        if hasattr(self.Meta, 'index'):
            logging.debug('using index {}'.format(self.Meta.index))
            return self.Meta.index

        # check django project settings
        if hasattr(settings, 'ES_INDEX_NAME'):
            logging.debug('using index {}'.format(settings.ES_INDEX_NAME))
            return settings.ES_INDEX_NAME

        # raise exception -- no name found
        logging.error('no index name information found')
        raise ConfigurationError('No index name information found')

    def _get_doc_type_name(self) -> str:
        """gets or creates the name for the elasticsearch doc type -- if created, it's the importable django model name

        :return: the name of the doc type
        :raise ConfigurationError: if no Meta.doc_type property or Meta.model or Meta.model is not a django model
        """
        # check meta
        if hasattr(self.Meta, 'doc_type'):
            logging.debug('using doc type {}'.format(self.Meta.doc_type))
            return self.Meta.doc_type

        # create from django model
        if hasattr(self.Meta, 'model'):
            model = self.Meta.model
            if isinstance(model, ModelBase):
                name = '{}.{}'.format(model._meta.app_label, model._meta.model_name)
                logging.debug('created doc type name {}'.format(name))
                return name

            # raise exception -- model is not a django model
            logging.error('Meta.model object is not an instance of a django model')
            raise ConfigurationError('Meta.model object is not an instance of a django model')

        # raise exception -- no doc type name found
        logging.error('no doc type name information found')
        raise ConfigurationError('No doc type name information found')

    def _get_model_pk(self) -> (str, str):
        """gets the name of the django model's pk and its field's internal type

        :return: the name of the django model's pk and its field's internal type
        :raise ConfigurationError: if no Meta.model or Meta.model is not a django model
        """
        if hasattr(self.Meta, 'model'):
            model = self.Meta.model
            if isinstance(model, ModelBase):
                name = model._meta.pk.name
                internal_type = model._meta.get_field(name).get_internal_type()
                logging.debug('model pk: {}, {}'.format(name, internal_type))
                return name, internal_type

            # raise exception -- not a django model
            logging.error('Meta.model object is not an instance of a django model')
            raise ConfigurationError('Meta.model object is not an instance of a django model')

        # raise exception -- no model found
        logging.error('Meta.model attribute not found')
        raise ConfigurationError('Meta.model attribute not found')

    def _make_mapping(self) -> dict:
        """creates an elasticsearch mapping based on attributes

        :return: the full elasticsearch mapping document mapping
        """
        # build base properties based on declared attributes (self._mapped_fields)
        properties = {}
        for name, field_type in self._mapped_fields.items():
            # check if name is dotted (for FKs, 121s and M2Ms)
            if '.' in name:
                name = name.split('.')[0]
            properties[name] = field_type.define_mapping()

        # add in model pk
        properties[self.model_pk_name] = get_es_type_mapping(self.model_pk_name, self.model_pk_type)

        # check dynamic mapping from meta
        dynamic = getattr(self.Meta, 'dynamic', None)

        # build mapping
        mapping = {
            '_id': {'path': self.model_pk_name},
            'properties': properties,
        }

        # handle mapping and date detection
        if dynamic == 'strict':
            mapping['dynamic'] = 'strict'
        elif dynamic:
            mapping['dynamic'] = dynamic
        else:
            mapping['date_detection'] = False

        # return
        logging.debug('mapping created: {}'.format(mapping))
        return mapping

    def _make_document(self) -> dict:
        """creates an elasticsearch document based on the mapped fields from the init-ed instance

        :return: the full document to be indexed
        """
        document = {}

        # parse instance
        if self.instance:
            for doc_key, field in self._mapped_fields.items():
                # get source name
                name = field.source

                # check if name is dotted (for FKs, 121s and M2Ms)
                if '.' in name:
                    name, attr = name.split('.')[:2]
                    rel_model = getattr(self.instance, name, None)

                    # descend into relationship
                    if rel_model is not None:
                        dj_type = self.Meta.model._meta.get_field(str(name)).get_internal_type()

                        # FK or 121
                        if dj_type in ('ForeignKey', 'OneToOneField', 'ManyToOneRel', 'OneToOneRel'):
                            value = getattr(rel_model, attr, None)

                        # M2M
                        elif dj_type in ('ManyToManyField', 'ManyToManyRel'):
                            values = [getattr(obj, attr, None) for obj in rel_model.all()]
                            value = ' '.join([str(val) for val in values])

                        # WTF?
                        else:
                            value = None

                    else:
                        value = None

                # get value
                else:
                    value = getattr(self.instance, name, None)
                # set value
                document[doc_key] = value

            # add pk to document
            document[self.model_pk_name] = getattr(self.instance, self.model_pk_name, None)

        # return
        logging.debug('document created: {}'.format(document))
        return document
