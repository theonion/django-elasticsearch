from datetime import date, datetime

from django.utils import timezone


class IndexableField(object):
    """
    maps python to es field types
    """

    _type = None
    _attrs = []

    def __init__(self, source: str, **kwargs: dict):
        """initializes the field and sets internal attributes of the object

        :param source: the django model attribute to source the data from
        :param kwargs: `dict` of mapping information about field mapping attributes
        """
        self.source = source
        for key, value in kwargs.items():
            self._attrs.append(key)
            setattr(self, key, value)

    def define_mapping(self) -> dict:
        """builds an elasticsearch field mapping definition

        :return: the elasticsearch field mapping declaration
        """
        definition = {'type': self._type}
        for attr in self._attrs:
            value = getattr(self, attr, None)
            if value is not None:
                definition[attr] = value
        return definition

    ##
    # static methods

    @staticmethod
    def to_es(value: type) -> type:
        """converts value to elasticsearch type

        :param value: the thing to be indexed
        :return: the value mapped to the acceptable type in elasticsearch
        """
        raise NotImplementedError()

    @staticmethod
    def to_python(value: type) -> type:
        """converts value to python type

        :param value: the thing from the index
        :return: the value mapped back to its original python type
        """
        raise NotImplementedError()


class StringField(IndexableField):
    """
    maps string types
    """

    _type = 'string'

    @staticmethod
    def to_es(value: type) -> str or None:
        if value is None:
            return None
        return str(value)

    @staticmethod
    def to_python(value: type) -> str or None:
        if value is None:
            return None
        return str(value)


class IntegerField(IndexableField):
    """
    maps integer types
    """

    _type = 'integer'

    @staticmethod
    def to_es(value: type) -> int or None:
        if value is None:
            return None
        return int(value)

    @staticmethod
    def to_python(value: type) -> int or None:
        if value is None:
            return None
        return int(value)


class FloatField(IndexableField):
    """
    maps float types
    """

    _type = 'float'

    @staticmethod
    def to_es(value: type) -> float or None:
        if value is None:
            return None
        return float(value)

    @staticmethod
    def to_python(value: type) -> float or None:
        if value is None:
            return None
        return float(value)


class DateField(IndexableField):
    """
    maps date types
    """

    _type = 'date'

    @staticmethod
    def to_es(value: type) -> str:
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        return str(value)

    @staticmethod
    def to_python(value: type) -> datetime or None:
        if isinstance(value, str):
            return datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%f+00:00')\
                .replace(tzinfo=timezone.utc)
        elif isinstance(value, (date, datetime)):
            return value
        else:
            return str(value)


##
# field constants

DJANGO_TO_ES = {
    'AutoField': IntegerField,
    'BigIntegerField': IntegerField,
    'CharField': StringField,
    'DateField': DateField,
    'DateTimeField': DateField,
    'DecimalField': FloatField,
    'EmailField': StringField,
    'FloatField': FloatField,
    'IntegerField': IntegerField,
    'IPAddressField': StringField,
    'GenericIPAddressField': StringField,
    'PositiveIntegerField': IntegerField,
    'PositiveSmallIntegerField': IntegerField,
    'SlugField': StringField,
    'SmallIntegerField': IntegerField,
    'TextField': StringField,
    'URLField': StringField,
}


##
# functions

def get_es_type_mapping(source: str, django_type: str) -> dict or None:
    """maps the django type to an es type and then gets its mapping declaration

    :type source: str
    :param source: the name of the django model field

    :type django_type: str
    :param django_type: the name of the django field type

    :rtype: dict or None
    :return: an elasticsearch field mapping
    """
    es_type = DJANGO_TO_ES.get(django_type, None)
    if es_type is None:
        return None
    return es_type(source).define_mapping()
