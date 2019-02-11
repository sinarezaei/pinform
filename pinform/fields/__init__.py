from enum import Enum
from typing import Optional, List, Union, Set


class FieldType(Enum):
    INTEGER = 1
    FLOAT = 2
    BOOLEAN = 3
    STRING = 4


class Field:

    # noinspection PyProtectedMember
    def __get__(self, instance, owner) -> int:
        if instance is None:
            raise Exception('Cannot access field without instance')
        return instance._data[self.name]

    # noinspection PyProtectedMember
    def __set__(self, instance, value):
        if not self.null:
            assert value is not None, "Null value for not nullable field: " + self.name
        if instance is None:
            raise Exception('Cannot access field without instance')
        instance._data[self.name] = value

    def __init__(self, field_type: FieldType, name: Optional[str] = None, null: bool = True):
        self.field_type = field_type
        self.name = name
        self.null = null


class IntegerField(Field):

    def __set__(self, instance, value):
        if value is not None and not isinstance(value, int):
            raise TypeError(instance, self.name, int, value)
        super().__set__(instance, value)

    def __init__(self, name: Optional[str] = None, null: bool = True):
        super(IntegerField, self).__init__(field_type=FieldType.INTEGER, name=name, null=null)


class FloatField(Field):

    def __set__(self, instance, value):
        if value is not None and not isinstance(value, float):
            raise TypeError(instance, self.name, float, value)
        super().__set__(instance, value)

    def __init__(self, name: Optional[str] = None, null: bool = True):
        super(FloatField, self).__init__(field_type=FieldType.FLOAT, name=name, null=null)


class BooleanField(Field):

    def __set__(self, instance, value):
        if value is not None and not isinstance(value, bool):
            raise TypeError(instance, self.name, bool, value)
        super().__set__(instance, value)

    def __init__(self, name: Optional[str] = None, null: bool = True):
        super(BooleanField, self).__init__(field_type=FieldType.BOOLEAN, name=name, null=null)


class StringField(Field):

    def __set__(self, instance, value):
        if value is not None and not isinstance(value, str):
            raise TypeError(instance, self.name, str, value)
        super().__set__(instance, value)

    def __init__(self, name: Optional[str] = None, null: bool = True):
        super(StringField, self).__init__(field_type=FieldType.STRING, name=name, null=null)


class MultipleChoiceStringField(Field):

    def __set__(self, instance, value):
        if value is not None and not isinstance(value, str):
            raise TypeError(instance, self.name, str, value)
        if value is not None and not (value in self.options):
            raise ValueError('Invalid value '+str(value)+' not present in options ' + str(self.options))
        super().__set__(instance, value)

    def __init__(self, options: Union[List[str], Set[str]], name: Optional[str] = None, null: bool = True):
        super(MultipleChoiceStringField, self).__init__(field_type=FieldType.STRING, name=name, null=null)
        if options is None:
            raise Exception("Null options passed for multiple choice string field")
        if not (isinstance(options, list) or isinstance(options, set)):
            raise Exception("Invalid type for options passed for multiple choice string field, must be either set or list but found " + str(type(options)) )
        if len(options) == 0:
            raise Exception("Empty options passed for enum string field")
        if len(options) != len(set(options)):
            raise Exception("Duplicate values passed for options of multiple choice string field")
        for option in options:
            if not isinstance(option, str):
                raise Exception("Invalid value in options of multiple choice string field, " + str(option) + ', expected str value but found ' + str(type(option)))
        self.options = set(options)


class EnumStringField(Field):

    def __set__(self, instance, value):
        if value is not None and not isinstance(value, str):
            raise TypeError(instance, self.name, str, value)
        if value is not None and not (value in self.options):
            raise ValueError('Invalid value '+str(value)+' not present in enum values ' + str(self.options))
        super().__set__(instance, value)

    def __init__(self, enum, name: Optional[str] = None, null: bool = True):
        super(EnumStringField, self).__init__(field_type=FieldType.STRING, name=name, null=null)
        if enum is None:
            raise Exception("Null enum passed for enum string field")
        if not issubclass(enum, Enum):
            raise Exception("Passed enum class must be a subclass of Enum")
        options = [e.value for e in enum]
        if len(options) == 0:
            raise Exception("Enum with no values for enum string field")
        if len(options) != len(set(options)):
            raise Exception("Duplicate values passed for options of enum string field")
        for option in options:
            if not isinstance(option, str):
                raise Exception("Invalid value in enum string field, " + str(option) + ', expected str value but found ' + str(type(option)))
        self.enum = enum
        self.options = set(options)


class MultipleChoiceIntegerField(Field):

    def __set__(self, instance, value):
        if value is not None and not isinstance(value, int):
            raise TypeError(instance, self.name, int, value)
        if value is not None and not (value in self.options):
            raise ValueError('Invalid value '+str(value)+' not present in options ' + str(self.options))
        super().__set__(instance, value)

    def __init__(self, options: Union[List[int], Set[int]], name: Optional[str] = None, null: bool = True):
        super(MultipleChoiceIntegerField, self).__init__(field_type=FieldType.INTEGER, name=name, null=null)
        if options is None:
            raise Exception("Null options passed for multiple choice integer field")
        if not (isinstance(options, list) or isinstance(options, set)):
            raise Exception("Invalid type for options passed for multiple choice integer field, must be either set or list but found " + str(type(options)))
        if len(options) == 0:
            raise Exception("Empty options passed for multiple choice integer field")
        if len(options) != len(set(options)):
            raise Exception("Duplicate values passed for options of multiple choice integer field")
        for option in options:
            if not isinstance(option, int):
                raise Exception("Invalid value in options of multiple choice integer field, " + str(option) + ', expected int value but found ' + str(type(option)))
        self.options = set(options)


class EnumIntegerField(Field):

    def __set__(self, instance, value):
        if value is not None and not isinstance(value, int):
            raise TypeError(instance, self.name, int, value)
        if value is not None and not (value in self.options):
            raise ValueError('Invalid value '+str(value)+' not present in enum values ' + str(self.options))
        super().__set__(instance, value)

    def __init__(self, enum, name: Optional[str] = None, null: bool = True):
        super(EnumIntegerField, self).__init__(field_type=FieldType.INTEGER, name=name, null=null)
        if enum is None:
            raise Exception("Null enum passed for enum string field")
        if not issubclass(enum, Enum):
            raise Exception("Passed enum class must be a subclass of Enum")
        options = [e.value for e in enum]
        if len(options) == 0:
            raise Exception("Enum with no values passed for enum integer field")
        if len(options) != len(set(options)):
            raise Exception("Duplicate values passed for options of enum integer field")
        for option in options:
            if not isinstance(option, int):
                raise Exception("Invalid value in enum integer field, " + str(option) + ', expected int value but found ' + str(type(option)))
        self.options = set(options)

