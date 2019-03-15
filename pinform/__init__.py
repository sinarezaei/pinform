import datetime
from pandas import DataFrame
from typing import Dict, Any, Tuple, List, Optional
from .fields import Field
from .tags import Tag
from collections import defaultdict
import six
import re
from .utils import dromedary_to_underline, underline_to_dromedary


name = "pinform"


class MeasurementMeta(type):
    # noinspection PyInitNewSignature,PyUnresolvedReferences,PyTypeChecker,SpellCheckingInspection,PyMethodParameters
    def __new__(cls, name, bases, attrs: Dict[str, Any]):
        m_module = attrs.pop('__module__')
        new_attrs = {'__module__': m_module}
        class_cell = attrs.pop('__classcell__', None)
        if class_cell is not None:
            new_attrs['__classcell__'] = class_cell

        new_class = super(MeasurementMeta, cls).__new__(cls, name, bases, new_attrs)

        attr_meta = attrs.pop('Meta', None)
        if not attr_meta:
            meta = getattr(new_class, 'Meta', None)
        else:
            meta = attr_meta

        measurement_name = getattr(meta, 'measurement_name', None)
        if measurement_name is None:
            measurement_name = dromedary_to_underline(name)
        setattr(new_class, 'measurement_name', measurement_name)

        for field_name, field in attrs.items():
            if isinstance(field, Field):
                if field.name is None:
                    field.name = field_name
                setattr(new_class, field.name, field)
            elif isinstance(field, Tag):
                if field.name is None:
                    field.name = field_name
                setattr(new_class, field.name, field)
            else:
                setattr(new_class, field_name, field)

        # noinspection PyUnusedLocal
        def my_custom_init(instance_self, time_point: datetime.datetime, *init_args, **init_kwargs):
            instance_self._data = {}  # dict.fromkeys(attrs.keys())
            instance_self.time_point = time_point
            tmp_class_dict = instance_self.__class__.__dict__
            model_field_names = [k for k in tmp_class_dict.keys() if isinstance(tmp_class_dict.get(k), Field)]
            model_tag_names = [k for k in tmp_class_dict.keys() if isinstance(tmp_class_dict.get(k), Tag)]
            for key, value in init_kwargs.items():
                if key in model_field_names:
                    setattr(instance_self, key, value)
                elif key in model_tag_names:
                    setattr(instance_self, key, value)
                elif key == "time_point":
                    if isinstance(value, datetime.datetime):
                        setattr(instance_self, key, value)
                    else:
                        raise Exception("time_point given is not instance of datetime.datetime  given value:" + str(
                            value) + " type(value):" + str(type(value)))
                else:
                    raise Exception("value given in instance initialization but was not defined in model as Tag or Field. key:" + str(key) +
                                    " val:" + str(value) + " type(value):" + str(type(value)))
        new_class.__init__ = my_custom_init
        return new_class


class Measurement(six.with_metaclass(MeasurementMeta)):
    measurement_name = "Measurement"

    # noinspection PyUnusedLocal
    def __init__(self, time_point: datetime.datetime, *args, **kwargs):
        super(Measurement, self).__init__()
        field_names = []
        tag_names = []
        _setattr = setattr

        # Note: maybe ? save all timestamps as utc in database. Convert them to appropriate timezones when needed in python.
        #       By default, InfluxDB stores and returns timestamps in UTC.
        #
        # influx has timezones:
        # https://docs.influxdata.com/influxdb/v1.7/query_language/data_exploration/#the-time-zone-clause
        # I tested and it supports timezones. But there is something wrong with python client.
        # >>> If I give a datetime with timezone to python client, it still stores it in db with utc.
        #     the time is not wrong but it when u query that data point again,
        #     it does not give it with the timezone we gave it in first place.
        #
        #
        # I think it is better to save times in utc and then convert it on client side (python).
        # because when I insert a point from python influx client with timezone offset (+03:30 for example),
        # it gets saved in the database as a utc. see:
        # https://stackoverflow.com/questions/39736238/how-to-set-time-zone-in-influxdb-using-python-client
        #
        self.time_point = time_point

        fields_iter = Measurement.get_fields(type(self))
        for f_name, field in fields_iter.items():
            if not field.null and kwargs.get(f_name, None) is None:
                raise ValueError("Null value passed for non-nullable field " + str(f_name))
            field_names.append(f_name)

        tags_iter = Measurement.get_tags(type(self))
        for t_name, tag in tags_iter.items():
            if not tag.null and kwargs.get(t_name, None) is None:
                raise ValueError("Null value passed for non-nullable tag " + str(t_name))
            tag_names.append(t_name)

        if kwargs:
            for prop in tuple(kwargs):
                if prop in field_names:
                    _setattr(self, prop, kwargs[prop])
                    del kwargs[prop]
                if prop in tag_names:
                    _setattr(self, prop, kwargs[prop])
                    del kwargs[prop]

        if kwargs:
            raise TypeError("'%s' is an invalid keyword argument for this function" % list(kwargs)[0])

    @staticmethod
    def get_field_names(cls) -> List[str]:
        field_names_list = []
        type_dicts = cls.__dict__  # type(self).__dict__
        for name, field in type_dicts.items():
            if isinstance(field, Field):
                field_names_list.append(field.name)
        return field_names_list

    @staticmethod
    def get_tag_names(cls) -> List[str]:
        tag_names_list = []
        type_dicts = cls.__dict__
        for name, field in type_dicts.items():
            if isinstance(field, Tag):
                tag_names_list.append(field.name)
        return tag_names_list

    @staticmethod
    def get_fields(cls) -> Dict[str, Field]:
        fields_dict = {}
        type_dicts = cls.__dict__  # type(self).__dict__
        for name, field in type_dicts.items():
            if isinstance(field, Field):
                field_name = field.name
                fields_dict[field_name] = field
        return fields_dict

    def get_field_values_as_dict(self) -> Dict[str, Any]:
        fields_dict = {}
        type_dicts = type(self).__dict__
        for name, field in type_dicts.items():
            if isinstance(field, Field):
                field_name = field.name
                field_value = self.__getattribute__(field_name)
                fields_dict[field_name] = field_value
        return fields_dict

    def get_fields_and_field_values_as_dict(self) -> Dict[str, Tuple[Field, Any]]:
        fields_dict = {}
        type_dicts = type(self).__dict__
        for name, field in type_dicts.items():
            if isinstance(field, Field):
                field_name = field.name
                field_value = self.__getattribute__(field_name)
                fields_dict[field_name] = (field, field_value)
        return fields_dict

    @staticmethod
    def get_tags(cls) -> Dict[str, Tag]:
        tags_dict = {}
        type_dicts = cls.__dict__  # type(self).__dict__
        for name, tag in type_dicts.items():
            if isinstance(tag, Tag):
                tag_name = tag.name if tag.name is not None else name
                tags_dict[tag_name] = tag
        return tags_dict

    def get_tag_values_as_dict(self) -> Dict[str, Any]:
        tags_dict = {}
        type_dicts = type(self).__dict__
        for name, tag in type_dicts.items():
            if isinstance(tag, Tag):
                tag_name = tag.name  # tag.name if tag.name is not None else item[0]
                tag_value = self.__getattribute__(tag_name)
                tags_dict[tag_name] = tag_value
        return tags_dict

    def get_tags_and_tag_values_as_dict(self) -> Dict[str, Tuple[Tag, Optional[str]]]:
        tags_dict = {}
        type_dicts = type(self).__dict__
        for name, tag in type_dicts.items():
            if isinstance(tag, Tag):
                tag_name = tag.name  # tag.name if tag.name is not None else item[0]
                tag_value = self.__getattribute__(tag_name)
                tags_dict[tag_name] = (tag, tag_value)
        return tags_dict

    @staticmethod
    def get_name(cls, name_resolution_tags: Dict[str, str]=None) -> str:
        measurement_name = cls.measurement_name
        name_tags = re.findall('\((.*?)\)', measurement_name)
        for name_tag in name_tags:
            if name_resolution_tags is None:
                raise Exception('Name resolution needs a tag named ' + name_tag + ' but null name resolution tags is provided')
            if name_tag in name_resolution_tags.keys():
                measurement_name.replace('('+name_tag+')', name_resolution_tags[name_tag])
            else:
                raise Exception('Tag with name '+name_tag+' not provided in name resolution tags for resolving dynamic measurement name')
        return measurement_name

    def get_measurement_name(self) -> str:
        return Measurement.get_name(type(self), name_resolution_tags=self.get_tag_values_as_dict())

    def get_cli_format(self) -> Dict[str, Any]:

        fields_iter = self.get_fields_and_field_values_as_dict()
        for f_name, (field, field_value) in fields_iter.items():
            if not field.null and field_value is None:
                raise ValueError("Null value passed for non-nullable field " + field.name)

        tags_iter = self.get_tags_and_tag_values_as_dict()
        for f_name, (tag, tag_value) in tags_iter.items():
            if not tag.null and tag_value is None:
                raise ValueError("Null value passed for non-nullable tag " + tag.name)

        tags_dict = self.get_tag_values_as_dict()
        measurement_name = Measurement.get_name(type(self), name_resolution_tags=tags_dict)
        return {
            "measurement": measurement_name,
            "tags": tags_dict,
            "time": str(self.time_point),
            "fields": self.get_field_values_as_dict()
        }


class MeasurementUtils:

    @staticmethod
    def field_to_dataframe_column_name(field_name: str) -> str:
        return underline_to_dromedary(field_name)

    @staticmethod
    def dataframe_column_to_field_name(column_name: str) -> str:
        return dromedary_to_underline(column_name)

    @staticmethod
    def to_dataframe(items: List[Measurement]) -> DataFrame:
        if len(items) == 0:
            return DataFrame()
        item0 = items[0]
        type_error = "Items passed to create dataframe must be of measurement type"
        assert isinstance(item0, Measurement), type_error
        item_type = type(items[0])
        for item in items:
            if not isinstance(item, Measurement):
                raise Exception(type_error)
            if type(item) != item_type:
                raise Exception("Items passed to create dataframe must have same type")
        m_fields = Measurement.get_fields(type(item0))
        m_tags = Measurement.get_tags(type(item0))

        field_names = []
        tag_names = []
        column_names = []
        for f_name, field in m_fields.items():
            column_names.append(MeasurementUtils.field_to_dataframe_column_name(field.name))
            field_names.append(field.name)
        for t_name, tag in m_tags.items():
            column_names.append(MeasurementUtils.field_to_dataframe_column_name(tag.name))
            tag_names.append(tag.name)

        data_points = defaultdict(list)
        for item in items:
            item_fields = item.get_field_values_as_dict()
            item_tags = item.get_tag_values_as_dict()

            data_points["time_point"].append(item.time_point)
            for f_name in field_names:
                data_points[f_name].append(item_fields.get(f_name))
            for t_name in tag_names:
                data_points[t_name].append(item_tags.get(t_name))

        df_result = DataFrame.from_dict(data=data_points, orient='columns')
        df_result.set_index("time_point", drop=True, inplace=True)
        df_result.columns = [MeasurementUtils.field_to_dataframe_column_name(col) for col in df_result.columns]
        return df_result

    @staticmethod
    def from_dataframe(df: DataFrame, cls: type) -> List[Measurement]:
        assert df is not None, "Null DataFrame passed to create list of measurements"
        measurements = []
        m_fields = Measurement.get_fields(cls=cls)
        m_tags = Measurement.get_tags(cls=cls)

        field_names = []
        tag_names = []

        for field_name, f in m_fields.items():
            field_names.append(field_name)
        for tag_name, t in m_tags.items():
            tag_names.append(tag_name)

        for index, row in df.iterrows():
            assert isinstance(index, datetime.datetime), 'Invalid index type, must be datetime'
            data_points = {}
            for f_name in field_names:
                data_points[f_name] = row[MeasurementUtils.field_to_dataframe_column_name(f_name)]
            for t_name in tag_names:
                data_points[t_name] = row[MeasurementUtils.field_to_dataframe_column_name(t_name)]
            data_points['time_point'] = index
            measurements.append(cls(**data_points))

        return measurements
