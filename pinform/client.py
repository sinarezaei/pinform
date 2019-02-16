from influxdb import InfluxDBClient
from . import Measurement, MeasurementUtils
from typing import List, Type, Optional, Dict, Union, Tuple, TypeVar, Generic
import logging
import pytz
from pandas import DataFrame, Series
import datetime
import rfc3339
import dateutil
import traceback
from enum import Enum
import re

logger = logging.getLogger('kappa')
T = TypeVar('T', bound=Measurement)


class FillMode(Enum):
    NULL = 1
    PREVIOUS = 2
    NUMBER = 3
    NONE = 4
    LINEAR = 5

    def get_str(self) -> str:
        if self == FillMode.NULL:
            return 'null'
        elif self == FillMode.PREVIOUS:
            return 'previous'
        elif self == FillMode.NUMBER:
            return 'number'
        elif self == FillMode.NONE:
            return 'none'
        elif self == FillMode.LINEAR:
            return 'linear'
        else:
            raise Exception("Invalid enum value")


class AggregationMode(Enum):
    NONE = 1
    MEAN = 2
    MEDIAN = 3
    COUNT = 4
    MIN = 5
    MAX = 6
    SUM = 7
    FIRST = 8
    LAST = 9
    SPREAD = 10
    STDDEV = 11

    def get_str(self) -> str:
        if self == AggregationMode.NONE:
            return ''
        elif self == AggregationMode.MEAN:
            return 'mean'
        elif self == AggregationMode.MEDIAN:
            return 'median'
        elif self == AggregationMode.COUNT:
            return 'count'
        elif self == AggregationMode.MIN:
            return 'min'
        elif self == AggregationMode.MAX:
            return 'max'
        elif self == AggregationMode.SUM:
            return 'sum'
        elif self == AggregationMode.FIRST:
            return 'first'
        elif self == AggregationMode.LAST:
            return 'last'
        elif self == AggregationMode.SPREAD:
            return 'spread'
        elif self == AggregationMode.STDDEV:
            return 'stddev'
        else:
            raise Exception("Invalid enum value")

    def get_result_field_name(self, field_name: str):
        if self == AggregationMode.NONE:
            return field_name
        else:
            return self.get_str() + '_' + field_name

    def aggregate_field(self, field_name: str) -> str:
        if self == AggregationMode.NONE:
            return field_name
        else:
            str_value = self.get_str()
            return str_value + '(' + field_name + ') AS ' + str_value + '_' + field_name


class AggregationTimeUnit(Enum):
    SECOND = 1
    MINUTE = 2
    HOUR = 3
    DAY = 4

    @staticmethod
    def from_str(unit_str: str):
        if unit_str == 'm':
            return AggregationTimeUnit.MINUTE
        elif unit_str == 'd':
            return AggregationTimeUnit.DAY
        elif unit_str == 'h':
            return AggregationTimeUnit.HOUR
        elif unit_str == 's':
            return AggregationTimeUnit.SECOND
        else:
            raise Exception("invalid time unit " + str(unit_str))


class AggregationWindowIndex(Enum):
    START = 1
    CENTER = 2
    END = 3

    def get_time_point_of_window(self, window_start_time: datetime.datetime, group_by_time_str: str) -> datetime.datetime:
        if self == AggregationWindowIndex.START:
            return window_start_time
        else:
            value, unit = AggregationWindowIndex.get_value_and_unit(group_by_time_str=group_by_time_str)
            if self == AggregationWindowIndex.CENTER:
                if unit == AggregationTimeUnit.DAY:
                    return window_start_time + datetime.timedelta(days=value / 2.0)
                elif unit == AggregationTimeUnit.HOUR:
                    return window_start_time + datetime.timedelta(hours=value / 2.0)
                elif unit == AggregationTimeUnit.MINUTE:
                    return window_start_time + datetime.timedelta(minutes=value / 2.0)
                elif unit == AggregationTimeUnit.SECOND:
                    return window_start_time + datetime.timedelta(seconds=value / 2.0)
                else:
                    raise Exception('cannot find center point of time window for time unit ' + str(unit))
            elif self == AggregationWindowIndex.END:
                if unit == AggregationTimeUnit.DAY:
                    return window_start_time + datetime.timedelta(days=value)
                elif unit == AggregationTimeUnit.HOUR:
                    return window_start_time + datetime.timedelta(hours=value)
                elif unit == AggregationTimeUnit.MINUTE:
                    return window_start_time + datetime.timedelta(minutes=value)
                elif unit == AggregationTimeUnit.SECOND:
                    return window_start_time + datetime.timedelta(seconds=value)
                else:
                    raise Exception('cannot find center point of time window for time unit ' + str(unit))
            else:
                raise Exception('get_time_point_of_window not defined for AggregationWindowIndex ' + str(self))

    @staticmethod
    def get_value_and_unit(group_by_time_str: str) -> Tuple[int, AggregationTimeUnit]:
        unit = group_by_time_str[-1]
        return int(group_by_time_str[0:len(group_by_time_str) - 1]), AggregationTimeUnit.from_str(unit)


def parse_influx_str_time(time_str: str, tz: datetime.tzinfo = pytz.utc) -> datetime.datetime:
    return dateutil.parser.parse(time_str).astimezone(tz)


class InfluxClient:

    def __init__(self, host: str = "localhost", port: int = 8086, username: str = None, password: str = None, database_name: str = 'default'):
        self.database_name = database_name

        self.db_client = InfluxDBClient(database=self.database_name, host=host, port=port, username=username, password=password)
        try:
            self.db_client.create_database(dbname=self.database_name)
        except:
            logger.debug(traceback.format_exc())

    def save_points(self, items: List[T]) -> bool:
        items_list = []
        for item in items:
            items_list.append(item.get_cli_format())
        return self.db_client.write_points(items_list)

    def save_dataframe(self, df: DataFrame, measurement_type: Type[T]) -> bool:
        points = MeasurementUtils.from_dataframe(df, measurement_type)
        return self.save_points(points)

    def load_points(self, measurement_type: Type[T], tags: Optional[Dict[str, str]] = None,
                    time_range: Union[datetime.date, Tuple[datetime.datetime, datetime.datetime]] = None,
                    limit: Optional[int] = None) -> List[T]:
        # noinspection SqlNoDataSourceInspection
        query_string = "SELECT * FROM {measurement_name}".format(measurement_name=Measurement.get_name(measurement_type, name_resolution_tags=tags))

        and_conditions_list = []
        if tags is not None:
            for tag_name, tag_value in tags.items():
                and_conditions_list.append(""""{tag_name}"='{tag_value}'""".format(tag_name=tag_name, tag_value=tag_value))

        if time_range is not None:
            if isinstance(time_range, datetime.date):
                and_conditions_list.append(
                    """time >= '{day_start}' and time < '{nex_day_start}'""".format(
                        day_start=rfc3339.format(time_range, use_system_timezone=False),
                        nex_day_start=rfc3339.format(time_range + datetime.timedelta(days=1), use_system_timezone=False)))
            else:
                if time_range[0] is not None:
                    and_conditions_list.append("""time >= '{since_dt}'""".format(since_dt=rfc3339.format(time_range[0], use_system_timezone=False)))
                if time_range[1] is not None:
                    and_conditions_list.append("""time <= '{until_dt}'""".format(until_dt=rfc3339.format(time_range[1], use_system_timezone=False)))

        if len(and_conditions_list) > 0:
            query_string += " WHERE " + (" AND ".join(and_conditions_list))

        if limit is not None:
            query_string += " LIMIT {limit}".format(limit=limit)
        query_string += ';'

        measurement_tags = Measurement.get_tags(cls=measurement_type)
        measurement_fields = Measurement.get_fields(cls=measurement_type)

        result = [p for p in self.db_client.query(query_string).get_points()]

        measurements_list = []
        field_names = []
        tag_names = []

        for field_name, f in measurement_fields.items():
            field_names.append(field_name)
        for tag_name, t in measurement_tags.items():
            tag_names.append(tag_name)

        for item in result:
            data_points = {}
            for f_name in field_names:
                data_points[f_name] = item.get(f_name)
            for t_name in tag_names:
                data_points[t_name] = item.get(t_name)
            data_points['time_point'] = parse_influx_str_time(item.get('time'))
            # noinspection PyCallingNonCallable
            measurements_list.append(measurement_type(**data_points))

        return measurements_list

    def load_points_as_dataframe(self, measurement: Type[T], tags: Optional[Dict[str, str]] = None,
                                 time_range: Union[datetime.date, Tuple[datetime.datetime, datetime.datetime]] = None,
                                 limit: Optional[int] = None) -> DataFrame:
        return MeasurementUtils.to_dataframe(self.load_points(measurement, tags, time_range, limit))

    def get_fields_as_series(self, measurement: Type[T], field_aggregations: Dict[str, Optional[List[AggregationMode]]],
                             tags: Optional[Dict[str, str]] = None, group_by_time_interval: Optional[str] = None,
                             fill_mode: Optional[FillMode] = None, fill_number: Optional[int] = None,
                             window_index_location: AggregationWindowIndex = AggregationWindowIndex.START,
                             time_range: Union[datetime.date, Tuple[datetime.datetime, datetime.datetime]] = None,
                             limit: Optional[int] = None, tz: datetime.tzinfo = pytz.utc) -> Dict[str, Series]:
        if field_aggregations is None or len(field_aggregations.items()) == 0:
            raise Exception('Null or invalid field aggregations')

        if fill_mode is not None and fill_mode == FillMode.NUMBER:
            assert fill_number is not None, 'Null fill number passed with number fill mode'
        else:
            assert fill_number is None, 'Fill number passed with non-number fill mode'

        group_by_time_regex = re.compile('^[1-9][0-9]*[dhms]$')
        assert group_by_time_interval is None or bool(group_by_time_regex.match(group_by_time_interval)), \
            'Invalid group by time ' + str(group_by_time_interval) + ', needs to be a positive integer and either of [dhms]'

        query_string = "SELECT "

        measurement_name = Measurement.get_name(measurement, name_resolution_tags=tags)
        fields = Measurement.get_fields(measurement)

        aggregated_field_names = []
        properties = []
        for field_name, aggregation_modes in field_aggregations.items():
            if field_name not in fields:
                raise Exception('Field name ' + str(field_name) + ' not found in measurement ' + measurement_name + ' fields')
            if aggregation_modes is None or len(aggregation_modes) == 0:
                properties.append(field_name)
                aggregated_field_names.append(field_name)
            else:
                for aggregation_mode in aggregation_modes:
                    properties.append(aggregation_mode.aggregate_field(field_name=field_name))
                    aggregated_field_names.append(aggregation_mode.get_result_field_name(field_name))

        query_string += ', '.join(properties)
        query_string += " FROM {measurement_name}".format(measurement_name=measurement_name)

        and_conditions_list = []
        if tags is not None:
            for tag_name, tag_value in tags.items():
                and_conditions_list.append(""""{tag_name}"='{tag_value}'""".format(tag_name=tag_name, tag_value=tag_value))

        if time_range is not None:
            if isinstance(time_range, datetime.date):
                and_conditions_list.append(
                    """time >= '{day_start}' and time < '{nex_day_start}'""".format(
                        day_start=rfc3339.format(time_range, use_system_timezone=False),
                        nex_day_start=rfc3339.format(time_range + datetime.timedelta(days=1), use_system_timezone=False)))
            else:
                if time_range[0] is not None:
                    and_conditions_list.append("""time >= '{since_dt}'""".format(since_dt=rfc3339.format(time_range[0], use_system_timezone=False)))
                if time_range[1] is not None:
                    and_conditions_list.append("""time <= '{until_dt}'""".format(until_dt=rfc3339.format(time_range[1], use_system_timezone=False)))

        if len(and_conditions_list) > 0:
            query_string += " WHERE " + (" AND ".join(and_conditions_list))

        if group_by_time_interval is not None:
            query_string += " GROUP BY time({time_interval})".format(time_interval=group_by_time_interval)

        if limit is not None:
            query_string += " LIMIT {limit}".format(limit=limit)

        if fill_mode is not None:
            if fill_mode == FillMode.NUMBER:
                query_string += " FILL(" + str(fill_number) + ")"
            else:
                query_string += " FILL(" + fill_mode.get_str() + ")"

        points = [p for p in self.db_client.query(query_string).get_points()]
        if group_by_time_interval is not None:
            times = [window_index_location.get_time_point_of_window(
                parse_influx_str_time(p.get('time'), tz), str(group_by_time_interval)) for p in points]
        else:
            times = [parse_influx_str_time(p.get('time'), tz) for p in points]

        result_dict = {}
        for aggregated_field_name in aggregated_field_names:
            result_dict[aggregated_field_name] = Series(data=[p.get(aggregated_field_name) for p in points], index=times)

        return result_dict

    def get_distinct_existing_tag_values(self, tag_name: str, measurement: Optional[Type[T]] = None, name_resolution_tags: Dict[str, str] = None):
        """
        This function returns the list of existing tag values inside db.

        If a MeasurementClass is specified, the search will be limited to respective measurement_name

        :param measurement: optional measurement class
        :param tag_name: name of tag
        :param name_resolution_tags: tags for finding name of measurement with dynamic name
        :return: list of tag values
        """
        # https://docs.influxdata.com/influxdb/v1.7/query_language/schema_exploration/#show-tag-values
        query_string = "show tag values" + ("" if measurement is None else (" from " + Measurement.get_name(measurement, name_resolution_tags=name_resolution_tags))) \
                       + " " + ('with key = "{tag_name}"'.format(tag_name=tag_name))

        result_set = self.db_client.query(query_string)

        tag_values_set = set()
        for item_dict in result_set.get_points():
            tag_values_set.add(item_dict.get("value"))

        return list(tag_values_set)
