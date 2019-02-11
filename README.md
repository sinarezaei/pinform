# Pinform: An InfluxDB ORM (OSTM) for Python

PInfORM is an Object/TimeSeries Mapping layer for using [InfluxDB](https://www.influxdata.com/) in python.

Use the following command to install using pip:
```
pip install pinform
```

## Usage example
### Create Measurement Models
First, create your measurement model in 
```
from pinform import Measurement
from pinform.fields import FloatField
from pinform.tags import Tag

class OHLC(Measurement):
  class Meta:
    measurement_name = 'ohlc'

  symbol = Tag(null=False)
  open = FloatField(null=False)
  high = FloatField(null=False)
  low = FloatField(null=False)
  close = FloatField(null=False)
```

### Create InfluxClient
Then you must create an instance of `InfluxClient` to connect to database:
```
from pinform.client import InfluxClient

cli = InfluxClient(host="localhost", port=8086, database_name="defaultdb")
```

If the database needs authentication, use:
```
cli = InfluxClient(host="localhost", port=8086, database_name="defaultdb", username='your db username', password='your db password')
```


### Save and Retrieve Points
To save data in database, use `save_points` or `save_dataframe` functions of InfluxClient:
```
ohlc = OHLC(time_point=datetime.datetime.now(), symbol='AAPL', open=100.6, high=102.5, low=90.4, close=94.2)
cli.save_points([ohlc])
```

To retrieve data from database, use `load_points` or `load_points_as_dataframe` functions of InfluxClient:
```
ohlc_points = cli.load_points(OHLC, {'symbol':'AAPL'})
```

### Get Distinct Tag Values
To get distinct tag values from all measurements, use `get_distinct_existing_tag_values` function from InfluxClient:
```
tag_values = cli.get_distinct_existing_tag_values('symbol')
```

To get distinct tag values from an specific measurements,pass measurement to the previous function:
```
tag_values = cli.get_distinct_existing_tag_values('symbol', measurement=OHLC)
```



## Fields
It's possible to use `IntegerField`, `FloatField`, `BooleanField` and `StringField` to save field values in InfluxDB.
There are four other types of fields which help with storing fields with specific integer or string values. To create a field with multiple choice integer values, use `MultipleChoiceIntegerField` or `EnumIntegerField` classes. To create a field with multiple choice string values, use `MultipleChoiceStringField` or `EnumStringField` classes.

Example for MultipleChoiceStringField:
```
from pinform.fields import MultipleChoiceStringField

class WeatherInfo(Measurement):
  class Meta:
    measurement_name = 'weather_info'
  
  condition = MultipleChoiceStringField(options=['sunny','cloudy','rainy'], null=False)

```

Example for EnumStringField:
```
from enum import Enum
from pinform.fields import EnumStringField

class WeatherCondition(Enum):
  SUNNY = 'sunny'
  CLOUDY = 'cloudy'
  RAINY = 'rainy'


class WeatherInfo(Measurement):
  class Meta:
    measurement_name = 'weather_info'
  
  condition = EnumStringField(enunm=WeatherCondition, null=False)

```



## Advanced usage

### Dynamic measurement names
It is possible to use tags in measurement name wrapped in parenthesis
```
class OHLC(Measurement):
  class Meta:
    measurement_name = 'ohlc_(symbol)'
  
  symbol = Tag(null=False)
  ...
```

### Query Field and Pandas Series
Use `get_fields_as_series` function from InfluxClient to get fields of specific measurement class as Pandas Series. It's also possible to aggregate data and group by time. This function returnes a `dict` with aggregated field names as keys and pandas series as values.
```
series_dict = cli.get_fields_as_series(OHLC, 
                field_aggregations={'close': [AggregationMode.MEAN, AggregationMode.STDDEV]},
                tags={'symbol': 'AAPL'},
                group_by_time_interval='10d')
mean_close_series = series_dict['mean_close']
stddev_close_series = series_dict['stddev_close']
```