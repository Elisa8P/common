from enum import Enum

from ual.influx.influx_buckets import InfluxBuckets


class LUBWSensors(Enum):
    """
    All current in use sensors from lubw
    debw is english abbreviation for lubw
    """
    DEBW015 = "DEBW015"
    DEBW152 = "DEBW152"


class UALSensors(Enum):
    """
    All current in use custom sensors from UrbanAirLab project
    """
    UAL_1 = "ual-1"
    UAL_2 = "ual-2"
    UAL_3 = "ual-3"
    UAL_4 = "ual-4"
    UAL_5 = "ual-5"
    UAL_6 = "ual-6"


class SensorSource:
    def __init__(self, bucket: InfluxBuckets, sensor: UALSensors | LUBWSensors):
        self.bucket: InfluxBuckets = bucket
        self.sensor: UALSensors | LUBWSensors = sensor

    def get_bucket(self) -> str:
        return self.bucket.value

    def get_sensor(self) -> str:
        return self.sensor.value