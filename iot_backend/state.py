"""Runtime state for ESP32 device control and automation."""

from pydantic import BaseModel
from iot_backend.config import FAN_ON_TEMP, LAMP_ON_TEMP, FOG_ON_HUMIDITY, FOG_OFF_HUMIDITY


COMMANDS = {
    "fan_on": "1",
    "fan_off": "2",
    "fog_on": "3",
    "fog_off": "4",
    "lamp_on": "5",
    "lamp_off": "6",
}


class DeviceState(BaseModel):
    fan: bool = False
    fog: bool = False
    lamp: bool = False
    auto: bool = True


class RuntimeState:
    def __init__(self):
        self.devices = DeviceState(auto=True)

    def set_fan_state(self, enabled: bool):
        self.devices.fan = bool(enabled)

    def set_fog_state(self, enabled: bool):
        self.devices.fog = bool(enabled)

    def apply_auto(self, temperature: float, humidity: float):
        if not self.devices.auto:
            return
        self.set_fan_state(temperature >= FAN_ON_TEMP)
        self.devices.lamp = bool(temperature <= LAMP_ON_TEMP)
        if humidity <= FOG_ON_HUMIDITY:
            self.set_fog_state(True)
        elif humidity >= FOG_OFF_HUMIDITY:
            self.set_fog_state(False)

    def set_manual(self, fan=None, mist=None, fog=None, lamp=None, auto=None):
        if auto is not None:
            self.devices.auto = bool(auto)
        if self.devices.auto:
            return
        if fan is not None:
            self.set_fan_state(bool(fan))
        if fog is not None:
            self.set_fog_state(bool(fog))
        if mist is not None:
            self.set_fog_state(bool(mist))
        if lamp is not None:
            self.devices.lamp = bool(lamp)

    def response(self):
        return {
            "state": self.devices.model_dump(),
            "commands": {
                "fan": COMMANDS["fan_on"] if self.devices.fan else COMMANDS["fan_off"],
                "fog": COMMANDS["fog_on"] if self.devices.fog else COMMANDS["fog_off"],
                "lamp": COMMANDS["lamp_on"] if self.devices.lamp else COMMANDS["lamp_off"],
            },
        }


runtime_state = RuntimeState()
