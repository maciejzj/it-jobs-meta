import functools
from typing import Optional

from geopy.geocoders import Nominatim


class Geolocator:
    geolocator = Nominatim(user_agent='it-jobs-meta')

    @functools.cache
    def __call__(
        self, city_name: str, only_from_poland=True
    ) -> tuple[Optional[str], Optional[float], Optional[float]]:
        return self.get_universal_city_name_lat_lon(city_name)

    @classmethod
    def get_universal_city_name_lat_lon(
        cls, city_name: str, only_from_poland=True
    ) -> tuple[Optional[str], Optional[float], Optional[float]]:

        location = cls.geolocator.geocode(city_name)

        if location is None:
            return Geolocator.make_none_location()

        city_name, country_name = Geolocator.address_str_to_city_country_name(
            location.address
        )

        if only_from_poland and country_name != "Polska":
            return Geolocator.make_none_location()

        return city_name, location.latitude, location.longitude

    @staticmethod
    def address_str_to_city_country_name(address: str) -> tuple[str, str]:
        split_loc = address.split(",")
        city_name, country_name = split_loc[0].strip(), split_loc[-1].strip()
        return city_name, country_name

    @staticmethod
    def make_none_location() -> tuple[None, None, None]:
        return None, None, None
