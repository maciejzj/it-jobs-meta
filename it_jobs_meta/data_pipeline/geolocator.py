"""Geolocation services."""

import functools

from geopy.geocoders import Nominatim


class Geolocator:
    def __init__(self, country_filter: tuple[str, ...] | None = None):
        """Create geolocator instance.

        :param country_filter: Tuple of country names that the geolocation
            should be limited to.
        """
        self._geolocator = Nominatim(user_agent='it-jobs-meta')
        self._country_filter = country_filter

    @functools.cache
    def __call__(
        self, city_name: str
    ) -> tuple[str, float, float] | tuple[None, None, None]:
        """For given city name get it's location.

        :param city_name: Name of the city to geolocate, can be in native
            language or in English, different name variants will be unified on
            return.
        :return: Tuple with location as (unified_city_name, latitude,
            longitude), if geolocation fails or country is not in
            "contry_filters" will return Nones".
        """
        return self.get_universal_city_name_lat_lon(city_name)

    def get_universal_city_name_lat_lon(
        self, city_name: str
    ) -> tuple[str, float, float] | tuple[None, None, None]:
        """Same as __call__."""

        location = self._geolocator.geocode(city_name)

        if location is None:
            return Geolocator._make_none_location()

        city_name, country_name = Geolocator._address_str_to_city_country_name(
            location.address
        )

        filter_ = self._country_filter
        if filter_ is not None and country_name not in filter_:
            return Geolocator._make_none_location()

        return city_name, location.latitude, location.longitude

    @staticmethod
    def _address_str_to_city_country_name(address: str) -> tuple[str, str]:
        split_loc = address.split(',')
        city_name, country_name = split_loc[0].strip(), split_loc[-1].strip()
        return city_name, country_name

    @staticmethod
    def _make_none_location() -> tuple[None, None, None]:
        return None, None, None
