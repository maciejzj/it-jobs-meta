import functools

from geopy.geocoders import Nominatim


class Geolocator:
    def __init__(self, country_filter: tuple[str, ...] | None = None):
        self._geolocator = Nominatim(user_agent='it-jobs-meta')
        self._country_filter = country_filter

    @functools.cache
    def __call__(
        self, city_name: str
    ) -> tuple[str, float, float] | tuple[None, None, None]:
        return self.get_universal_city_name_lat_lon(city_name)

    def get_universal_city_name_lat_lon(
        self, city_name: str
    ) -> tuple[str, float, float] | tuple[None, None, None]:

        location = self._geolocator.geocode(city_name)

        if location is None:
            return Geolocator.make_none_location()

        city_name, country_name = Geolocator.address_str_to_city_country_name(
            location.address
        )

        filter_ = self._country_filter
        if filter_ is not None and country_name not in filter_:
            return Geolocator.make_none_location()

        return city_name, location.latitude, location.longitude

    @staticmethod
    def address_str_to_city_country_name(address: str) -> tuple[str, str]:
        split_loc = address.split(',')
        city_name, country_name = split_loc[0].strip(), split_loc[-1].strip()
        return city_name, country_name

    @staticmethod
    def make_none_location() -> tuple[None, None, None]:
        return None, None, None
