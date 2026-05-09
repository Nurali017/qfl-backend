"""Tests for the METAR weather provider parser and ICAO mapping."""

from __future__ import annotations

from app.data.icao_mapping import icao_for_city
from app.services.weather.metar import parse_condition
from app.services.weather import format_weather


class TestParseCondition:
    """``parse_condition`` maps METAR ``wxString`` + ``cover`` to our keys."""

    def test_clear_no_phenomena_clear_sky(self):
        assert parse_condition("", "CLR") == "clear"
        assert parse_condition(None, "SKC") == "clear"
        assert parse_condition(None, "NSC") == "clear"
        assert parse_condition(None, "CAVOK") == "clear"

    def test_few_or_scattered_treated_as_clear(self):
        assert parse_condition("", "FEW") == "clear"
        assert parse_condition(None, "SCT") == "clear"

    def test_broken_or_overcast_is_clouds(self):
        assert parse_condition("", "BKN") == "clouds"
        assert parse_condition(None, "OVC") == "clouds"

    def test_light_rain(self):
        assert parse_condition("-RA", "BKN") == "rain"
        assert parse_condition("RA", "OVC") == "rain"

    def test_heavy_rain_shower(self):
        assert parse_condition("+SHRA", "OVC") == "rain"
        assert parse_condition("SHRA", "BKN") == "rain"

    def test_thunderstorm_takes_priority_over_rain(self):
        # "TSRA" combines rain and thunder — prefer "thunderstorm".
        assert parse_condition("TSRA", "OVC") == "thunderstorm"
        assert parse_condition("+TSRAGR", "BKN") == "thunderstorm"
        assert parse_condition("-SHRA VCTS", "BKN") == "thunderstorm"

    def test_snow(self):
        assert parse_condition("-SN", "OVC") == "snow"
        assert parse_condition("SHSN", "BKN") == "snow"
        assert parse_condition("BLSN", "OVC") == "snow"

    def test_drizzle(self):
        assert parse_condition("DZ", "OVC") == "drizzle"
        assert parse_condition("-DZ", "BKN") == "drizzle"

    def test_fog_and_mist(self):
        assert parse_condition("FG", "OVC") == "fog"
        assert parse_condition("BR", "BKN") == "fog"
        assert parse_condition("HZ", "FEW") == "fog"

    def test_real_uacc_observation_during_rainstorm(self):
        # Captured from aviationweather.gov during the Astana-Kyzylzhar match
        # on 2026-05-09: "-SHRA VCTS" (light rain shower, thunderstorm in vicinity).
        assert parse_condition("-SHRA VCTS", "BKN") == "thunderstorm"

    def test_unknown_falls_back_to_clouds(self):
        # Unknown phenomenon with unknown cover defaults to clouds.
        assert parse_condition("FZRA", None) == "rain"  # freezing rain still rain
        assert parse_condition(None, None) == "clouds"
        assert parse_condition("", "ZZZ") == "clouds"


class TestIcaoForCity:
    """City alias → ICAO map covers KZ/RU/EN spellings of Kazakh cities."""

    def test_astana_aliases(self):
        assert icao_for_city("Астана") == "UACC"
        assert icao_for_city("astana") == "UACC"
        assert icao_for_city("ASTANA") == "UACC"
        assert icao_for_city("  Астана  ") == "UACC"

    def test_almaty_aliases(self):
        assert icao_for_city("Алматы") == "UAAA"
        assert icao_for_city("Алма-Ата") == "UAAA"
        assert icao_for_city("Almaty") == "UAAA"

    def test_petropavl(self):
        assert icao_for_city("Петропавл") == "UACP"
        assert icao_for_city("Петропавловск") == "UACP"

    def test_unknown_city_returns_none(self):
        assert icao_for_city("Hogwarts") is None
        assert icao_for_city("") is None
        assert icao_for_city(None) is None


class TestFormatWeather:
    """``format_weather`` is the rendering entrypoint reused across providers."""

    def test_positive_temp_with_ru_label(self):
        assert format_weather(20, "clear", "ru") == "+20°C, Ясно"

    def test_negative_temp_no_plus_sign(self):
        assert format_weather(-5, "snow", "ru") == "-5°C, Снег"

    def test_thunderstorm_label_kz(self):
        assert format_weather(16, "thunderstorm", "kz") == "+16°C, Найзағай"

    def test_unknown_lang_falls_back_to_english(self):
        assert format_weather(15, "rain", "fr") == "+15°C, Rain"

    def test_missing_data_returns_none(self):
        assert format_weather(None, "clear", "ru") is None
        assert format_weather(20, None, "ru") is None
