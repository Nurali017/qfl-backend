from app.utils.lineup_feed_parser import parse_maybe_json_array, parse_team_lineup_feed


def test_parse_team_lineup_feed_supports_case_insensitive_keys_and_markers():
    payload = [
        {"Number": "FORMATION", "First_name": "4-3-3 down", "Full_name": "#2494D2"},
        {"Number": "MAIN", "Full_name": "Main Coach"},
        {"Number": "STARTING"},
        {
            "Number": "1",
            "First_name": "Home",
            "Last_name": "Goalkeeper",
            "Amplua": "Gk",
            "Position": "C",
            "Gk": True,
            "Capitan": False,
            "Id": "11111111-1111-4111-8111-111111111111",
        },
        {
            "Number": "9",
            "First_name": "Home",
            "Last_name": "Forward",
            "Amplua": "F",
            "Position": "L",
            "Gk": False,
            "Capitan": True,
        },
        {"Number": "SUBS"},
        {
            "Number": "12",
            "First_name": "Bench",
            "Last_name": "Player",
            "Amplua": "",
            "Position": "",
            "Gk": False,
            "Capitan": False,
        },
    ]

    parsed = parse_team_lineup_feed(payload)

    assert parsed["formation_string"] == "4-3-3 down"
    assert parsed["formation_color"] == "#2494D2"
    assert parsed["coach"] == "Main Coach"
    assert parsed["has_starting_marker"] is True
    assert parsed["has_subs_marker"] is True
    assert len(parsed["main"]) == 2
    assert len(parsed["subs"]) == 1
    assert parsed["main"][0]["amplua"] == "Gk"
    assert parsed["main"][0]["position"] == "C"
    assert parsed["main"][1]["capitan"] is True
    assert parsed["is_valid_for_field"] is False


def test_parse_team_lineup_feed_marks_field_valid_when_markers_and_positions_present():
    starters = [
        {
            "number": index + 1,
            "first_name": f"Player{index}",
            "last_name": "Home",
            "amplua": "D" if index else "Gk",
            "position": "C",
            "gk": index == 0,
            "capitan": index == 1,
        }
        for index in range(11)
    ]
    payload = [{"number": "STARTING"}, *starters, {"number": "SUBS"}]

    parsed = parse_team_lineup_feed(payload)

    assert parsed["has_starting_marker"] is True
    assert parsed["has_subs_marker"] is True
    assert len(parsed["main"]) == 11
    assert parsed["is_valid_for_field"] is True


def test_parse_maybe_json_array_handles_json_text():
    parsed = parse_maybe_json_array('[{"number":"STARTING"},{"number":1}]')
    assert isinstance(parsed, list)
    assert len(parsed) == 2

