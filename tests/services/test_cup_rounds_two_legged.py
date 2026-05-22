"""Unit tests for two-legged tie aggregation in the cup playoff bracket."""
from datetime import date

from app.schemas.cup import CupGameBrief, CupRound, CupTeamBrief
from app.services.cup_rounds import build_playoff_bracket_from_rounds


ORDABASY = CupTeamBrief(id=81, name="Ордабасы")
ALTAI = CupTeamBrief(id=295, name="Алтай")
KAYSAR = CupTeamBrief(id=94, name="Кайсар")
ZHENIS = CupTeamBrief(id=92, name="Женис")


def _leg(
    game_id, day, home, away, hs=None, ascore=None, hp=None, ap=None, status="finished"
):
    return CupGameBrief(
        id=game_id,
        date=date(2026, 7 if day < 20 else 8, day),
        home_team=home,
        away_team=away,
        home_score=hs,
        away_score=ascore,
        home_penalty_score=hp,
        away_penalty_score=ap,
        status=status,
    )


def _semis_round(games):
    return CupRound(
        stage_id=226,
        round_name="1/2 финала",
        round_key="1_2",
        total_games=len(games),
        played_games=sum(1 for g in games if g.status == "finished"),
        games=games,
    )


def _entries_by_round(bracket, round_key):
    for r in bracket.rounds:
        if r.round_name == round_key:
            return r.entries
    return []


def test_two_legged_tie_aggregates_by_sum():
    # Ордабасы 1:0 Алтай (leg1) + Алтай 1:2 Ордабасы (leg2) → agg Ордабасы 3:1 Алтай
    games = [
        _leg(1, 15, ORDABASY, ALTAI, 1, 0),
        _leg(2, 19, ALTAI, ORDABASY, 1, 2),
    ]
    bracket = build_playoff_bracket_from_rounds(202, [_semis_round(games)])
    entries = _entries_by_round(bracket, "1_2")

    assert len(entries) == 1
    entry = entries[0]
    assert entry.is_two_legged is True
    assert len(entry.legs) == 2
    # Top team = first-leg host (Ордабасы)
    assert entry.game.home_team.id == ORDABASY.id
    assert entry.game.away_team.id == ALTAI.id
    assert entry.game.home_score == 3
    assert entry.game.away_score == 1
    assert entry.game.winner_team.id == ORDABASY.id
    assert entry.game.decided_in is None
    assert entry.game.status == "finished"


def test_level_aggregate_decided_by_return_leg_penalties():
    # Кайсар 1:0 Женис (leg1) + Женис 1:0 Кайсар (leg2) → agg 1:1, penalties in leg2
    # leg2: Женис home pens 4, Кайсар (away) pens 5 → Кайсар advances
    games = [
        _leg(3, 15, KAYSAR, ZHENIS, 1, 0),
        _leg(4, 19, ZHENIS, KAYSAR, 1, 0, hp=4, ap=5),
    ]
    bracket = build_playoff_bracket_from_rounds(202, [_semis_round(games)])
    entry = _entries_by_round(bracket, "1_2")[0]

    assert entry.is_two_legged is True
    assert entry.game.home_team.id == KAYSAR.id
    assert entry.game.home_score == 1
    assert entry.game.away_score == 1
    assert entry.game.winner_team.id == KAYSAR.id
    assert entry.game.decided_in == "penalties"
    # Penalties oriented to top team (Кайсар was away in the return leg → 5)
    assert entry.game.home_penalty_score == 5
    assert entry.game.away_penalty_score == 4


def test_two_ties_split_across_sides():
    games = [
        _leg(1, 15, ORDABASY, ALTAI, 1, 0),
        _leg(2, 19, ALTAI, ORDABASY, 1, 2),
        _leg(3, 15, KAYSAR, ZHENIS, 0, 0),
        _leg(4, 19, ZHENIS, KAYSAR, 0, 0, hp=2, ap=4),
    ]
    bracket = build_playoff_bracket_from_rounds(202, [_semis_round(games)])
    entries = _entries_by_round(bracket, "1_2")

    assert len(entries) == 2
    sides = {e.game.home_team.id: e.side for e in entries}
    assert sides[ORDABASY.id] == "left"
    assert sides[KAYSAR.id] == "right"


def test_unplayed_two_legged_tie_has_no_scores_or_winner():
    games = [
        _leg(1, 15, ORDABASY, ALTAI, status="upcoming"),
        _leg(2, 19, ALTAI, ORDABASY, status="upcoming"),
    ]
    bracket = build_playoff_bracket_from_rounds(202, [_semis_round(games)])
    entry = _entries_by_round(bracket, "1_2")[0]

    assert entry.is_two_legged is True
    assert entry.game.home_score is None
    assert entry.game.away_score is None
    assert entry.game.winner_team is None
    assert entry.game.status == "upcoming"


def test_single_leg_round_unchanged():
    # A single game per pair (quarterfinal-style) stays a one-match entry
    single = CupRound(
        stage_id=224,
        round_name="1/4 финала",
        round_key="1_4",
        total_games=1,
        played_games=1,
        games=[
            CupGameBrief(
                id=9,
                date=date(2026, 7, 13),
                home_team=ORDABASY,
                away_team=ALTAI,
                home_score=2,
                away_score=1,
                status="finished",
                winner_team_id=ORDABASY.id,
            )
        ],
    )
    bracket = build_playoff_bracket_from_rounds(202, [single])
    entry = _entries_by_round(bracket, "1_4")[0]

    assert entry.is_two_legged is False
    assert entry.legs == []
    assert entry.game.home_score == 2
    assert entry.game.winner_team.id == ORDABASY.id
