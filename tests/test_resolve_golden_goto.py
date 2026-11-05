"""A goto link counts as resolved only when Google redirects off Google."""
import pytest

from scripts.resolve_golden_goto import classify, golden_url_for, is_goto

GOTO = "https://www.google.com/goto?url=CAESeAHrOzAVFZ"


def test_is_goto():
    assert is_goto(GOTO)
    assert not is_goto("https://www.google.com/search?q=x")
    assert not is_goto("https://example.com/goto?url=x")


@pytest.mark.parametrize(
    "http_status, location, expected",
    [
        (302, "https://web.gekisaka.jp/news/1", "resolved"),
        (301, "https://example.com/", "resolved"),
        (302, "https://www.google.com/sorry/index?continue=x", "blocked"),
        (302, "https://consent.google.com/ml?continue=x", "blocked"),
        (429, None, "blocked"),
        (503, None, "blocked"),
        (302, None, "error"),
        (200, None, "error"),
        (404, None, "error"),
    ],
)
def test_classify(http_status, location, expected):
    assert classify(http_status, location)[0] == expected


def test_lookalike_host_is_not_google():
    assert classify(302, "https://notgoogle.com/a")[0] == "resolved"


def test_golden_url_for_keeps_unresolved():
    resolved = {GOTO: "https://a.com/p"}
    assert golden_url_for(GOTO, resolved) == "https://a.com/p"
    assert golden_url_for("https://b.com/", resolved) == "https://b.com/"


def test_wait_after_skips_one_block_and_backs_off_on_two():
    from argparse import Namespace
    from scripts.resolve_golden_goto import wait_after
    args = Namespace(interval=5, backoff=600, pause_after=2, max_blocked=5)
    assert wait_after("resolved", 0, args) == 5
    assert wait_after("error", 0, args) == 5
    assert wait_after("blocked", 1, args) == 5
    assert wait_after("blocked", 2, args) == 600
    assert wait_after("blocked", 4, args) == 600
    assert wait_after("blocked", 5, args) is None
