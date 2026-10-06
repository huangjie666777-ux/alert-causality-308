import pytest

from causewatch_308.parser import ParseError, parse_text


def test_simple_gauge_with_help_and_type():
    text = "# HELP temp Temperature\n# TYPE temp gauge\ntemp 1.5\n"
    samples = parse_text(text)
    assert len(samples) == 1
    assert samples[0].metric == "temp"
    assert samples[0].labels == {}
    assert samples[0].value == 1.5


def test_untyped_metric_is_kept():
    samples = parse_text("some_metric 42\n")
    assert [s.metric for s in samples] == ["some_metric"]


def test_label_escapes():
    text = 'temp{room="server",note="a\\nb \\"q\\" \\\\"} 2\n'
    (sample,) = parse_text(text)
    assert sample.labels == {"room": "server", "note": 'a\nb "q" \\'}


def test_empty_braces_allowed():
    (sample,) = parse_text("temp{} 1\n")
    assert sample.labels == {}


def test_value_formats():
    for token, expected in [
        ("1e3", 1000.0),
        (".5", 0.5),
        ("5.", 5.0),
        ("-2", -2.0),
        ("+3.25E-2", 0.0325),
    ]:
        (sample,) = parse_text(f"m {token}\n")
        assert sample.value == pytest.approx(expected)


def test_explicit_timestamp_rejected():
    with pytest.raises(ParseError, match="timestamp"):
        parse_text("temp 1 1395066363000\n")


def test_duplicate_series_rejected():
    with pytest.raises(ParseError, match="duplicate series"):
        parse_text('temp{a="1"} 1\ntemp{a="1"} 2\n')


def test_duplicate_series_label_order_independent():
    with pytest.raises(ParseError, match="duplicate series"):
        parse_text('temp{a="1",b="2"} 1\ntemp{b="2",a="1"} 2\n')


def test_non_finite_values_rejected():
    for token in ("NaN", "+Inf", "-Inf", "Inf"):
        with pytest.raises(ParseError, match="non-finite"):
            parse_text(f"temp {token}\n")


def test_invalid_value_rejected():
    with pytest.raises(ParseError, match="invalid sample value"):
        parse_text("temp abc\n")


def test_missing_value_rejected():
    with pytest.raises(ParseError, match="missing sample value"):
        parse_text("temp\n")


def test_extra_tokens_rejected():
    with pytest.raises(ParseError, match="malformed"):
        parse_text("temp 1 2 3\n")


def test_invalid_metric_name_rejected():
    with pytest.raises(ParseError, match="invalid metric name"):
        parse_text("1temp 1\n")


def test_invalid_label_name_rejected():
    with pytest.raises(ParseError, match="invalid label name"):
        parse_text('temp{1a="x"} 1\n')


def test_duplicate_label_rejected():
    with pytest.raises(ParseError, match="duplicate label"):
        parse_text('temp{a="1",a="2"} 1\n')


def test_unterminated_label_value_rejected():
    with pytest.raises(ParseError, match="unterminated label value"):
        parse_text('temp{a="x} 1\n')


def test_invalid_escape_rejected():
    with pytest.raises(ParseError, match="invalid escape"):
        parse_text('temp{a="x\\ty"} 1\n')


def test_non_gauge_types_validated_but_skipped():
    text = (
        "# TYPE hits counter\nhits 5\n"
        "# TYPE temp gauge\ntemp 1\n"
        "other 2\n"
    )
    samples = parse_text(text)
    assert [s.metric for s in samples] == ["temp", "other"]


def test_duplicate_inside_skipped_type_still_fails():
    with pytest.raises(ParseError, match="duplicate series"):
        parse_text("# TYPE h counter\nh 1\nh 2\n")


def test_timestamp_inside_skipped_type_still_fails():
    with pytest.raises(ParseError, match="timestamp"):
        parse_text("# TYPE h counter\nh 1 12345\n")


def test_unknown_type_rejected():
    with pytest.raises(ParseError, match="unknown metric type"):
        parse_text("# TYPE m weird\nm 1\n")


def test_type_after_sample_rejected():
    with pytest.raises(ParseError, match="after samples"):
        parse_text("m 1\n# TYPE m gauge\n")


def test_duplicate_type_rejected():
    with pytest.raises(ParseError, match="duplicate TYPE"):
        parse_text("# TYPE m gauge\n# TYPE m gauge\nm 1\n")


def test_comments_and_blank_lines_ignored():
    text = "# a comment\n\n# EOF\nm 1\n"
    (sample,) = parse_text(text)
    assert sample.value == 1.0
