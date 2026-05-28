"""Tests for sanitize_news_html — the XSS sanitization boundary for News.content."""
import pytest

from app.utils.html_cleaner import sanitize_news_html


class TestSanitizeNewsHtmlBasics:
    def test_none_passes_through(self):
        assert sanitize_news_html(None) is None

    def test_empty_passes_through(self):
        assert sanitize_news_html("") == ""

    def test_plain_text_unchanged(self):
        assert sanitize_news_html("Hello world") == "Hello world"

    def test_allowed_tags_preserved(self):
        html = "<p>Hello <strong>world</strong></p>"
        result = sanitize_news_html(html)
        assert "<p>" in result
        assert "<strong>" in result
        assert "world" in result


class TestSanitizeNewsHtmlXSS:
    def test_script_stripped(self):
        result = sanitize_news_html("<p>safe</p><script>alert(1)</script>")
        assert "<script>" not in (result or "")
        assert "alert(1)" not in (result or "")
        assert "safe" in result

    def test_onerror_attr_stripped(self):
        result = sanitize_news_html('<img src="x" onerror="alert(1)">')
        assert "onerror" not in (result or "")
        assert "alert" not in (result or "")
        assert 'src="x"' in result

    def test_javascript_href_stripped(self):
        result = sanitize_news_html('<a href="javascript:alert(1)">click</a>')
        # bleach drops the href, our post-pass unwraps the empty <a>
        assert "javascript" not in (result or "")
        assert "click" in result

    def test_onclick_stripped(self):
        result = sanitize_news_html('<p onclick="alert(1)">text</p>')
        assert "onclick" not in (result or "")
        assert "text" in result

    def test_data_url_dropped(self):
        result = sanitize_news_html('<a href="data:text/html,<script>alert(1)</script>">x</a>')
        assert "data:" not in (result or "")
        assert "alert" not in (result or "")


class TestSanitizeNewsHtmlIframe:
    def test_youtube_embed_kept(self):
        html = '<iframe src="https://www.youtube.com/embed/abc123" allowfullscreen></iframe>'
        result = sanitize_news_html(html)
        assert "<iframe" in result
        assert "youtube.com/embed/abc123" in result

    def test_youtube_nocookie_embed_kept(self):
        html = '<iframe src="https://www.youtube-nocookie.com/embed/xyz"></iframe>'
        result = sanitize_news_html(html)
        assert "youtube-nocookie.com/embed/xyz" in result

    def test_vimeo_iframe_dropped(self):
        result = sanitize_news_html('<iframe src="https://player.vimeo.com/video/123"></iframe>')
        assert "<iframe" not in (result or "")

    def test_evil_iframe_dropped(self):
        result = sanitize_news_html('<iframe src="https://evil.com/xss"></iframe>')
        assert "<iframe" not in (result or "")
        assert "evil.com" not in (result or "")

    def test_iframe_without_src_dropped(self):
        result = sanitize_news_html("<iframe></iframe>")
        assert "<iframe" not in (result or "")

    def test_youtube_with_dangerous_attrs_keeps_only_allowlist(self):
        html = '<iframe src="https://www.youtube.com/embed/abc" onload="alert(1)"></iframe>'
        result = sanitize_news_html(html)
        assert "<iframe" in result
        assert "onload" not in result


class TestSanitizeNewsHtmlAnchor:
    def test_anchor_with_valid_href_kept(self):
        result = sanitize_news_html('<a href="https://example.com">link</a>')
        assert '<a href="https://example.com">link</a>' in result

    def test_anchor_target_blank_gets_rel(self):
        result = sanitize_news_html('<a href="https://example.com" target="_blank">link</a>')
        assert 'target="_blank"' in result
        assert 'rel="noopener noreferrer"' in result

    def test_anchor_without_href_unwrapped(self):
        result = sanitize_news_html("<p>before <a>orphan</a> after</p>")
        assert "<a" not in result
        assert "orphan" in result
        assert "before" in result and "after" in result

    def test_mailto_kept(self):
        result = sanitize_news_html('<a href="mailto:test@example.com">email</a>')
        assert "mailto:test@example.com" in result


class TestSanitizeNewsHtmlContentTags:
    def test_table_preserved(self):
        html = "<table><tr><td colspan='2'>cell</td></tr></table>"
        result = sanitize_news_html(html)
        assert "<table>" in result
        assert "<td" in result
        assert "colspan" in result

    def test_img_attrs_preserved(self):
        html = '<img src="https://x.com/a.jpg" alt="alt" width="100" height="50">'
        result = sanitize_news_html(html)
        assert "src=" in result
        assert "alt=" in result
        assert "width=" in result

    def test_headings_preserved(self):
        for tag in ("h2", "h3", "h4"):
            result = sanitize_news_html(f"<{tag}>title</{tag}>")
            assert f"<{tag}>" in result

    def test_h1_dropped(self):
        # h1 not in allowlist (title is in a separate field)
        result = sanitize_news_html("<h1>title</h1>")
        assert "<h1>" not in (result or "")
        assert "title" in result


class TestSanitizeNewsHtmlIdempotent:
    def test_sanitize_twice_is_noop(self):
        html = '<p>safe</p><script>evil</script><a href="javascript:x">click</a>'
        once = sanitize_news_html(html)
        twice = sanitize_news_html(once)
        assert once == twice

    def test_sanitize_clean_input_unchanged(self):
        html = '<p>Hello <a href="https://example.com">world</a></p>'
        result = sanitize_news_html(html)
        assert sanitize_news_html(result) == result
