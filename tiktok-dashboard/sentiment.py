"""
sentiment.py — VADER-based sentiment analysis for TikTok comments.
"""

from nltk.sentiment.vader import SentimentIntensityAnalyzer

_analyzer = None


def _get_analyzer():
    global _analyzer
    if _analyzer is None:
        _analyzer = SentimentIntensityAnalyzer()
    return _analyzer


def analyze(text: str) -> tuple[float, str]:
    """
    Returns (compound_score, label) where:
      - compound is in [-1, 1]
      - label is 'positive', 'neutral', or 'negative'
    """
    if not text or not text.strip():
        return 0.0, "neutral"

    sia = _get_analyzer()
    scores = sia.polarity_scores(text)
    compound = scores["compound"]

    if compound >= 0.05:
        label = "positive"
    elif compound <= -0.05:
        label = "negative"
    else:
        label = "neutral"

    return round(compound, 4), label
