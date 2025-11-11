
from textblob import TextBlob

def analyze_sentiment(text):
    """Performs sentiment analysis on the given text."""
    analysis = TextBlob(text)
    # Return 'positive', 'negative', or 'neutral'
    if analysis.sentiment.polarity > 0:
        return 'positive'
    elif analysis.sentiment.polarity < 0:
        return 'negative'
    else:
        return 'neutral'
