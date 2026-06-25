"""
Pure-Python transforms for news ingestion.

Intentionally free of Spark, Polygon SDK, and Databricks dependencies so this
function can be imported by Databricks notebooks AND tested directly in pytest.

Used by:
    - databricks/bronze/news_ingestion.py
"""


def extract_article(news_item) -> dict:
    """Extract a single Polygon TickerNews API object into a flat dict.

    Uses getattr/hasattr for resilience against missing optional fields
    on the Polygon SDK response objects.

    The returned ``id`` key is renamed to ``article_id`` by the caller
    (news_ingestion.py) when building the Bronze DataFrame.

    Args:
        news_item: A Polygon TickerNews object (or any object with the same
                   attributes: id, title, description, author, published_utc,
                   article_url, amp_url, image_url, tickers, keywords,
                   insights, publisher).
                   ``amp_url`` is optional and retrieved with getattr;
                   all other listed attributes are accessed directly.

    Returns:
        dict: Flat dict with keys id, title, description, author, published_utc,
              article_url, amp_url, image_url, tickers, keywords, insights,
              publisher.  ``insights`` is a list of dicts each with keys
              sentiment, sentiment_reasoning, ticker.  ``publisher`` is a dict
              with keys name, homepage_url, logo_url, favicon_url (or {} when
              the publisher field is absent).
    """
    insights_list = []
    if hasattr(news_item, "insights") and news_item.insights:
        for insight in news_item.insights:
            insights_list.append(
                {
                    "sentiment": getattr(insight, "sentiment", None),
                    "sentiment_reasoning": getattr(insight, "sentiment_reasoning", None),
                    "ticker": getattr(insight, "ticker", None),
                }
            )

    return {
        "id": news_item.id,
        "title": news_item.title,
        "description": news_item.description,
        "author": news_item.author,
        "published_utc": news_item.published_utc,
        "article_url": news_item.article_url,
        "amp_url": getattr(news_item, "amp_url", None),
        "image_url": news_item.image_url,
        "tickers": news_item.tickers if hasattr(news_item, "tickers") else [],
        "keywords": news_item.keywords if hasattr(news_item, "keywords") else [],
        "insights": insights_list,
        "publisher": {
            "name": news_item.publisher.name if news_item.publisher else None,
            "homepage_url": news_item.publisher.homepage_url if news_item.publisher else None,
            "logo_url": news_item.publisher.logo_url if news_item.publisher else None,
            "favicon_url": getattr(news_item.publisher, "favicon_url", None) if news_item.publisher else None,
        }
        if hasattr(news_item, "publisher") and news_item.publisher
        else {},
    }
