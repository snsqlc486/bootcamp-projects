VERSION = "1.0.0"

from .review_collector import collect_imdb_reviews
from .rating_collector import collect_imdb_ratings
__all__ = ["collect_imdb_reviews", "collect_imdb_ratings"]