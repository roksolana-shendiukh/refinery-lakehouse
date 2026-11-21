"""What the RAG assistant reads from eia.gov: Today in Energy articles by tag and the current STEO petroleum pages."""

from datetime import date

TIE_BASE_URL = "https://www.eia.gov/todayinenergy/"

# Today in Energy tags that cover the U.S. petroleum market.
TIE_TAGS = ("oil/petroleum", "crude oil", "gasoline", "liquid fuels")

# Articles published before this date are not part of the knowledge base.
ARTICLES_SINCE = date(2024, 1, 1)

# STEO pages always show the latest monthly edition at the same URL.
STEO_PAGES = {
    "global_oil": "https://www.eia.gov/outlooks/steo/report/global_oil.php",
    "petro_prod": "https://www.eia.gov/outlooks/steo/report/petro_prod.php",
}
