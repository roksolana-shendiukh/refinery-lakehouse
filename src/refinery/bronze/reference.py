"""Reference CSVs from the repo, landed in the volume so that bronze reads them with Auto Loader."""

import hashlib
import logging
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)

REFERENCE_FOLDER = "reference"

# Columns of every reference CSV, as agreed in the STM (sheet "Bronze tables")
REFERENCE_COLUMNS = {
    "ref_area": ["area_bk", "area_name", "area_level", "parent_area_bk", "is_coastal"],
    "ref_hub": ["hub_bk", "hub_name", "served_area_bk"],
    "ref_product": ["product_bk", "product_name", "product_family", "unit", "representative_grade_bk"],
    "ref_fuel_grade": ["grade_bk", "grade_name", "source_unit", "product_bk"],
    "ref_eia_product": ["eia_product", "price_type", "grade_bk"],
    "ref_terminal_zone": ["port_bk", "port_name", "terminal_type", "area_bk", "boundary_wkt"],
    "ref_mid": ["mid", "country_name"],
}


def content_hash(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()[:16]


def landed_reference_file(root, name, digest):
    return f"{root}/{REFERENCE_FOLDER}/{name}/sha256={digest}/{name}.csv"


def land_reference(reference_dir, root):
    landed = []
    for source in sorted(Path(reference_dir).glob("*.csv")):
        digest = content_hash(source)
        target = Path(landed_reference_file(root, source.stem, digest))
        if target.exists():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        logger.info("Landed reference %s (sha256=%s)", source.stem, digest)
        landed.append(source.stem)
    return landed
