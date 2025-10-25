def flatten_record(record: dict):
    """Flatten and clean a single Finnish National Gallery artwork record."""

    # extract first person if exists
    people_list = record.get("people") or []  # may be []
    multimedia_list = record.get("multimedia") or []
    person = people_list[0] if len(people_list) > 0 else {}
    multimedia = multimedia_list[0] if len(multimedia_list) > 0 else {}
    category = record.get("category", {})

    # combine first + last name
    artist_name = None
    if person:
        first = person.get("firstName", "")
        last = person.get("familyName", "")
        artist_name = f"{first} {last}".strip() or None

    # select image (mid-size)
    image_url = None
    if multimedia.get("jpg") and "500" in multimedia["jpg"]:
        image_url = "https://cdn.fng.fi" + multimedia["jpg"]["500"]

    # flatten record
    flat = {
        "object_id": record.get("objectId"),
        "title": (record.get("title", {}) or {}).get("fi") or (record.get("title", {}) or {}).get("en"),
        "artist": artist_name,
        "artist_birth_year": person.get("birthYear"),
        "artist_death_year": person.get("deathYear"),
        "role": (person.get("role", {}) or {}).get("en"),
        "organization": record.get("responsibleOrganisation"),
        "category_fi": category.get("fi"),
        "inventory_number": record.get("inventoryNumber"),
        "license": multimedia.get("license"),
        "image_url": image_url,
        "photographer": multimedia.get("photographer_name"),
        "owner": record.get("owner"),
        "parent_id": next(iter(record.get("parents", [])), None),
    }

    return flat


def is_valid_record(record: dict):
    """Filter: only include artworks with CC0 license."""
    multimedia = record.get("multimedia", [])
    if not multimedia:
        return False  # no image, skip
    license_name = multimedia[0].get("license", "").lower()
    return "cc0" in license_name
