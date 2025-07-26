def normalize_name(name):
    if not isinstance(name, str):
        return ""

    name = unidecode(name).strip()
    name = re.sub(r"[^\w\s,\.]", "", name)
    name = re.sub(r"\s+", " ", name)

    suffixes = {"Jr", "Sr", "II", "III", "IV", "V", "Jr.", "Sr."}
    name = name.replace(",", "").strip()
    tokens = name.split()

    if len(tokens) < 2:
        return name.title()

    # Extract suffix if present
    if tokens[-1] in suffixes:
        suffix = tokens[-1]
        last = " ".join(tokens[:-2] + [suffix])
        first = tokens[-2]
    else:
        last = tokens[-2]
        first = tokens[-1]

    return f"{last.strip().title()}, {first.strip().title()}"
