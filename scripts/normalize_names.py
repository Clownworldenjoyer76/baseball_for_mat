def normalize_name(name):
    if not isinstance(name, str):
        return ""

    # Allowed suffixes (can be expanded)
    suffixes = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv"}

    # Step 1: Strip accents
    name = strip_accents(name)

    # Step 2: Preserve apostrophes and hyphens
    name = re.sub(r"[^a-zA-Z0-9,'’\-\. ]", "", name)
    name = name.replace("’", "'").strip()

    # Step 3: Normalize whitespace and commas
    name = re.sub(r"\s+", " ", name)
    name = re.sub(r",\s*", ", ", name)

    # Step 4: Handle "Last, First" or "First Last [Suffix]" format
    if "," in name:
        parts = name.split(",")
        last = parts[0].strip().title()
        rest = parts[1].strip().split()

        if not rest:
            return f"{last}"

        first = rest[0].title()
        suffix = " ".join(rest[1:]).lower().rstrip(".")
        if suffix in suffixes:
            last = f"{last} {suffix.title()}"
        return f"{last}, {first}"

    else:
        # Assume "First Middle Last [Suffix]"
        tokens = name.strip().split()
        if len(tokens) < 2:
            return name.title()

        first = tokens[0].title()
        suffix = tokens[-1].lower().rstrip(".")
        if suffix in suffixes:
            last = " ".join(tokens[1:-1]).title() + f" {suffix.title()}"
        else:
            last = " ".join(tokens[1:]).title()
        return f"{last}, {first}"
