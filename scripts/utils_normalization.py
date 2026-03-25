def normalize_branch(branch: str) -> str:
    if not branch:
        return "UNKNOWN"

    b = branch.lower().strip()

    if "waigani" in b:
        return "WAIGANI"
    if "bena" in b:
        return "BENA_ROAD"
    if "malaita" in b:
        return "LAE_MALAITA"
    if "5th" in b:
        return "5TH_STREET"

    return b.upper()
