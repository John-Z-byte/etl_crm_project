NAME_MAP = {
    149: "Green Bay",      203: "Appleton",      238: "Sheboygan",
    363: "Madison",        391: "Cedarburg",     427: "Racine",
    850: "Burlington",     858: "Stevens Point", 237: "Nashville",
    434: "Bowling Green",  629: "Frankfort",     668: "Clarksville",
    772: "Franklin",       780: "Gadsden",       827: "Goodlettsville",
}

ACRO_MAP = {
    149: "GB",   203: "Apl", 238: "Sheb", 363: "Mad",
    391: "Ced",  427: "Rac", 850: "Burl", 858: "SP",
    237: "Nash", 434: "BG",  629: "FT",   668: "Clar",
    772: "Fran", 780: "Gad", 827: "Good",
}

def enrich_franchise_columns(df):
    df["franchise_name"] = df["franchise"].map(NAME_MAP)
    df["franchise_acro"] = df["franchise"].map(ACRO_MAP)
    return df
