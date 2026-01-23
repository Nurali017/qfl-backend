#!/usr/bin/env python3
"""Debug fuzzy matching for da Costa Monteiro."""

from fuzzywuzzy import fuzz

# DB entry
db_first = "Jorge Gabriel"
db_last = "da Costa Monteiro"
db_full = f"{db_first} {db_last}".lower()

print("=" * 80)
print(f"DB Entry: {db_last} {db_first}")
print(f"Full name: {db_full}")
print("=" * 80)
print()

# Test various scraped combinations
test_cases = [
    # What we tested before
    ("Jorge", "Gabriel"),
    ("Gabriel", "Jorge"),
    ("Monteiro", "Jorge"),

    # More variations
    ("Jorge Gabriel", "da Costa Monteiro"),  # Exact match (should work)
    ("da Costa", "Monteiro"),
    ("Costa", "Monteiro Jorge"),
    ("Monteiro", "Jorge Gabriel"),
    ("Gabriel", "da Costa Monteiro"),
    ("Jorge", "Monteiro"),
    ("Gabriel Jorge", "Monteiro"),
    ("Jorge Gabriel", "Monteiro"),
    ("Jorge Gabriel", "Costa Monteiro"),
    ("Jorge", "da Costa Monteiro"),

    # Possible Cyrillic variations from site
    ("Жоржи", "Габриэль"),
    ("Монтейру", "Жоржи"),
]

print("Testing fuzzy matching scores:")
print()

for first, last in test_cases:
    scraped_full = f"{first} {last}".lower()

    # Test different fuzzy matching algorithms
    token_sort = fuzz.token_sort_ratio(scraped_full, db_full)
    partial = fuzz.partial_ratio(scraped_full, db_full)
    token_set = fuzz.token_set_ratio(scraped_full, db_full)
    ratio = fuzz.ratio(scraped_full, db_full)

    final_score = max(token_sort, partial)
    match = "✅" if final_score >= 75 else "❌"

    print(f"{match} {first:20} {last:25}")
    print(f"   Scraped: {scraped_full}")
    print(f"   Scores: token_sort={token_sort}, partial={partial}, token_set={token_set}, ratio={ratio}")
    print(f"   Final: {final_score} (threshold=75)")
    print()
